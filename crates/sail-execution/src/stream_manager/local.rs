use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use futures::StreamExt;
use log::debug;
use tokio::sync::{mpsc, watch};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamSink, TaskStreamSinkState};
use crate::stream_manager::shuffle_file::{
    read_map_output_channel, MapOutputStatus, MapOutputWriter,
};

pub trait LocalStream: Send {
    fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamSink>>;
    fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource>;
}

/// A memory stream that can be read multiple times.
/// It maintains multiple replicas of the stream internally.
/// Since [`Arc`] is used inside the record batch, it is relatively cheap
/// to clone the data in multiple replicas.
pub(crate) struct MemoryStream {
    sender: Option<MemoryStreamReplicaSender>,
    receivers: Vec<mpsc::Receiver<TaskStreamResult<RecordBatch>>>,
}

impl MemoryStream {
    pub fn new(
        buffer: usize,
        replicas: usize,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    ) -> Self {
        let replicas = replicas.max(senders.len());
        let diff = replicas - senders.len();
        let mut senders = senders.into_iter().map(Some).collect::<Vec<_>>();
        senders.reserve(diff);
        let mut receivers = Vec::with_capacity(diff);
        for _ in 0..diff {
            let (tx, rx) = mpsc::channel(buffer);
            senders.push(Some(tx));
            receivers.push(rx);
        }
        let overflow = vec![VecDeque::new(); senders.len()];
        Self {
            sender: Some(MemoryStreamReplicaSender { senders, overflow }),
            receivers,
        }
    }
}

impl LocalStream for MemoryStream {
    fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        let sender = self.sender.take().ok_or_else(|| {
            ExecutionError::InternalError("memory stream can only be written once".to_string())
        })?;
        Ok(Box::new(sender))
    }

    fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let rx = self.receivers.pop().ok_or_else(|| {
            ExecutionError::InternalError("memory stream has exhausted all replica(s)".to_string())
        })?;
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

struct MemoryStreamReplicaSender {
    senders: Vec<Option<mpsc::Sender<TaskStreamResult<RecordBatch>>>>,
    /// An overflow buffer for each sender to avoid blocking sending for slow senders.
    /// This also avoids deadlock situations where the task stream buffer size is small.
    // TODO: More investigation is needed to understand why deadlocks might happen among stages
    //   when the task stream buffer is of a limited size.
    overflow: Vec<VecDeque<TaskStreamResult<RecordBatch>>>,
}

#[tonic::async_trait]
impl TaskStreamSink for MemoryStreamReplicaSender {
    async fn write(&mut self, batch: TaskStreamResult<RecordBatch>) -> TaskStreamSinkState {
        let mut active = false;
        for (i, sender) in self.senders.iter_mut().enumerate() {
            if sender.is_none() {
                continue;
            }

            let overflow = &mut self.overflow[i];
            let mut dropped = false;

            if let Some(tx) = sender.as_ref() {
                // Try to flush overflow first
                while let Some(item) = overflow.pop_front() {
                    match tx.try_send(item) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(x)) => {
                            overflow.push_front(x);
                            break;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            dropped = true;
                            break;
                        }
                    }
                }
            }

            // A dropped receiver can happen under normal operation when the receiver no longer
            // needs more data (e.g., after a LIMIT operator has received enough rows).

            if dropped {
                debug!("memory stream replica receiver has been dropped");
                *sender = None;
                overflow.clear();
                continue;
            }

            if let Some(tx) = sender.as_ref() {
                if overflow.is_empty() {
                    match tx.try_send(batch.clone()) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(x)) => {
                            overflow.push_back(x);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            dropped = true;
                        }
                    }
                } else {
                    overflow.push_back(batch.clone());
                }
            }

            if dropped {
                debug!("memory stream replica receiver has been dropped");
                *sender = None;
                overflow.clear();
            } else {
                active = true;
            }
        }
        if active {
            TaskStreamSinkState::Ok
        } else {
            TaskStreamSinkState::Closed
        }
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        for (i, sender) in self.senders.iter_mut().enumerate() {
            if sender.is_none() {
                continue;
            }

            let overflow = &mut self.overflow[i];
            let mut dropped = false;
            while let Some(item) = overflow.pop_front() {
                if let Some(tx) = sender.as_ref() {
                    // TODO: `send` here is blocking and may introduce deadlocks among tasks.
                    //   This is low-risk empirically though.
                    if tx.send(item).await.is_err() {
                        dropped = true;
                        break;
                    }
                }
            }

            if dropped {
                *sender = None;
                overflow.clear();
            }
        }
        Ok(())
    }
}

/// One channel of the disk-backed output of a map task.
/// The data of all channels of a map task is stored in a single data file
/// managed by a shared [`MapOutputWriter`].
pub(crate) struct DiskStream {
    data_path: PathBuf,
    channel: usize,
    writer: Option<Arc<MapOutputWriter>>,
    status: Option<watch::Receiver<MapOutputStatus>>,
    senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
}

impl DiskStream {
    /// Creates a stream for a channel that will be written via the shared map output writer.
    pub fn new(
        data_path: PathBuf,
        channel: usize,
        writer: Arc<MapOutputWriter>,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    ) -> Self {
        Self {
            data_path,
            channel,
            status: Some(writer.subscribe_status()),
            writer: Some(writer),
            senders,
        }
    }

    /// Creates a read-only stream for a channel of an existing map output.
    pub fn existing(data_path: PathBuf, channel: usize) -> Self {
        Self {
            data_path,
            channel,
            writer: None,
            status: None,
            senders: vec![],
        }
    }
}

impl LocalStream for DiskStream {
    fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        let writer = self.writer.take().ok_or_else(|| {
            ExecutionError::InternalError("disk stream can only be written once".to_string())
        })?;
        Ok(Box::new(DiskStreamSink {
            writer,
            channel: self.channel,
            senders: std::mem::take(&mut self.senders),
        }))
    }

    fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let status = match &self.status {
            // The writer has been dropped without finalizing the map output.
            Some(status)
                if status.has_changed().is_err()
                    && *status.borrow() == MapOutputStatus::Writing =>
            {
                return Err(ExecutionError::InternalError(format!(
                    "shuffle output was not completed: {}",
                    self.data_path.display()
                )));
            }
            Some(status) => status.borrow().clone(),
            None => MapOutputStatus::Finalized,
        };
        match status {
            MapOutputStatus::Finalized => Ok(read_map_output_channel(
                self.data_path.clone(),
                self.channel,
            )),
            MapOutputStatus::Failed(e) => Err(ExecutionError::InternalError(format!(
                "shuffle output failed for {}: {e}",
                self.data_path.display()
            ))),
            MapOutputStatus::Writing => {
                // The map output is not complete yet, so we wait for it to be finalized
                // before reading the data. We never return partial data.
                let mut status = self.status.clone().ok_or_else(|| {
                    ExecutionError::InternalError("missing map output status".to_string())
                })?;
                let data_path = self.data_path.clone();
                let channel = self.channel;
                let source = async move {
                    let result = status
                        .wait_for(|x| *x != MapOutputStatus::Writing)
                        .await
                        .map(|x| x.clone());
                    let error = |m: String| -> TaskStreamSource {
                        Box::pin(futures::stream::once(async move {
                            Err(TaskStreamError::Unknown(m))
                        }))
                    };
                    match result {
                        Ok(MapOutputStatus::Finalized) => {
                            read_map_output_channel(data_path.clone(), channel)
                        }
                        Ok(MapOutputStatus::Failed(e)) => error(format!(
                            "shuffle output failed for {}: {e}",
                            data_path.display()
                        )),
                        Ok(MapOutputStatus::Writing) | Err(_) => error(format!(
                            "shuffle output was not completed: {}",
                            data_path.display()
                        )),
                    }
                };
                Ok(Box::pin(futures::stream::once(source).flatten()))
            }
        }
    }
}

struct DiskStreamSink {
    writer: Arc<MapOutputWriter>,
    channel: usize,
    senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
}

#[tonic::async_trait]
impl TaskStreamSink for DiskStreamSink {
    async fn write(&mut self, batch: TaskStreamResult<RecordBatch>) -> TaskStreamSinkState {
        let batch = match batch {
            Ok(b) => b,
            Err(e) => {
                return TaskStreamSinkState::Error(datafusion::error::DataFusionError::Execution(
                    e.to_string(),
                ));
            }
        };

        if let Err(e) = self.writer.write(self.channel, batch.clone()).await {
            return TaskStreamSinkState::Error(e);
        }

        // Forward the batch to any waiting receivers. The batch must not be dropped
        // when the channel is full, so we wait for capacity. Receivers that have gone
        // away are removed since they no longer need the data.
        let mut i = 0;
        while i < self.senders.len() {
            if self.senders[i].send(Ok(batch.clone())).await.is_err() {
                self.senders.swap_remove(i);
            } else {
                i += 1;
            }
        }

        TaskStreamSinkState::Ok
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        self.writer.close(self.channel).await?;
        // Drop senders to close channels for any pending receivers
        self.senders.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::path::Path;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use super::*;
    use crate::id::{JobId, TaskStreamKey};
    use crate::stream::writer::LocalStreamStorage;
    use crate::stream_manager::shuffle_file::{ShuffleIndex, DEFAULT_SHUFFLE_SPILL_THRESHOLD};
    use crate::stream_manager::{StreamManager, StreamManagerOptions};

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sail_test_{name}_{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn list_files(dir: &Path) -> Vec<String> {
        let mut files = std::fs::read_dir(dir)
            .unwrap()
            .map(|x| x.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    /// Creates one disk stream per channel for a map task, sharing one map output writer.
    fn create_streams(
        data_path: &Path,
        channels: usize,
        spill_threshold: usize,
    ) -> Vec<DiskStream> {
        let writer = Arc::new(
            MapOutputWriter::try_new(data_path.to_path_buf(), channels, spill_threshold).unwrap(),
        );
        (0..channels)
            .map(|c| DiskStream::new(data_path.to_path_buf(), c, writer.clone(), vec![]))
            .collect()
    }

    async fn read_all(mut source: TaskStreamSource) -> Vec<RecordBatch> {
        let mut out = vec![];
        while let Some(x) = source.next().await {
            out.push(x.unwrap());
        }
        out
    }

    /// Returns whether the source (or reading from it) fails before returning any data.
    async fn read_error(source: ExecutionResult<TaskStreamSource>) -> bool {
        match source {
            Ok(mut source) => matches!(source.next().await, Some(Err(_))),
            Err(_) => true,
        }
    }

    /// Waits until the condition holds, which may depend on background work.
    async fn wait_until(condition: impl Fn() -> bool) {
        for _ in 0..500 {
            if condition() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("condition not met in time");
    }

    /// Writes the given batches for each channel, interleaving writes across channels.
    async fn write_channels(streams: &mut [DiskStream], data: &[Vec<RecordBatch>]) -> Result<()> {
        let mut sinks = streams
            .iter_mut()
            .map(|s| s.publish().unwrap())
            .collect::<Vec<_>>();
        let rounds = data.iter().map(|x| x.len()).max().unwrap_or(0);
        for i in 0..rounds {
            for (c, sink) in sinks.iter_mut().enumerate() {
                if let Some(b) = data[c].get(i) {
                    assert!(matches!(
                        sink.write(Ok(b.clone())).await,
                        TaskStreamSinkState::Ok
                    ));
                }
            }
        }
        for sink in sinks {
            sink.close().await?;
        }
        Ok(())
    }

    fn test_data(channels: usize) -> Vec<Vec<RecordBatch>> {
        (0..channels)
            .map(|c| match c % 4 {
                // empty channel
                0 => vec![],
                // channel with only an empty batch
                1 => vec![RecordBatch::new_empty(schema())],
                _ => (0..c)
                    .map(|i| {
                        batch(((c * 100 + i * 10) as i32..(c * 100 + i * 10 + 3) as i32).collect())
                    })
                    .collect(),
            })
            .collect()
    }

    #[tokio::test]
    async fn test_disk_stream_round_trip_many_channels() -> Result<()> {
        let dir = temp_dir("round_trip");
        let data_path = dir.join("shuffle_3_1.data");
        let channels = 17;
        let data = test_data(channels);
        let mut streams = create_streams(&data_path, channels, DEFAULT_SHUFFLE_SPILL_THRESHOLD);
        write_channels(&mut streams, &data).await?;

        // There is only one data file and one index file for all channels.
        assert_eq!(
            list_files(&dir),
            vec!["shuffle_3_1.data", "shuffle_3_1.index"]
        );
        let index = ShuffleIndex::read(&dir.join("shuffle_3_1.index")).unwrap();
        assert_eq!(index.channels(), channels);
        assert_eq!(index.data_size(), std::fs::metadata(&data_path)?.len());
        for (c, batches) in data.iter().enumerate() {
            let rows = batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
            assert_eq!(index.channel_records(c), Some(rows));
            assert_eq!(index.channel_bytes(c) == Some(0), batches.is_empty());
            assert_eq!(&read_all(streams[c].subscribe().unwrap()).await, batches);
            // A channel can be read multiple times, also via a read-only stream.
            let mut existing = DiskStream::existing(data_path.clone(), c);
            assert_eq!(&read_all(existing.subscribe().unwrap()).await, batches);
        }
        assert!(read_error(DiskStream::existing(data_path.clone(), channels).subscribe()).await);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_disk_stream_spill() -> Result<()> {
        let dir = temp_dir("spill");
        let data_path = dir.join("shuffle_0_0.data");
        let channels = 9;
        let data = test_data(channels);
        let size = batch(vec![1, 2, 3]).get_array_memory_size();
        // Spill after every few batches, and keep some batches in memory at the end.
        let mut streams = create_streams(&data_path, channels, size * 4 + 1);
        let mut sinks = streams
            .iter_mut()
            .map(|s| s.publish().unwrap())
            .collect::<Vec<_>>();
        let mut spilled = false;
        for i in 0..channels {
            for (c, sink) in sinks.iter_mut().enumerate() {
                if let Some(b) = data[c].get(i) {
                    assert!(matches!(
                        sink.write(Ok(b.clone())).await,
                        TaskStreamSinkState::Ok
                    ));
                }
            }
            spilled |= dir.join("shuffle_0_0.spill").exists();
        }
        assert!(spilled);
        // Close the channels in a different order.
        for sink in sinks.into_iter().rev() {
            sink.close().await?;
        }

        // The spill file is removed after the data file is written.
        assert_eq!(
            list_files(&dir),
            vec!["shuffle_0_0.data", "shuffle_0_0.index"]
        );
        for (c, batches) in data.iter().enumerate() {
            assert_eq!(&read_all(streams[c].subscribe().unwrap()).await, batches);
        }

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_disk_stream_spill_every_batch() -> Result<()> {
        let dir = temp_dir("spill_all");
        let data_path = dir.join("shuffle_0_0.data");
        let channels = 6;
        let data = test_data(channels);
        let mut streams = create_streams(&data_path, channels, 1);
        write_channels(&mut streams, &data).await?;
        assert_eq!(
            list_files(&dir),
            vec!["shuffle_0_0.data", "shuffle_0_0.index"]
        );
        for (c, batches) in data.iter().enumerate() {
            assert_eq!(&read_all(streams[c].subscribe().unwrap()).await, batches);
        }
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_disk_stream_concurrent_readers() -> Result<()> {
        let dir = temp_dir("concurrent");
        let data_path = dir.join("shuffle_0_0.data");
        let channels = 32;
        let data = test_data(channels);
        let mut streams = create_streams(&data_path, channels, 1024);
        write_channels(&mut streams, &data).await?;

        let mut handles = vec![];
        for _ in 0..4 {
            for (c, stream) in streams.iter_mut().enumerate() {
                let source = stream.subscribe().unwrap();
                let expected = data[c].clone();
                handles.push(tokio::spawn(async move {
                    assert_eq!(read_all(source).await, expected);
                }));
            }
        }
        for h in handles {
            h.await.unwrap();
        }

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_disk_stream_with_pending_senders() -> Result<()> {
        let dir = temp_dir("senders");
        let data_path = dir.join("shuffle_0_0.data");
        let writer = Arc::new(MapOutputWriter::try_new(
            data_path.clone(),
            1,
            DEFAULT_SHUFFLE_SPILL_THRESHOLD,
        )?);
        let (tx, mut rx) = mpsc::channel(16);
        let mut stream = DiskStream::new(data_path.clone(), 0, writer, vec![tx]);
        let b = batch(vec![10, 20, 30]);

        let mut sink = stream.publish().unwrap();
        assert!(stream.publish().is_err());
        sink.write(Ok(b.clone())).await;
        sink.close().await?;

        // Receiver from pending senders should get the batch and channel close
        assert_eq!(rx.recv().await.unwrap().unwrap(), b);
        assert!(rx.recv().await.is_none());

        // Subsequent subscribers can still read from disk
        assert_eq!(read_all(stream.subscribe().unwrap()).await, vec![b]);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_disk_stream_subscribe_before_finalized() -> Result<()> {
        let dir = temp_dir("early");
        let data_path = dir.join("shuffle_0_0.data");
        let mut streams = create_streams(&data_path, 2, DEFAULT_SHUFFLE_SPILL_THRESHOLD);
        let mut sink0 = streams[0].publish().unwrap();
        let sink1 = streams[1].publish().unwrap();
        let b = batch(vec![1, 2]);
        sink0.write(Ok(b.clone())).await;
        sink0.close().await?;

        // The map output is not complete until all channels are closed,
        // so the reader must wait instead of observing partial data.
        let source = streams[0].subscribe().unwrap();
        let reader = tokio::spawn(read_all(source));
        tokio::task::yield_now().await;
        assert!(!reader.is_finished());
        sink1.close().await?;
        assert_eq!(reader.await.unwrap(), vec![b]);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_disk_stream_incomplete_output_is_an_error() -> Result<()> {
        let dir = temp_dir("incomplete");
        let data_path = dir.join("shuffle_0_0.data");
        let mut streams = create_streams(&data_path, 2, 1);
        let mut sink0 = streams[0].publish().unwrap();
        let sink1 = streams[1].publish().unwrap();
        sink0.write(Ok(batch(vec![1]))).await;
        let mut source = streams[0].subscribe().unwrap();
        // The map task fails and the sinks are dropped without being closed.
        drop(sink0);
        drop(sink1);
        assert!(source.next().await.unwrap().is_err());
        assert!(streams[1].subscribe().is_err());
        // The partial output is cleaned up.
        wait_until(|| list_files(&dir).is_empty()).await;

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_disk_stream_lost_output_is_an_error() -> Result<()> {
        let dir = temp_dir("lost");
        let data_path = dir.join("shuffle_0_0.data");
        let mut streams = create_streams(&data_path, 1, DEFAULT_SHUFFLE_SPILL_THRESHOLD);
        let sink = streams[0].publish().unwrap();
        sink.close().await?;
        assert!(read_all(streams[0].subscribe().unwrap()).await.is_empty());

        // Simulate the shuffle output being cleaned up.
        std::fs::remove_dir_all(&dir)?;
        assert!(read_error(streams[0].subscribe()).await);
        assert!(read_error(DiskStream::existing(data_path, 0).subscribe()).await);
        Ok(())
    }

    /// Reading a channel happens lazily in blocking tasks, which works on a single-threaded
    /// runtime, returns all batches in order across multiple read chunks, and stops
    /// reading when the stream is dropped.
    #[tokio::test(flavor = "current_thread")]
    async fn test_disk_stream_read_in_blocking_tasks() -> Result<()> {
        let dir = temp_dir("blocking_read");
        let data_path = dir.join("shuffle_0_0.data");
        // There are more batches than what is read in a single blocking operation.
        let data = vec![(0..100).map(|i| batch(vec![i, i + 1])).collect::<Vec<_>>()];
        let mut streams = create_streams(&data_path, 1, 1000);
        write_channels(&mut streams, &data).await?;

        // Subscribing to a missing map output does not touch the file system,
        // and the error is returned when the stream is polled.
        let mut missing = DiskStream::existing(dir.join("shuffle_9_9.data"), 0);
        let mut source = missing.subscribe().unwrap();
        assert!(source.next().await.unwrap().is_err());
        assert!(source.next().await.is_none());

        assert_eq!(read_all(streams[0].subscribe().unwrap()).await, data[0]);

        let mut source = streams[0].subscribe().unwrap();
        assert_eq!(source.next().await.unwrap().unwrap(), data[0][0]);
        drop(source);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn test_shuffle_index_encoding() {
        let index = ShuffleIndex::try_new(vec![0, 0, 10, 25], vec![0, 3, 4]).unwrap();
        let data = index.encode();
        assert_eq!(data.len(), 16 + 8 * 7);
        assert_eq!(ShuffleIndex::decode(&data).unwrap(), index);
        assert_eq!(index.channel_range(1), Some((0, 10)));
        assert_eq!(index.channel_bytes(2), Some(15));
        assert_eq!(index.channel_records(2), Some(4));
        assert_eq!(index.channel_range(3), None);

        let empty = ShuffleIndex::try_new(vec![0], vec![]).unwrap();
        assert_eq!(ShuffleIndex::decode(&empty.encode()).unwrap(), empty);

        // invalid magic
        let mut bad = data.clone();
        bad[0] = b'X';
        assert!(ShuffleIndex::decode(&bad).is_err());
        // truncated or trailing data
        assert!(ShuffleIndex::decode(&data[..data.len() - 1]).is_err());
        assert!(ShuffleIndex::decode(&data[..data.len() - 8]).is_err());
        assert!(ShuffleIndex::decode(&[data.clone(), vec![0; 8]].concat()).is_err());
        assert!(ShuffleIndex::decode(&data[..12]).is_err());
        assert!(ShuffleIndex::decode(&[]).is_err());
        // huge channel count
        let mut huge = data[..16].to_vec();
        huge[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(ShuffleIndex::decode(&huge).is_err());
        // invalid offsets
        assert!(ShuffleIndex::try_new(vec![0, 10, 5], vec![1, 1]).is_err());
        assert!(ShuffleIndex::try_new(vec![1, 10], vec![1]).is_err());
        assert!(ShuffleIndex::try_new(vec![0, 10], vec![1, 1]).is_err());
    }

    #[tokio::test]
    async fn test_stream_manager_disk_streams() -> Result<()> {
        let dir = temp_dir("manager");
        let options = StreamManagerOptions::default().with_shuffle_dir(dir.clone());
        let mut manager = StreamManager::new(options);
        let channels = 3;
        let key = |channel| TaskStreamKey {
            job_id: JobId::from(1),
            stage: 2,
            partition: 3,
            attempt: 4,
            channel,
        };
        let mut sinks = vec![];
        for c in 0..channels {
            sinks.push(
                manager
                    .create_local_stream(key(c), LocalStreamStorage::Disk { channels }, schema())
                    .unwrap(),
            );
        }
        // The shared writer is owned by the sinks once all channels are created.
        assert!(manager.map_outputs.is_empty());
        assert!(manager
            .create_local_stream(key(0), LocalStreamStorage::Disk { channels }, schema())
            .is_err());
        assert!(manager
            .create_local_stream(
                key(channels),
                LocalStreamStorage::Disk { channels },
                schema()
            )
            .is_err());
        assert!(manager.map_outputs.is_empty());
        for (c, sink) in sinks.iter_mut().enumerate() {
            sink.write(Ok(batch(vec![c as i32]))).await;
        }
        for sink in sinks {
            sink.close().await?;
        }
        let stage_dir = dir.join("1").join("2");
        assert_eq!(
            list_files(&stage_dir),
            vec!["shuffle_3_4.data", "shuffle_3_4.index"]
        );

        manager.remove_local_streams(JobId::from(1), Some(2));
        wait_until(|| !stage_dir.exists()).await;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
