use std::collections::VecDeque;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use log::debug;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::error::TaskStreamResult;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamSink, TaskStreamSinkState};

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

/// A disk-backed stream that persists record batches to a data file and an index file.
pub(crate) struct DiskStream {
    file_path: std::path::PathBuf,
    is_written: bool,
    senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
}

impl DiskStream {
    pub fn new(file_path: std::path::PathBuf) -> Self {
        Self::new_with_senders(file_path, vec![])
    }

    pub fn new_with_senders(
        file_path: std::path::PathBuf,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    ) -> Self {
        Self {
            file_path,
            is_written: false,
            senders,
        }
    }

    pub fn index_path(&self) -> std::path::PathBuf {
        self.file_path.with_extension("index")
    }

    /// Read the partition shuffle statistics (total_bytes, total_records) from the index file.
    #[cfg(test)]
    pub fn read_stats(&self) -> Option<(u64, usize)> {
        let content = std::fs::read_to_string(self.index_path()).ok()?;
        let mut lines = content.lines();
        let bytes = lines.next()?.parse::<u64>().ok()?;
        let records = lines.next()?.parse::<usize>().ok()?;
        Some((bytes, records))
    }
}

impl LocalStream for DiskStream {
    fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        if self.is_written {
            return Err(ExecutionError::InternalError(
                "disk stream can only be written once".to_string(),
            ));
        }
        self.is_written = true;
        if let Some(parent) = self.file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp_file_path = self.file_path.with_extension("data.tmp");
        let tmp_index_path = self.index_path().with_extension("index.tmp");
        let file = std::fs::File::create(&tmp_file_path)?;
        Ok(Box::new(DiskStreamWriter {
            writer: Some(file),
            stream_writer: None,
            final_file_path: self.file_path.clone(),
            tmp_file_path,
            final_index_path: self.index_path(),
            tmp_index_path,
            total_records: 0,
            senders: std::mem::take(&mut self.senders),
        }))
    }

    fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        if !self.file_path.exists() {
            return Ok(Box::pin(futures::stream::empty()));
        }
        let file = std::fs::File::open(&self.file_path)?;
        if file.metadata()?.len() == 0 {
            return Ok(Box::pin(futures::stream::empty()));
        }
        let reader = match datafusion::arrow::ipc::reader::StreamReader::try_new(file, None) {
            Ok(r) => r,
            Err(e) => {
                return Err(ExecutionError::InternalError(format!(
                    "failed to open arrow ipc stream reader: {e}"
                )));
            }
        };

        let stream = futures::stream::unfold(reader, |mut reader| async move {
            match reader.next() {
                Some(Ok(batch)) => Some((Ok(batch), reader)),
                Some(Err(e)) => Some((
                    Err(crate::stream::error::TaskStreamError::Unknown(
                        e.to_string(),
                    )),
                    reader,
                )),
                None => None,
            }
        });

        Ok(Box::pin(stream))
    }
}

struct DiskStreamWriter {
    writer: Option<std::fs::File>,
    stream_writer: Option<datafusion::arrow::ipc::writer::StreamWriter<std::fs::File>>,
    final_file_path: std::path::PathBuf,
    tmp_file_path: std::path::PathBuf,
    final_index_path: std::path::PathBuf,
    tmp_index_path: std::path::PathBuf,
    total_records: usize,
    senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
}

#[tonic::async_trait]
impl TaskStreamSink for DiskStreamWriter {
    async fn write(&mut self, batch: TaskStreamResult<RecordBatch>) -> TaskStreamSinkState {
        let batch = match batch {
            Ok(b) => b,
            Err(e) => {
                return TaskStreamSinkState::Error(datafusion::error::DataFusionError::Execution(
                    e.to_string(),
                ));
            }
        };

        self.total_records += batch.num_rows();

        if self.stream_writer.is_none() {
            if let Some(file) = self.writer.take() {
                match datafusion::arrow::ipc::writer::StreamWriter::try_new(file, &batch.schema()) {
                    Ok(sw) => self.stream_writer = Some(sw),
                    Err(e) => {
                        return TaskStreamSinkState::Error(
                            datafusion::error::DataFusionError::Execution(e.to_string()),
                        );
                    }
                }
            } else {
                return TaskStreamSinkState::Closed;
            }
        }

        if let Some(ref mut sw) = self.stream_writer {
            if let Err(e) = sw.write(&batch) {
                return TaskStreamSinkState::Error(datafusion::error::DataFusionError::Execution(
                    e.to_string(),
                ));
            }

            // Forward batch to any waiting receivers
            for sender in &self.senders {
                let _ = sender.try_send(Ok(batch.clone()));
            }

            TaskStreamSinkState::Ok
        } else {
            TaskStreamSinkState::Closed
        }
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        let mut total_bytes = 0u64;
        if let Some(mut sw) = self.stream_writer.take() {
            sw.finish()
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            let file = sw
                .into_inner()
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            if let Ok(metadata) = file.metadata() {
                total_bytes = metadata.len();
            }
        } else if let Some(file) = self.writer.take() {
            drop(file);
        }

        // Write simple index metadata (total bytes, total records) to tmp index file
        let index_content = format!("{}\n{}\n", total_bytes, self.total_records);
        let _ = std::fs::write(&self.tmp_index_path, index_content);

        // Atomically rename temporary files to final destination
        if self.tmp_file_path.exists() {
            let _ = std::fs::rename(&self.tmp_file_path, &self.final_file_path);
        }
        if self.tmp_index_path.exists() {
            let _ = std::fs::rename(&self.tmp_index_path, &self.final_index_path);
        }

        // Drop senders to close channels for any pending receivers
        self.senders.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::sync::Arc;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_disk_stream_round_trip() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("sail_test_{}", rand::random::<u64>()));
        std::fs::create_dir_all(&temp_dir)?;
        let file_path = temp_dir.join("test_stream.data");
        let mut disk_stream = DiskStream::new(file_path.clone());

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )?;

        let mut sink = disk_stream.publish().unwrap();
        let state = sink.write(Ok(batch.clone())).await;
        assert!(matches!(state, TaskStreamSinkState::Ok));
        sink.close().await?;

        assert!(file_path.exists());
        let index_path = disk_stream.index_path();
        assert!(index_path.exists());

        // Check index file content (bytes, rows)
        let index_str = std::fs::read_to_string(&index_path)?;
        assert!(index_str.contains("\n5\n"));
        let stats = disk_stream.read_stats();
        assert!(stats.is_some());
        let (bytes, records) = stats.unwrap();
        assert!(bytes > 0);
        assert_eq!(records, 5);

        // Subscribe and read back
        let mut source = disk_stream.subscribe().unwrap();
        let read_batch = source.next().await.unwrap().unwrap();
        assert_eq!(read_batch, batch);
        assert!(source.next().await.is_none());

        let _ = std::fs::remove_dir_all(&temp_dir);

        Ok(())
    }

    #[tokio::test]
    async fn test_disk_stream_multi_batch_and_empty_batch() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("sail_test_mb_{}", rand::random::<u64>()));
        std::fs::create_dir_all(&temp_dir)?;
        let file_path = temp_dir.join("test_stream_mb.data");
        let mut disk_stream = DiskStream::new(file_path.clone());

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![4, 5]))])?;

        let mut sink = disk_stream.publish().unwrap();
        assert!(matches!(
            sink.write(Ok(batch1.clone())).await,
            TaskStreamSinkState::Ok
        ));
        assert!(matches!(
            sink.write(Ok(empty_batch.clone())).await,
            TaskStreamSinkState::Ok
        ));
        assert!(matches!(
            sink.write(Ok(batch2.clone())).await,
            TaskStreamSinkState::Ok
        ));
        sink.close().await?;

        let (bytes, records) = disk_stream.read_stats().unwrap();
        assert!(bytes > 0);
        assert_eq!(records, 5); // 3 + 0 + 2

        let mut source = disk_stream.subscribe().unwrap();
        let r1 = source.next().await.unwrap().unwrap();
        assert_eq!(r1, batch1);
        let r2 = source.next().await.unwrap().unwrap();
        assert_eq!(r2, empty_batch);
        let r3 = source.next().await.unwrap().unwrap();
        assert_eq!(r3, batch2);
        assert!(source.next().await.is_none());

        let _ = std::fs::remove_dir_all(&temp_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_disk_stream_with_pending_senders() -> Result<()> {
        let temp_dir =
            std::env::temp_dir().join(format!("sail_test_senders_{}", rand::random::<u64>()));
        std::fs::create_dir_all(&temp_dir)?;
        let file_path = temp_dir.join("test_senders.data");

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let mut disk_stream = DiskStream::new_with_senders(file_path.clone(), vec![tx]);

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )?;

        let mut sink = disk_stream.publish().unwrap();
        sink.write(Ok(batch.clone())).await;
        sink.close().await?;

        // Receiver from pending senders should get the batch and channel close
        let received = rx.recv().await.unwrap().unwrap();
        assert_eq!(received, batch);
        assert!(rx.recv().await.is_none());

        // Subsequent subscribers can still read from disk
        let mut source = disk_stream.subscribe().unwrap();
        let from_disk = source.next().await.unwrap().unwrap();
        assert_eq!(from_disk, batch);
        assert!(source.next().await.is_none());

        let _ = std::fs::remove_dir_all(&temp_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_disk_stream_empty_file() -> Result<()> {
        let temp_dir =
            std::env::temp_dir().join(format!("sail_test_empty_{}", rand::random::<u64>()));
        std::fs::create_dir_all(&temp_dir)?;
        let file_path = temp_dir.join("test_empty.data");
        let mut disk_stream = DiskStream::new(file_path.clone());

        let sink = disk_stream.publish().unwrap();
        sink.close().await?;

        let (bytes, records) = disk_stream.read_stats().unwrap();
        assert_eq!(bytes, 0);
        assert_eq!(records, 0);

        let mut source = disk_stream.subscribe().unwrap();
        assert!(source.next().await.is_none());

        let _ = std::fs::remove_dir_all(&temp_dir);
        Ok(())
    }
}
