use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use log::warn;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{Actor, ActorContext};
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
use crate::stream_manager::local::{DiskStream, LocalStream, MemoryStream};
use crate::stream_manager::options::StreamManagerOptions;
use crate::stream_manager::shuffle_file::{
    run_blocking_in_background, MapOutputKey, MapOutputWriter,
};
use crate::stream_manager::{LocalStreamState, StreamManager, StreamManagerMessage};

impl StreamManager {
    pub fn new(options: StreamManagerOptions) -> Self {
        Self {
            options,
            local_streams: HashMap::new(),
            map_outputs: HashMap::new(),
        }
    }

    pub fn create_local_stream(
        &mut self,
        key: TaskStreamKey,
        storage: LocalStreamStorage,
        _schema: SchemaRef,
    ) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        let map_output = match storage {
            LocalStreamStorage::Memory { .. } => None,
            // The stream cannot be created again, which is reported as an error below.
            LocalStreamStorage::Disk { .. }
                if matches!(
                    self.local_streams.get(&key),
                    Some(LocalStreamState::Created { .. } | LocalStreamState::Failed { .. })
                ) =>
            {
                None
            }
            LocalStreamStorage::Disk { channels } => Some(self.get_map_output(&key, channels)?),
        };
        let create = |senders: Vec<_>| -> ExecutionResult<_> {
            let mut stream = Self::create_local_stream_with_senders(
                &key,
                storage,
                map_output.clone(),
                senders,
                &self.options,
            )?;
            let sink = stream.publish()?;
            Ok((stream, sink))
        };

        match self.local_streams.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let senders = match entry.get_mut() {
                    LocalStreamState::Created { .. } => {
                        return Err(ExecutionError::InternalError(format!(
                            "local stream {} is already created",
                            TaskStreamKeyDisplay(&key)
                        )));
                    }
                    LocalStreamState::Pending { senders } => senders,
                    LocalStreamState::Failed { cause } => {
                        return Err(ExecutionError::InternalError(format!(
                            "local stream creation has failed for {}: {}",
                            TaskStreamKeyDisplay(&key),
                            TaskStreamError::from(cause.clone())
                        )));
                    }
                };
                match create(senders.clone()) {
                    Ok((stream, sink)) => {
                        *entry.into_mut() = LocalStreamState::Created { stream };
                        self.on_map_output_channel_created(&key, storage);
                        Ok(sink)
                    }
                    Err(e) => {
                        let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                        Self::fail_senders(senders, &cause);
                        *entry.into_mut() = LocalStreamState::Failed { cause };
                        Err(e)
                    }
                }
            }
            Entry::Vacant(entry) => match create(vec![]) {
                Ok((stream, sink)) => {
                    entry.insert(LocalStreamState::Created { stream });
                    self.on_map_output_channel_created(&key, storage);
                    Ok(sink)
                }
                Err(e) => {
                    let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                    entry.insert(LocalStreamState::Failed { cause });
                    Err(e)
                }
            },
        }
    }

    pub fn create_remote_stream(
        &mut self,
        _uri: String,
        _key: TaskStreamKey,
        _schema: SchemaRef,
    ) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        Err(ExecutionError::InternalError(
            "not implemented: remote stream".to_string(),
        ))
    }

    pub fn fetch_local_stream<T>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: &TaskStreamKey,
    ) -> ExecutionResult<TaskStreamSource>
    where
        T: Actor,
        T::Message: StreamManagerMessage,
    {
        match self.local_streams.entry(key.clone()) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                LocalStreamState::Created { stream } => stream.subscribe(),
                LocalStreamState::Pending { senders } => {
                    let (tx, rx) = mpsc::channel(self.options.task_stream_buffer);
                    senders.push(tx);
                    // There is no need to probe the pending stream again.
                    Ok(Box::pin(ReceiverStream::new(rx)))
                }
                LocalStreamState::Failed { cause } => Err(ExecutionError::InternalError(format!(
                    "local stream creation has failed for {}: {}",
                    TaskStreamKeyDisplay(key),
                    TaskStreamError::from(cause.clone())
                ))),
            },
            Entry::Vacant(entry) => {
                let map_output = MapOutputKey::from(key);
                if map_output.index_path(&self.options.shuffle_dir).exists() {
                    let data_path = map_output.data_path(&self.options.shuffle_dir);
                    let mut stream = Box::new(DiskStream::existing(data_path, key.channel));
                    let source = stream.subscribe()?;
                    entry.insert(LocalStreamState::Created { stream });
                    return Ok(source);
                }
                let (tx, rx) = mpsc::channel(self.options.task_stream_buffer);
                entry.insert(LocalStreamState::Pending { senders: vec![tx] });
                ctx.send_with_delay(
                    T::Message::probe_pending_local_stream(key.clone()),
                    self.options.task_stream_creation_timeout,
                );
                Ok(Box::pin(ReceiverStream::new(rx)))
            }
        }
    }

    pub fn fetch_remote_stream<T>(
        &mut self,
        _ctx: &mut ActorContext<T>,
        _uri: String,
        _key: &TaskStreamKey,
        _schema: SchemaRef,
    ) -> ExecutionResult<TaskStreamSource>
    where
        T: Actor,
        T::Message: StreamManagerMessage,
    {
        Err(ExecutionError::InternalError(
            "not implemented: fetch remote stream".to_string(),
        ))
    }

    pub fn remove_local_streams(&mut self, job_id: JobId, stage: Option<usize>) {
        let dir = if let Some(stage) = stage {
            self.local_streams
                .retain(|key, _| key.job_id != job_id || key.stage != stage);
            self.map_outputs
                .retain(|key, _| key.job_id != job_id || key.stage != stage);
            self.options
                .shuffle_dir
                .join(format!("{job_id}"))
                .join(format!("{stage}"))
        } else {
            self.local_streams.retain(|key, _| key.job_id != job_id);
            self.map_outputs.retain(|key, _| key.job_id != job_id);
            self.options.shuffle_dir.join(format!("{job_id}"))
        };
        // Removing the directory may take a long time if there are large files,
        // so we do not block the caller (an actor) on it.
        run_blocking_in_background(move || {
            if dir.exists() {
                let _ = std::fs::remove_dir_all(dir);
            }
        });
    }

    pub fn remove_remote_streams<T>(
        &mut self,
        _ctx: &mut ActorContext<T>,
        _job_id: JobId,
        _stage: Option<usize>,
    ) where
        T: Actor,
    {
        warn!("removing remote streams is not implemented");
    }

    pub fn fail_local_stream_if_pending(&mut self, key: &TaskStreamKey) {
        let Some(value) = self.local_streams.get_mut(key) else {
            return;
        };
        if let LocalStreamState::Pending { senders } = value {
            let message = "local stream is not created within the expected time".to_string();
            let cause = CommonErrorCause::Execution(message);
            Self::fail_senders(senders, &cause);
            *value = LocalStreamState::Failed { cause };
        }
    }

    pub fn fail_senders(
        senders: &[mpsc::Sender<TaskStreamResult<RecordBatch>>],
        cause: &CommonErrorCause,
    ) {
        for tx in senders {
            // `try_send` would not fail due to full buffer because we have
            // never sent any data to the channel.
            // So we do not need to spawn a task to send the error asynchronously.
            let _ = tx.try_send(Err(TaskStreamError::from(cause.clone())));
        }
    }

    pub async fn stop(&mut self) {
        self.local_streams.clear();
        self.map_outputs.clear();
        let shuffle_dir = self.options.shuffle_dir.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if shuffle_dir.exists() {
                let _ = std::fs::remove_dir_all(&shuffle_dir);
            }
        })
        .await;
    }

    /// Returns the shared writer for the disk-backed output of the map task.
    fn get_map_output(
        &mut self,
        key: &TaskStreamKey,
        channels: usize,
    ) -> ExecutionResult<Arc<MapOutputWriter>> {
        if key.channel >= channels {
            return Err(ExecutionError::InternalError(format!(
                "invalid channel for {} with {channels} channels",
                TaskStreamKeyDisplay(key)
            )));
        }
        match self.map_outputs.entry(MapOutputKey::from(key)) {
            Entry::Occupied(entry) => Ok(entry.get().0.clone()),
            Entry::Vacant(entry) => {
                let writer = MapOutputWriter::try_new(
                    entry.key().data_path(&self.options.shuffle_dir),
                    channels,
                    self.options.shuffle_write,
                )
                .map_err(|e| ExecutionError::InternalError(e.to_string()))?;
                Ok(entry.insert((Arc::new(writer), 0)).0.clone())
            }
        }
    }

    fn on_map_output_channel_created(&mut self, key: &TaskStreamKey, storage: LocalStreamStorage) {
        let LocalStreamStorage::Disk { channels } = storage else {
            return;
        };
        let map_output = MapOutputKey::from(key);
        if let Some((_, created)) = self.map_outputs.get_mut(&map_output) {
            *created += 1;
            if *created >= channels {
                self.map_outputs.remove(&map_output);
            }
        }
    }

    fn create_local_stream_with_senders(
        key: &TaskStreamKey,
        storage: LocalStreamStorage,
        map_output: Option<Arc<MapOutputWriter>>,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
        options: &StreamManagerOptions,
    ) -> ExecutionResult<Box<dyn LocalStream>> {
        match storage {
            LocalStreamStorage::Memory { replicas } => Ok(Box::new(MemoryStream::new(
                options.task_stream_buffer,
                replicas,
                senders,
            ))),
            LocalStreamStorage::Disk { .. } => {
                let writer = map_output.ok_or_else(|| {
                    ExecutionError::InternalError("missing map output writer".to_string())
                })?;
                let data_path = MapOutputKey::from(key).data_path(&options.shuffle_dir);
                Ok(Box::new(DiskStream::new(
                    data_path,
                    key.channel,
                    writer,
                    senders,
                )))
            }
        }
    }
}

impl From<&TaskStreamKey> for MapOutputKey {
    fn from(key: &TaskStreamKey) -> Self {
        Self {
            job_id: key.job_id,
            stage: key.stage,
            partition: key.partition,
            attempt: key.attempt,
        }
    }
}
