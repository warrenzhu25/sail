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

/// A disk-backed stream that persists record batches to a data file and reads them back.
pub(crate) struct DiskStream {
    file_path: std::path::PathBuf,
    is_written: bool,
}

impl DiskStream {
    pub fn new(file_path: std::path::PathBuf) -> Self {
        Self {
            file_path,
            is_written: false,
        }
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
        let file = std::fs::File::create(&self.file_path)?;
        Ok(Box::new(DiskStreamWriter {
            writer: Some(file),
            stream_writer: None,
        }))
    }

    fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let file = std::fs::File::open(&self.file_path)?;
        let reader = match datafusion::arrow::ipc::reader::StreamReader::try_new(file, None) {
            Ok(r) => r,
            Err(e) => {
                return Err(ExecutionError::InternalError(format!(
                    "failed to open arrow ipc stream reader: {e}"
                )));
            }
        };

        let batches: Vec<TaskStreamResult<RecordBatch>> = reader
            .map(|r| r.map_err(|e| crate::stream::error::TaskStreamError::Unknown(e.to_string())))
            .collect();

        Ok(Box::pin(futures::stream::iter(batches)))
    }
}

struct DiskStreamWriter {
    writer: Option<std::fs::File>,
    stream_writer: Option<datafusion::arrow::ipc::writer::StreamWriter<std::fs::File>>,
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
            TaskStreamSinkState::Ok
        } else {
            TaskStreamSinkState::Closed
        }
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        if let Some(mut sw) = self.stream_writer.take() {
            sw.finish()
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        }
        Ok(())
    }
}

