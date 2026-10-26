use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{exec_datafusion_err, internal_err, DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::future::try_join_all;
use futures::TryStreamExt;
use log::warn;
use sail_common_datafusion::error::ShuffleFetchError;

use crate::plan::ListListDisplay;
use crate::stream::error::TaskStreamError;
use crate::stream::merge::MergedRecordBatchStream;
use crate::stream::reader::{TaskReadLocation, TaskStreamReader, TaskStreamSource};

#[derive(Debug, Clone)]
pub struct ShuffleReadExec {
    /// For each output partition, a list of locations to read from.
    locations: Vec<Vec<TaskReadLocation>>,
    properties: Arc<PlanProperties>,
    reader: Arc<dyn TaskStreamReader>,
}

impl ShuffleReadExec {
    pub fn new(
        locations: Vec<Vec<TaskReadLocation>>,
        reader: Arc<dyn TaskStreamReader>,
        properties: Arc<PlanProperties>,
    ) -> Self {
        Self {
            locations,
            properties,
            reader,
        }
    }
}

impl DisplayAs for ShuffleReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleReadExec: partitioning={}, locations={}",
            self.properties.output_partitioning(),
            ListListDisplay(&self.locations)
        )
    }
}

impl ExecutionPlan for ShuffleReadExec {
    fn name(&self) -> &str {
        "ShuffleReadExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("ShuffleReadExec does not accept children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let locations = self
            .locations
            .get(partition)
            .ok_or_else(|| {
                exec_datafusion_err!("read locations for partition {partition} not found")
            })?
            .clone();
        if locations.is_empty() {
            warn!("empty read locations for partition {partition}");
        }
        let reader = self.reader.clone();
        let output_schema = self.schema();
        let output =
            futures::stream::once(
                async move { shuffle_read(reader, &locations, output_schema).await },
            )
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

async fn shuffle_read(
    reader: Arc<dyn TaskStreamReader>,
    locations: &[TaskReadLocation],
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let futures = locations.iter().map(|location| {
        let reader = reader.clone();
        let schema = schema.clone();
        async move {
            let stream = reader
                .open(location, schema)
                .await
                .map_err(|e| shuffle_fetch_datafusion_error(location, e))?;
            // Attach the location to errors that occur while reading the stream,
            // so that the driver can recompute the upstream task output if needed.
            let location = location.clone();
            let stream: TaskStreamSource =
                Box::pin(stream.map_err(move |e| shuffle_fetch_stream_error(&location, e)));
            Ok::<TaskStreamSource, DataFusionError>(stream)
        }
    });
    let streams = try_join_all(futures).await?;
    Ok(Box::pin(MergedRecordBatchStream::new(schema, streams)))
}

fn shuffle_fetch_datafusion_error(
    location: &TaskReadLocation,
    error: DataFusionError,
) -> DataFusionError {
    if has_shuffle_fetch_error(&error) {
        return error;
    }
    let message = error.to_string();
    DataFusionError::External(Box::new(build_shuffle_fetch_error(location, message)))
}

fn shuffle_fetch_stream_error(
    location: &TaskReadLocation,
    error: TaskStreamError,
) -> TaskStreamError {
    if has_shuffle_fetch_error(&error) {
        return error;
    }
    let message = error.to_string();
    TaskStreamError::External(Arc::new(build_shuffle_fetch_error(location, message)))
}

fn build_shuffle_fetch_error(location: &TaskReadLocation, message: String) -> ShuffleFetchError {
    let (worker_id, key) = match location {
        TaskReadLocation::Driver { key } => (None, key),
        TaskReadLocation::Worker { worker_id, key } => (Some(u64::from(*worker_id)), key),
        TaskReadLocation::Remote { key, .. } => (None, key),
    };
    let unreachable = is_unreachable_message(&message);
    ShuffleFetchError {
        worker_id,
        job_id: key.job_id.into(),
        stage: key.stage as u64,
        partition: key.partition as u64,
        attempt: key.attempt as u64,
        channel: key.channel as u64,
        unreachable,
        message,
    }
}

fn has_shuffle_fetch_error(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(e) = current {
        if e.downcast_ref::<ShuffleFetchError>().is_some() {
            return true;
        }
        if let Some(DataFusionError::External(inner)) = e.downcast_ref::<DataFusionError>() {
            current = Some(inner.as_ref());
            continue;
        }
        current = e.source();
    }
    false
}

/// Returns whether the error message indicates that the worker holding the stream is unreachable.
fn is_unreachable_message(message: &str) -> bool {
    let message = message.to_lowercase();
    [
        "transport error",
        "connection refused",
        "connection reset",
        "unknown peer worker",
        "broken pipe",
        "dns error",
    ]
    .iter()
    .any(|x| message.contains(x))
}
