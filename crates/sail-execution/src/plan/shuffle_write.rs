use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, RangePartitioning,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions, apply_expression_roots,
};
use futures::StreamExt;
use sail_physical_plan::repartition::RowRoundRobinPartitioner;

use crate::stream::writer::{TaskStreamWriteState, TaskStreamWriter};
use crate::task::stats::TaskShuffleStats;

enum ShufflePartitioner {
    Batch(BatchPartitioner),
    RoundRobin(RowRoundRobinPartitioner),
}

/// The partitioning scheme for shuffle output.
///
/// This captures only the schemes supported by shuffle writes. In particular,
/// row-level partitioning is only valid for round-robin distribution.
#[derive(Debug, Clone)]
pub(crate) enum ShufflePartitioning {
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    Range(RangePartitioning),
    RoundRobinBatch(usize),
    RoundRobinRow(usize),
}

impl ShufflePartitioning {
    fn normalize(self) -> Self {
        match self {
            // An empty hash distribution is equivalent to batch-level
            // round-robin distribution.
            Self::Hash(expressions, partitions) if expressions.is_empty() => {
                Self::RoundRobinBatch(partitions)
            }
            Self::Hash(expressions, partitions) => {
                // https://github.com/apache/arrow-datafusion/issues/5184
                Self::Hash(
                    expressions
                        .into_iter()
                        .filter(|expression| !expression.is::<UnKnownColumn>())
                        .collect(),
                    partitions,
                )
            }
            partitioning => partitioning,
        }
    }

    fn partition_count(&self) -> usize {
        match self {
            Self::Hash(_, partitions) => *partitions,
            Self::Range(partitioning) => partitioning.partition_count(),
            Self::RoundRobinBatch(partitions) | Self::RoundRobinRow(partitions) => *partitions,
        }
    }
}

impl Display for ShufflePartitioning {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Hash(expressions, partitions) => {
                write!(
                    f,
                    "{}",
                    Partitioning::Hash(expressions.clone(), *partitions)
                )
            }
            Self::Range(partitioning) => write!(f, "{partitioning}"),
            Self::RoundRobinBatch(partitions) => {
                write!(f, "{}", Partitioning::RoundRobinBatch(*partitions))
            }
            Self::RoundRobinRow(partitions) => write!(f, "RoundRobinRow({partitions})"),
        }
    }
}

impl ShufflePartitioner {
    fn partition<F>(&mut self, batch: RecordBatch, f: F) -> Result<()>
    where
        F: FnMut(usize, RecordBatch) -> Result<()>,
    {
        match self {
            Self::Batch(partitioner) => partitioner.partition(batch, f),
            Self::RoundRobin(partitioner) => partitioner.partition(batch, f),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShuffleWriteExec {
    plan: Arc<dyn ExecutionPlan>,
    /// The partitioning scheme for the shuffle output.
    /// The partition count for the shuffle output can be different from the
    /// partition count of the input plan.
    partitioning: ShufflePartitioning,
    properties: Arc<PlanProperties>,
    writer: Arc<dyn TaskStreamWriter>,
}

impl ShuffleWriteExec {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        writer: Arc<dyn TaskStreamWriter>,
        partitioning: ShufflePartitioning,
    ) -> Self {
        let partitioning = partitioning.normalize();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(TaskShuffleStats::schema()),
            // The shuffle write plan has the same number of partitions as the input plan.
            // For each partition that are executed, the data is further partitioned according to
            // the shuffle partitioning, resulting in multiple output streams.
            // These output streams are written to locations managed by the worker,
            // while the return value of `.execute()` is a single batch containing
            // the per-channel shuffle statistics (see [`TaskShuffleStats`]).
            Partitioning::UnknownPartitioning(plan.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Self {
            plan,
            partitioning,
            properties,
            writer,
        }
    }
}

impl DisplayAs for ShuffleWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShuffleWriteExec: partitioning={}", self.partitioning,)
    }
}

impl ExecutionPlan for ShuffleWriteExec {
    fn name(&self) -> &str {
        "ShuffleWriteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        match &self.partitioning {
            ShufflePartitioning::Hash(expressions, _) => apply_expression_roots(expressions, f),
            ShufflePartitioning::Range(range) => {
                apply_expression_roots(range.ordering().iter().map(|sort_expr| &sort_expr.expr), f)
            }
            ShufflePartitioning::RoundRobinBatch(_) | ShufflePartitioning::RoundRobinRow(_) => {
                Ok(TreeNodeRecursion::Continue)
            }
        }
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("ShuffleWriteExec should have one child");
        }
        let plan = Arc::clone(&children[0]);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                plan,
                ..self.as_ref().clone()
            })),
            ChildrenPropertiesMode::Recompute => Ok(Arc::new(Self::new(
                plan,
                Arc::clone(&self.writer),
                self.partitioning.clone(),
            ))),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let writer = self.writer.clone();
        let stream = self.plan.execute(partition, context)?;
        // TODO: Support metrics in batch partitioner
        let num_input_partitions = self
            .plan
            .properties()
            .output_partitioning()
            .partition_count();
        let partitioner = match &self.partitioning {
            ShufflePartitioning::Hash(expressions, partitions) => {
                ShufflePartitioner::Batch(BatchPartitioner::try_new(
                    Partitioning::Hash(expressions.clone(), *partitions),
                    Default::default(),
                    partition,
                    num_input_partitions,
                )?)
            }
            ShufflePartitioning::Range(partitioning) => {
                ShufflePartitioner::Batch(BatchPartitioner::try_new(
                    Partitioning::Range(partitioning.clone()),
                    Default::default(),
                    partition,
                    num_input_partitions,
                )?)
            }
            ShufflePartitioning::RoundRobinBatch(partitions) => {
                ShufflePartitioner::Batch(BatchPartitioner::try_new(
                    Partitioning::RoundRobinBatch(*partitions),
                    Default::default(),
                    partition,
                    num_input_partitions,
                )?)
            }
            ShufflePartitioning::RoundRobinRow(partitions) => ShufflePartitioner::RoundRobin(
                RowRoundRobinPartitioner::new(*partitions, partition, num_input_partitions)?,
            ),
        };
        let channels = self.partitioning.partition_count();
        let output = futures::stream::once(async move {
            let stats = shuffle_write(writer, stream, partition, channels, partitioner).await?;
            stats.to_record_batch()
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

async fn shuffle_write(
    writer: Arc<dyn TaskStreamWriter>,
    mut stream: SendableRecordBatchStream,
    partition: usize,
    channels: usize,
    mut partitioner: ShufflePartitioner,
) -> Result<TaskShuffleStats> {
    let mut stats = TaskShuffleStats::new(channels);
    let mut sink = writer.open(partition).await?;
    let result = async {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let mut partitions: Vec<Option<RecordBatch>> = vec![None; channels];
            partitioner.partition(batch, |p, batch| {
                stats.channel_bytes[p] += batch.get_array_memory_size() as u64;
                stats.channel_records[p] += batch.num_rows() as u64;
                partitions[p] = Some(batch);
                Ok(())
            })?;
            if sink.write(partitions).await? == TaskStreamWriteState::Closed {
                return Ok::<_, datafusion::error::DataFusionError>(false);
            }
        }
        Ok(true)
    }
    .await;
    match result {
        Ok(true) => {
            sink.commit().await?;
            Ok(stats)
        }
        Ok(false) => {
            // TODO: model successful early-stop separately from error-triggered aborts
            sink.abort().await?;
            Ok(stats)
        }
        Err(error) => {
            let _ = sink.abort().await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::common::collect;

    use super::*;
    use crate::stream::writer::TaskStreamSink;

    #[derive(Debug, Default)]
    struct RecordingWriter {
        rows: Arc<Mutex<Vec<usize>>>,
    }

    struct RecordingSink {
        rows: Arc<Mutex<Vec<usize>>>,
    }

    #[tonic::async_trait]
    impl TaskStreamWriter for RecordingWriter {
        async fn open(&self, _partition: usize) -> Result<Box<dyn TaskStreamSink>> {
            Ok(Box::new(RecordingSink {
                rows: self.rows.clone(),
            }))
        }
    }

    #[tonic::async_trait]
    impl TaskStreamSink for RecordingSink {
        async fn write(
            &mut self,
            batches: Vec<Option<RecordBatch>>,
        ) -> Result<TaskStreamWriteState> {
            let mut rows = self
                .rows
                .lock()
                .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?;
            rows.resize(batches.len(), 0);
            for (i, batch) in batches.iter().enumerate() {
                rows[i] += batch.as_ref().map_or(0, |b| b.num_rows());
            }
            Ok(TaskStreamWriteState::Active)
        }

        async fn commit(self: Box<Self>) -> Result<()> {
            Ok(())
        }

        async fn abort(self: Box<Self>) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn reports_per_channel_shuffle_stats() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = |n: i32| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from((0..n).collect::<Vec<_>>()))],
            )
        };
        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch(10)?, batch(7)?]], schema.clone(), None)?;
        let writer = Arc::new(RecordingWriter::default());
        let rows = writer.rows.clone();
        let plan = ShuffleWriteExec::new(input, writer, ShufflePartitioning::RoundRobinRow(3));
        assert_eq!(plan.schema(), TaskShuffleStats::schema());
        let batches = collect(plan.execute(0, Arc::new(TaskContext::default()))?).await?;
        assert_eq!(batches.len(), 1);
        let stats = TaskShuffleStats::try_from_record_batch(&batches[0])?
            .ok_or_else(|| datafusion::common::DataFusionError::Internal("no stats".into()))?;
        let written = rows
            .lock()
            .map_err(|e| datafusion::common::DataFusionError::Internal(e.to_string()))?
            .iter()
            .map(|x| *x as u64)
            .collect::<Vec<_>>();
        assert_eq!(stats.channel_records, written);
        assert_eq!(stats.channel_records.iter().sum::<u64>(), 17);
        assert!(stats.channel_bytes.iter().all(|x| *x > 0));
        Ok(())
    }
}
