//! Runtime adaptive coalescing of shuffle partitions.
//!
//! When the storage shuffle backend is used, each shuffle partition is materialized by a
//! blocking "merge" stage before the consumer stage runs. Once all merge stages read by a
//! consumer stage have succeeded, the shuffle statistics reported by the merge tasks tell
//! the size of each shuffle partition. Adjacent small partitions are then combined so that
//! each consumer task reads a contiguous range of shuffle partitions from every input.
//!
//! The decision is made at most once per stage, before any task attempt of the consumer
//! stage is created, so it is never revisited on retries.

use std::ops::Range;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, PlanProperties};
use log::{debug, warn};

use crate::driver::actor::AdaptiveCoalesceOptions;
use crate::driver::job_scheduler::state::{
    JobDescriptor, StageCoalesce, TaskRegionState, TaskState,
};
use crate::id::JobId;
use crate::job_graph::{InputMode, OutputMode};
use crate::plan::StageInputExec;
use crate::shuffle::ShuffleBackendKind;

/// Coalesced partitions are never made smaller than this size
/// for the sake of the minimum parallelism.
const MIN_PARTITION_BYTES: u64 = 1024 * 1024;

/// Combines adjacent partitions into ranges whose total size is at most `target` bytes,
/// unless a single partition is already larger than `target`.
pub(crate) fn coalesce_partitions(bytes: &[u64], target: u64) -> Vec<Range<usize>> {
    let mut ranges = vec![];
    let mut start = 0;
    let mut size = 0u64;
    for (i, &b) in bytes.iter().enumerate() {
        if i > start && size.saturating_add(b) > target {
            ranges.push(start..i);
            start = i;
            size = 0;
        }
        size = size.saturating_add(b);
    }
    if start < bytes.len() {
        ranges.push(start..bytes.len());
    }
    ranges
}

/// Returns the target partition size, reduced so that roughly `min_partitions` partitions
/// are produced, but never below [`MIN_PARTITION_BYTES`].
pub(crate) fn effective_target_partition_bytes(
    total: u64,
    target: u64,
    min_partitions: usize,
) -> u64 {
    if min_partitions <= 1 {
        return target.max(1);
    }
    target
        .min((total / min_partitions as u64).max(MIN_PARTITION_BYTES))
        .max(1)
}

/// Returns whether the stage is a blocking storage merge stage that materializes
/// shuffle partitions, i.e. a stage created by `create_shuffle_input` in the planner
/// for a shuffle consumed by a single task per partition.
fn is_shuffle_merge_stage(job: &JobDescriptor, stage: usize) -> bool {
    let stage = &job.graph.stages()[stage];
    matches!(stage.mode, OutputMode::Blocking)
        && matches!(stage.inputs.as_slice(), [input] if matches!(input.mode, InputMode::Shuffle))
        && stage.plan.downcast_ref::<StageInputExec<usize>>().is_some()
}

/// Returns the output size in bytes of each partition of the stage,
/// or [None] if any task has not succeeded with shuffle statistics.
fn partition_bytes(job: &JobDescriptor, stage: usize) -> Option<Vec<u64>> {
    job.stages[stage]
        .tasks
        .iter()
        .map(|task| {
            let attempt = task.attempts.last()?;
            if !matches!(attempt.state, TaskState::Succeeded) {
                return None;
            }
            Some(attempt.shuffle_stats.as_ref()?.channel_bytes.iter().sum())
        })
        .collect()
}

fn with_partition_count(partitioning: &Partitioning, n: usize) -> Partitioning {
    match partitioning {
        Partitioning::Hash(keys, _) => Partitioning::Hash(keys.clone(), n),
        Partitioning::RoundRobinBatch(_) => Partitioning::RoundRobinBatch(n),
        _ => Partitioning::UnknownPartitioning(n),
    }
}

/// Rewrites the plan of a consumer stage so that the stage inputs at the given indices
/// have `n` partitions. Returns [None] if the plan cannot be rewritten safely.
fn rewrite_stage_plan(
    plan: &Arc<dyn ExecutionPlan>,
    inputs: &[usize],
    partitions: usize,
    n: usize,
) -> Option<Arc<dyn ExecutionPlan>> {
    // Every leaf of the plan must be a stage input, and every stage input that is not
    // coalesced must not have the same partition count as the stage, so that no part of
    // the plan still expects the original partitioning after the rewrite.
    let mut safe = true;
    let _ = plan.apply(|node| {
        if let Some(input) = node.downcast_ref::<StageInputExec<usize>>() {
            if !inputs.contains(input.input())
                && node.output_partitioning().partition_count() == partitions
            {
                safe = false;
            }
        } else if node.children().is_empty() {
            safe = false;
        }
        Ok(if safe {
            datafusion::common::tree_node::TreeNodeRecursion::Continue
        } else {
            datafusion::common::tree_node::TreeNodeRecursion::Stop
        })
    });
    if !safe {
        return None;
    }
    let result = Arc::clone(plan).transform_up(|node| {
        let Some(input) = node.downcast_ref::<StageInputExec<usize>>() else {
            return Ok(Transformed::no(node));
        };
        if !inputs.contains(input.input()) {
            return Ok(Transformed::no(node));
        }
        let properties = input.properties();
        let partitioning = with_partition_count(properties.output_partitioning(), n);
        let properties = PlanProperties::clone(properties).with_partitioning(partitioning);
        Ok(Transformed::yes(
            Arc::new(StageInputExec::new(*input.input(), Arc::new(properties)))
                as Arc<dyn ExecutionPlan>,
        ))
    });
    let plan = match result {
        Ok(x) => x.data,
        Err(e) => {
            warn!("failed to rewrite stage plan for coalescing: {e}");
            return None;
        }
    };
    (plan.output_partitioning().partition_count() == n).then_some(plan)
}

/// Tries to coalesce the partitions of the given consumer stage.
/// Returns `true` if the stage has been coalesced.
fn coalesce_stage(
    job_id: JobId,
    job: &mut JobDescriptor,
    stage: usize,
    options: &AdaptiveCoalesceOptions,
) -> bool {
    if job.stages[stage].coalesced.is_some() {
        return false;
    }
    let consumer = &job.graph.stages()[stage];
    let partitions = consumer.plan.output_partitioning().partition_count();
    if partitions <= 1 {
        return false;
    }
    // The consumers of this stage must not depend on its partition count.
    let consumers = &job.topology.stages[stage].consumers;
    if consumers.is_empty()
        || consumers.iter().any(|&c| {
            job.graph.stages()[c]
                .inputs
                .iter()
                .filter(|x| x.stage == stage)
                .any(|x| !matches!(x.mode, InputMode::Shuffle | InputMode::Broadcast))
        })
    {
        return false;
    }
    // Find the shuffle inputs and add up the partition sizes.
    let mut inputs = vec![];
    let mut producers = vec![];
    let mut bytes = vec![0u64; partitions];
    for (i, input) in consumer.inputs.iter().enumerate() {
        let producer = &job.graph.stages()[input.stage];
        if !matches!(producer.mode, OutputMode::Blocking) {
            // Pipelined inputs run concurrently with this stage.
            return false;
        }
        if !matches!(input.mode, InputMode::Forward)
            || !is_shuffle_merge_stage(job, input.stage)
            || producer.plan.output_partitioning().partition_count() != partitions
        {
            continue;
        }
        let Some(sizes) = partition_bytes(job, input.stage) else {
            return false;
        };
        for (acc, b) in bytes.iter_mut().zip(sizes) {
            *acc = acc.saturating_add(b);
        }
        inputs.push(i);
        producers.push(input.stage);
    }
    if inputs.is_empty() {
        return false;
    }
    let total = bytes.iter().sum::<u64>();
    let target = effective_target_partition_bytes(
        total,
        options.target_partition_bytes,
        options.min_partitions,
    );
    let ranges = coalesce_partitions(&bytes, target);
    if ranges.len() >= partitions {
        return false;
    }
    let Some(plan) = rewrite_stage_plan(&consumer.plan, &inputs, partitions, ranges.len()) else {
        debug!("job {job_id} stage {stage} cannot be coalesced");
        return false;
    };
    debug!(
        "job {job_id} stage {stage} coalesced from {partitions} to {} partitions \
        ({total} bytes, target {target} bytes)",
        ranges.len()
    );
    let n = ranges.len();
    job.graph.set_stage_plan(stage, plan);
    job.stages[stage].tasks.truncate(n);
    job.stages[stage].coalesced = Some(StageCoalesce { ranges, producers });
    job.topology.truncate_stage_partitions(stage, n);
    true
}

/// Coalesces the partitions of stages in task regions that are about to be scheduled
/// for the first time.
pub(crate) fn coalesce_job_stages(
    job_id: JobId,
    job: &mut JobDescriptor,
    options: &AdaptiveCoalesceOptions,
) {
    if !matches!(
        job.graph.shuffle_backend(),
        ShuffleBackendKind::Storage { .. }
    ) {
        return;
    }
    let mut candidates = vec![];
    for (r, region) in job.topology.regions.iter().enumerate() {
        let ready = region
            .dependencies
            .iter()
            .all(|d| matches!(job.regions[*d].state, TaskRegionState::Succeeded));
        let fresh = region
            .tasks
            .iter()
            .all(|t| job.stages[t.stage].tasks[t.partition].attempts.is_empty());
        if !ready || !fresh {
            continue;
        }
        for t in &region.tasks {
            if t.partition != 0 {
                continue;
            }
            // All partitions of the stage must belong to this region.
            let partitions = job.stages[t.stage].tasks.len();
            let count = region.tasks.iter().filter(|x| x.stage == t.stage).count();
            if count == partitions {
                candidates.push((r, t.stage));
            }
        }
    }
    for (_, stage) in candidates {
        coalesce_stage(job_id, job, stage, options);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coalesce_partitions() {
        assert_eq!(coalesce_partitions(&[], 10), Vec::<Range<usize>>::new());
        assert_eq!(coalesce_partitions(&[1, 2, 3, 4], 100), vec![0..4]);
        assert_eq!(coalesce_partitions(&[5, 5, 5, 5], 10), vec![0..2, 2..4]);
        assert_eq!(
            coalesce_partitions(&[20, 1, 1, 20], 10),
            vec![0..1, 1..3, 3..4]
        );
        assert_eq!(coalesce_partitions(&[0, 0, 0], 1), vec![0..3]);
    }

    #[test]
    fn test_coalesce_job_stages() -> crate::error::ExecutionResult<()> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::TaskContext;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::repartition::RepartitionExec;

        use crate::driver::job_scheduler::state::{JobState, TaskAttemptDescriptor};
        use crate::job_graph::{JobGraph, JobGraphOptions};
        use crate::shuffle::ShuffleCompression;
        use crate::task::stats::TaskShuffleStats;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let hash = |n| Partitioning::Hash(vec![Arc::new(Column::new("a", 0)) as _], n);
        let input = MemorySourceConfig::try_new_exec(&vec![vec![]; 4], schema, None)?;
        let plan = Arc::new(RepartitionExec::try_new(
            Arc::new(RepartitionExec::try_new(input, hash(4))?),
            hash(2),
        )?);
        let graph = JobGraph::try_new(
            plan,
            JobGraphOptions {
                shuffle_backend: ShuffleBackendKind::Storage {
                    path: None,
                    max_file_size: 1,
                    compression: ShuffleCompression::None,
                },
            },
        )?;
        let mut job =
            JobDescriptor::try_new(graph, JobState::Draining, Arc::new(TaskContext::default()))?;
        // stage 0: map, stage 1: merge, stage 2: map reading the merged partitions, ...
        assert!(is_shuffle_merge_stage(&job, 1));
        let consumer = 2;
        assert_eq!(job.stages[consumer].tasks.len(), 4);
        for (stage, bytes) in [(0, vec![1, 1, 1, 1]), (1, vec![10, 10, 1, 1])] {
            for (task, b) in job.stages[stage].tasks.iter_mut().zip(bytes) {
                task.attempts.push(TaskAttemptDescriptor {
                    state: TaskState::Succeeded,
                    messages: vec![],
                    cause: None,
                    job_output_fetched: false,
                    shuffle_stats: Some(TaskShuffleStats {
                        channel_bytes: vec![b],
                        channel_records: vec![1],
                    }),
                });
            }
        }
        for (r, region) in job.topology.regions.iter().enumerate() {
            if region.tasks.iter().all(|t| t.stage <= 1) {
                job.regions[r].state = TaskRegionState::Succeeded;
            }
        }
        let options = AdaptiveCoalesceOptions {
            target_partition_bytes: 12,
            min_partitions: 0,
        };
        coalesce_job_stages(JobId::from(1), &mut job, &options);
        let coalesced = job.stages[consumer]
            .coalesced
            .clone()
            .ok_or_else(|| crate::error::ExecutionError::InternalError("not coalesced".into()))?;
        assert_eq!(coalesced.ranges, vec![0..1, 1..4]);
        assert_eq!(coalesced.producers, vec![1]);
        assert_eq!(job.stages[consumer].tasks.len(), 2);
        assert_eq!(
            job.graph.stages()[consumer]
                .plan
                .output_partitioning()
                .partition_count(),
            2
        );
        let tasks = job
            .topology
            .regions
            .iter()
            .flat_map(|r| &r.tasks)
            .filter(|t| t.stage == consumer)
            .count();
        assert_eq!(tasks, 2);
        // The decision is never revisited.
        coalesce_job_stages(JobId::from(1), &mut job, &options);
        assert_eq!(job.stages[consumer].tasks.len(), 2);
        Ok(())
    }

    #[test]
    fn test_effective_target_partition_bytes() {
        let mib = 1024 * 1024;
        assert_eq!(
            effective_target_partition_bytes(1000 * mib, 64 * mib, 0),
            64 * mib
        );
        assert_eq!(
            effective_target_partition_bytes(1000 * mib, 64 * mib, 100),
            10 * mib
        );
        assert_eq!(
            effective_target_partition_bytes(10 * mib, 64 * mib, 100),
            mib
        );
    }
}
