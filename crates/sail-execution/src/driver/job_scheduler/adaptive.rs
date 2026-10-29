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

use datafusion::common::JoinType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, PlanProperties};
use log::{debug, warn};

use crate::driver::actor::{AdaptiveCoalesceOptions, SkewJoinOptions};
use crate::driver::job_scheduler::state::{
    CoalescedPartition, JobDescriptor, StageCoalesce, TaskDescriptor, TaskRegionState, TaskState,
};
use crate::id::JobId;
use crate::job_graph::{InputMode, OutputMode};
use crate::plan::StageInputExec;
use crate::shuffle::ShuffleBackendKind;
use crate::task::definition::FileSlice;

/// Coalesced partitions are never made smaller than this size
/// for the sake of the minimum parallelism.
const MIN_PARTITION_BYTES: u64 = 1024 * 1024;

/// The maximum number of tasks that a skewed partition is split into.
const MAX_SKEW_SPLITS: usize = 64;

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
    // Find the shuffle inputs and their partition sizes.
    let mut inputs = vec![];
    let mut producers = vec![];
    let mut sizes = vec![];
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
        let Some(bytes) = partition_bytes(job, input.stage) else {
            return false;
        };
        inputs.push(i);
        producers.push(input.stage);
        sizes.push(bytes);
    }
    if inputs.is_empty() {
        return false;
    }
    let skew = options.skew_join.as_ref().and_then(|skew| {
        find_skewed_partitions(
            &consumer.plan,
            &inputs,
            &producers,
            &sizes,
            skew,
            options.target_partition_bytes,
        )
    });
    let bytes = (0..partitions)
        .map(|p| sizes.iter().map(|x| x[p]).fold(0u64, u64::saturating_add))
        .collect::<Vec<_>>();
    let total = bytes.iter().fold(0u64, |acc, x| acc.saturating_add(*x));
    let target = effective_target_partition_bytes(
        total,
        options.target_partition_bytes,
        options.min_partitions,
    );
    let specs = build_partition_specs(&bytes, target, skew.as_deref().unwrap_or(&[]));
    let n = specs.len();
    let skewed = specs.iter().filter(|x| x.slice.is_some()).count();
    if skewed == 0 && n >= partitions {
        return false;
    }
    let Some(plan) = rewrite_stage_plan(&consumer.plan, &inputs, partitions, n) else {
        debug!("job {job_id} stage {stage} cannot be coalesced");
        return false;
    };
    debug!(
        "job {job_id} stage {stage} coalesced from {partitions} to {n} partitions \
        ({total} bytes, target {target} bytes, {skewed} skewed partition splits)"
    );
    job.graph.set_stage_plan(stage, plan);
    job.stages[stage]
        .tasks
        .resize_with(n, || TaskDescriptor { attempts: vec![] });
    job.stages[stage].coalesced = Some(StageCoalesce {
        partitions: specs,
        producers,
    });
    job.topology.resize_stage_partitions(stage, n);
    true
}

/// Builds the partition specifications of a coalesced stage. Adjacent partitions are
/// coalesced, except skewed partitions, which are split into multiple partitions that
/// each read a slice of the skewed input.
fn build_partition_specs(
    bytes: &[u64],
    target: u64,
    skew: &[Option<(usize, usize)>],
) -> Vec<CoalescedPartition> {
    let mut specs = vec![];
    let mut start = 0;
    let flush = |specs: &mut Vec<CoalescedPartition>, start: usize, end: usize| {
        for range in coalesce_partitions(&bytes[start..end], target) {
            specs.push(CoalescedPartition {
                range: (range.start + start)..(range.end + start),
                slice: None,
            });
        }
    };
    for p in 0..bytes.len() {
        if let Some(Some((producer, count))) = skew.get(p) {
            flush(&mut specs, start, p);
            for index in 0..*count {
                specs.push(CoalescedPartition {
                    range: p..p + 1,
                    slice: Some((
                        *producer,
                        FileSlice {
                            index,
                            count: *count,
                        },
                    )),
                });
            }
            start = p + 1;
        }
    }
    flush(&mut specs, start, bytes.len());
    specs
}

/// Returns the plan input index and the join side of the shuffle inputs of a partitioned
/// hash join that can be split for skewed partitions.
///
/// The stage plan must consist of a single partitioned hash join whose children read
/// two different shuffle inputs, optionally through projections or filters, and only
/// such operators or partial aggregates can appear above the join.
/// A side can be split when each task reading a slice of that side and the full other
/// side produces disjoint output, which depends on the join type.
fn find_skew_join_inputs(
    plan: &Arc<dyn ExecutionPlan>,
    inputs: &[usize],
) -> Option<[Option<usize>; 2]> {
    fn is_transparent(node: &Arc<dyn ExecutionPlan>) -> bool {
        node.downcast_ref::<ProjectionExec>().is_some()
            || node.downcast_ref::<FilterExec>().is_some()
    }
    fn find_input(node: &Arc<dyn ExecutionPlan>, inputs: &[usize]) -> Option<usize> {
        let mut node = Arc::clone(node);
        loop {
            if let Some(input) = node.downcast_ref::<StageInputExec<usize>>() {
                return inputs.iter().position(|x| x == input.input());
            }
            if !is_transparent(&node) {
                return None;
            }
            let child = match node.children().as_slice() {
                [child] => Arc::clone(child),
                _ => return None,
            };
            node = child;
        }
    }
    let mut node = Arc::clone(plan);
    let join = loop {
        if let Some(join) = node.downcast_ref::<HashJoinExec>() {
            break join;
        }
        let partial = node
            .downcast_ref::<AggregateExec>()
            .is_some_and(|x| matches!(x.mode(), AggregateMode::Partial));
        if !partial && !is_transparent(&node) {
            return None;
        }
        let child = match node.children().as_slice() {
            [child] => Arc::clone(child),
            _ => return None,
        };
        node = child;
    };
    if !matches!(join.partition_mode(), PartitionMode::Partitioned) || join.null_aware {
        return None;
    }
    let left = find_input(join.left(), inputs)?;
    let right = find_input(join.right(), inputs)?;
    if left == right {
        return None;
    }
    let left_ok = matches!(
        join.join_type(),
        JoinType::Inner
            | JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark
    );
    let right_ok = matches!(
        join.join_type(),
        JoinType::Inner
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark
    );
    Some([left_ok.then_some(left), right_ok.then_some(right)])
}

/// Returns, for each partition, the producer stage to slice and the number of slices
/// if the partition is skewed, or [None] if no partition is skewed.
fn find_skewed_partitions(
    plan: &Arc<dyn ExecutionPlan>,
    inputs: &[usize],
    producers: &[usize],
    sizes: &[Vec<u64>],
    options: &SkewJoinOptions,
    target: u64,
) -> Option<Vec<Option<(usize, usize)>>> {
    let sides = find_skew_join_inputs(plan, inputs)?;
    let partitions = sizes.first()?.len();
    let mut output = vec![None; partitions];
    let mut found = false;
    for side in sides.into_iter().flatten() {
        // A producer stage read by both sides cannot be sliced for one side only.
        if producers.iter().filter(|x| **x == producers[side]).count() > 1 {
            continue;
        }
        let mut nonzero = sizes[side]
            .iter()
            .copied()
            .filter(|x| *x > 0)
            .collect::<Vec<_>>();
        if nonzero.is_empty() {
            continue;
        }
        nonzero.sort_unstable();
        let median = nonzero[nonzero.len() / 2];
        let threshold = ((median as f64 * options.factor) as u64).max(options.threshold_bytes);
        for (p, &size) in sizes[side].iter().enumerate() {
            if size > threshold && output[p].is_none() {
                let count = size
                    .div_ceil(target.max(1))
                    .clamp(2, MAX_SKEW_SPLITS as u64) as usize;
                output[p] = Some((producers[side], count));
                found = true;
            }
        }
    }
    found.then_some(output)
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
            skew_join: None,
        };
        coalesce_job_stages(JobId::from(1), &mut job, &options);
        let coalesced = job.stages[consumer]
            .coalesced
            .clone()
            .ok_or_else(|| crate::error::ExecutionError::InternalError("not coalesced".into()))?;
        assert_eq!(
            coalesced
                .partitions
                .iter()
                .map(|x| x.range.clone())
                .collect::<Vec<_>>(),
            vec![0..1, 1..4]
        );
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
    fn test_build_partition_specs_with_skew() {
        let slice = |index| Some((7, FileSlice { index, count: 2 }));
        let specs = build_partition_specs(&[1, 1, 100, 1, 1], 10, &[None, None, Some((7, 2))]);
        assert_eq!(
            specs,
            vec![
                CoalescedPartition {
                    range: 0..2,
                    slice: None
                },
                CoalescedPartition {
                    range: 2..3,
                    slice: slice(0)
                },
                CoalescedPartition {
                    range: 2..3,
                    slice: slice(1)
                },
                CoalescedPartition {
                    range: 3..5,
                    slice: None
                },
            ]
        );
    }

    #[test]
    fn test_find_skew_join_inputs() -> datafusion::common::Result<()> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::NullEquality;
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let input = |i: usize| -> Arc<dyn ExecutionPlan> {
            Arc::new(StageInputExec::new(
                i,
                Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(schema.clone()),
                    Partitioning::UnknownPartitioning(4),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
            ))
        };
        let join = |join_type, mode| -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(HashJoinExec::try_new(
                input(0),
                input(1),
                vec![(
                    Arc::new(Column::new("a", 0)) as _,
                    Arc::new(Column::new("a", 0)) as _,
                )],
                None,
                &join_type,
                None,
                mode,
                NullEquality::NullEqualsNothing,
                false,
            )?))
        };
        let inputs = [0, 1];
        assert_eq!(
            find_skew_join_inputs(&join(JoinType::Inner, PartitionMode::Partitioned)?, &inputs),
            Some([Some(0), Some(1)])
        );
        assert_eq!(
            find_skew_join_inputs(&join(JoinType::Left, PartitionMode::Partitioned)?, &inputs),
            Some([Some(0), None])
        );
        assert_eq!(
            find_skew_join_inputs(
                &join(JoinType::RightAnti, PartitionMode::Partitioned)?,
                &inputs
            ),
            Some([None, Some(1)])
        );
        assert_eq!(
            find_skew_join_inputs(&join(JoinType::Full, PartitionMode::Partitioned)?, &inputs),
            Some([None, None])
        );
        assert_eq!(
            find_skew_join_inputs(&join(JoinType::Inner, PartitionMode::CollectLeft)?, &inputs),
            None
        );
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            Arc::new(datafusion::physical_expr::expressions::Literal::new(
                datafusion::common::ScalarValue::Boolean(Some(true)),
            )),
            join(JoinType::Inner, PartitionMode::Partitioned)?,
        )?);
        assert_eq!(
            find_skew_join_inputs(&filtered, &inputs),
            Some([Some(0), Some(1)])
        );
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
