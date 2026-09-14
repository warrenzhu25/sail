use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use indexmap::{IndexMap, IndexSet};
use log::{debug, warn};
use prost::Message;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::ActorContext;

use crate::driver::job_scheduler::adaptive::StageShuffleStats;
use crate::driver::job_scheduler::state::{
    JobDescriptor, JobState, StageState, TaskAttemptDescriptor, TaskDescriptor,
    TaskRegionDescriptor, TaskRegionState, TaskState,
};
use crate::driver::job_scheduler::topology::{JobTopology, TaskRegionTopology};
use crate::driver::job_scheduler::{JobAction, JobScheduler, JobSchedulerOptions};
use crate::driver::output::build_job_output;
use crate::driver::DriverActor;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey};
use crate::job_graph::{
    InputMode, JobGraph, OutputDistribution, OutputMode, Stage, StageInput, TaskPlacement,
};
use crate::plan::StageInputExec;
use crate::task::definition::{
    TaskDefinition, TaskInput, TaskInputKey, TaskInputLocator, TaskOutput, TaskOutputDistribution,
    TaskOutputLocator,
};
use crate::task::scheduling::{
    TaskAssignment, TaskAssignmentGetter, TaskOutputKind, TaskRegion, TaskSet, TaskSetEntry,
};

impl JobScheduler {
    fn next_job_id(&mut self) -> ExecutionResult<JobId> {
        self.job_id_generator.next()
    }

    pub fn accept_job(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<(JobId, SendableRecordBatchStream)> {
        let job_id = self.next_job_id()?;

        debug!(
            "job {job_id} execution plan\n{}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let graph = JobGraph::try_new_with_mode(plan, self.options.shuffle_mode)?;
        debug!("job {job_id} job graph \n{graph}");

        let (output, stream) = build_job_output(ctx, job_id, graph.schema().clone());
        let descriptor = JobDescriptor::try_new(graph, JobState::Running { output, context })?;
        self.jobs.insert(job_id, descriptor);

        Ok((job_id, stream))
    }

    pub fn update_task(
        &mut self,
        key: &TaskKey,
        state: TaskState,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) {
        let Some(attempt) = self
            .jobs
            .get_mut(&key.job_id)
            .and_then(|job| job.stages.get_mut(key.stage))
            .and_then(|stage| stage.tasks.get_mut(key.partition))
            .and_then(|task| task.attempts.get_mut(key.attempt))
        else {
            warn!("{} not found", TaskKeyDisplay(key));
            return;
        };
        attempt.state = attempt.state.consolidate(state);
        if attempt.state.is_terminal() && attempt.stopped_at.is_none() {
            attempt.stopped_at = Some(Utc::now());
        }
        attempt.messages.extend(message);
        if let Some(cause) = cause {
            attempt.cause = Some(cause);
        }
    }

    pub fn get_task_state(&self, key: &TaskKey) -> Option<TaskState> {
        let attempt = self
            .jobs
            .get(&key.job_id)
            .and_then(|job| job.stages.get(key.stage))
            .and_then(|stage| stage.tasks.get(key.partition))
            .and_then(|task| task.attempts.get(key.attempt));
        if attempt.is_none() {
            warn!("{} not found", TaskKeyDisplay(key));
        };
        attempt.map(|x| x.state)
    }

    /// Determine the actions needed in the driver for the job whose
    /// task states may have changed.
    ///
    /// The method first determines the task regions and then decides
    /// the actions to take.
    ///   1. For each task region, if any task attempt fails, all existing task attempts
    ///      in the region are canceled if not already.
    ///   2. If any task in the final stages is running or has succeeded, all its channels are
    ///      added as job output streams if not already.
    ///   3. For each stage, if all the stages that consume it have succeeded, remove
    ///      the output streams of the stage if not already.
    ///   4. If any task exceeds the maximum allowed attempts, the task region and the job
    ///      are marked as failed.
    ///   5. If all the tasks in the final stages have succeeded, the job is marked as succeeded.
    ///   6. For each task region, schedule the tasks of the region if all the dependency
    ///      regions have succeeded.
    pub fn refresh_job(&mut self, job_id: JobId) -> Vec<JobAction> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        if !matches!(job.state, JobState::Running { .. }) {
            return vec![];
        }

        let mut actions = vec![];

        actions.extend(Self::cascade_cancel_task_attempts(job_id, job));
        actions.extend(Self::extend_job_output(job_id, job));
        actions.extend(Self::clean_up_job_by_stage(job_id, job));

        Self::update_task_regions(job, &self.options);

        if job
            .regions
            .iter()
            .any(|x| matches!(x.state, TaskRegionState::Failed))
        {
            let cause = Self::infer_job_failure_cause(job);
            if let JobState::Running { output, .. } = &job.state {
                actions.push(JobAction::FailJobOutput {
                    handle: output.handle(),
                    cause,
                })
            }
            job.state = JobState::Failed;
            job.stopped_at = Some(Utc::now());
            return actions;
        }

        if job
            .regions
            .iter()
            .all(|x| matches!(x.state, TaskRegionState::Succeeded))
        {
            // This drops `JobOutputManager` in the job state,
            // so that `JobOutputStream` turns to the draining state as well.
            job.state = JobState::Draining;
            return actions;
        }

        actions.extend(Self::schedule_task_regions(&self.options, job_id, job));

        actions
    }

    fn update_task_regions(job: &mut JobDescriptor, options: &JobSchedulerOptions) {
        for (r, region) in job.topology.regions.iter().enumerate() {
            let failed = region.tasks.iter().any(|t| {
                let attempts = &job.stages[t.stage].tasks[t.partition].attempts;
                if let Some(attempt) = attempts.last() {
                    if matches!(attempt.state, TaskState::Failed | TaskState::Canceled)
                        && attempts.len() >= options.task_max_attempts
                    {
                        return true;
                    }
                }
                false
            });
            if failed {
                job.regions[r].state = TaskRegionState::Failed;
                continue;
            }
            let succeeded = region.tasks.iter().all(|t| {
                job.stages[t.stage].tasks[t.partition]
                    .attempts
                    .last()
                    .is_some_and(|x| matches!(x.state, TaskState::Succeeded))
            });
            if succeeded {
                job.regions[r].state = TaskRegionState::Succeeded;
            } else {
                job.regions[r].state = TaskRegionState::Running;
            }
        }
    }

    fn cascade_cancel_task_attempts(job_id: JobId, job: &mut JobDescriptor) -> Vec<JobAction> {
        let mut actions = vec![];

        for region in &job.topology.regions {
            let mut failed = false;

            for t in &region.tasks {
                let attempts = &job.stages[t.stage].tasks[t.partition].attempts;
                if let Some(attempt) = attempts.last() {
                    if matches!(attempt.state, TaskState::Failed) {
                        failed = true;
                    }
                }
            }

            if failed {
                // cancel all other tasks in this region
                for t in &region.tasks {
                    let task = &mut job.stages[t.stage].tasks[t.partition];
                    for (a, attempt) in task.attempts.iter_mut().enumerate() {
                        if !attempt.state.is_terminal() {
                            attempt.state = TaskState::Canceled;
                            attempt.stopped_at = Some(Utc::now());
                            actions.push(JobAction::CancelTask {
                                key: TaskKey {
                                    job_id,
                                    stage: t.stage,
                                    partition: t.partition,
                                    attempt: a,
                                },
                            });
                        }
                    }
                }
            }
        }

        actions
    }

    fn clean_up_job_by_stage(job_id: JobId, job: &mut JobDescriptor) -> Vec<JobAction> {
        let mut actions = vec![];

        for (s, stage) in job.topology.stages.iter().enumerate() {
            if matches!(job.stages[s].state, StageState::Inactive) {
                continue;
            }
            let all_consumers_succeeded = stage.consumers.iter().all(|&c| {
                !job.stages[c].tasks.is_empty()
                    && job.stages[c].tasks.iter().all(|t| {
                        t.attempts
                            .last()
                            .is_some_and(|a| matches!(a.state, TaskState::Succeeded))
                    })
            });

            if all_consumers_succeeded && !stage.consumers.is_empty() {
                job.stages[s].state = StageState::Inactive;
                job.stages[s].stopped_at = Some(Utc::now());
                actions.push(JobAction::CleanUpJob {
                    job_id,
                    stage: Some(s),
                });
            }
        }

        actions
    }

    fn schedule_task_regions(
        options: &JobSchedulerOptions,
        job_id: JobId,
        job: &mut JobDescriptor,
    ) -> Vec<JobAction> {
        let mut actions = vec![];

        if options.adaptive_enabled {
            Self::optimize_stages_adaptively(options, job_id, job);
        }

        for r in 0..job.topology.regions.len() {
            if matches!(job.regions[r].state, TaskRegionState::Succeeded) {
                continue;
            }

            if !job.topology.regions[r]
                .dependencies
                .iter()
                .all(|d| matches!(job.regions[*d].state, TaskRegionState::Succeeded))
            {
                // The region has dependencies that are not yet succeeded.
                continue;
            }

            if job.topology.regions[r].tasks.iter().any(|t| {
                job.stages[t.stage].tasks[t.partition]
                    .attempts
                    .last()
                    .is_some_and(|x| !x.state.is_terminal())
            }) {
                // The latest task attempt is still active, so the region must have been
                // scheduled already.
                continue;
            }

            for t in &job.topology.regions[r].tasks {
                job.stages[t.stage].tasks[t.partition]
                    .attempts
                    .push(TaskAttemptDescriptor {
                        state: TaskState::Created,
                        messages: vec![],
                        cause: None,
                        job_output_fetched: false,
                        created_at: Utc::now(),
                        stopped_at: None,
                    });
            }

            actions.push(JobAction::ScheduleTaskRegion {
                region: Self::build_task_region(job_id, job, &job.topology.regions[r]),
            });
        }

        actions
    }

    fn optimize_stages_adaptively(
        options: &JobSchedulerOptions,
        job_id: JobId,
        job: &mut JobDescriptor,
    ) {
        let mut topology_changed = false;

        for s in 0..job.graph.stages().len() {
            let inputs = job.graph.stages()[s].inputs.clone();
            let shuffle_inputs: Vec<(usize, crate::job_graph::StageInput)> = inputs
                .iter()
                .cloned()
                .enumerate()
                .filter(|(_, input)| matches!(input.mode, InputMode::Shuffle))
                .collect();

            if shuffle_inputs.is_empty() {
                continue;
            }

            if shuffle_inputs
                .iter()
                .any(|(_, input)| input.partition_ranges.is_some())
            {
                continue;
            }

            let all_upstream_ready = shuffle_inputs.iter().all(|(_, input)| {
                let u = input.stage;
                matches!(job.graph.stages()[u].mode, OutputMode::Blocking)
                    && job.stages[u].tasks.iter().all(|t| {
                        t.attempts
                            .last()
                            .is_some_and(|a| matches!(a.state, TaskState::Succeeded))
                    })
            });

            if !all_upstream_ready {
                continue;
            }

            let upstream_channels = {
                let first_u = shuffle_inputs[0].1.stage;
                job.graph.stages()[first_u].distribution.channels()
            };

            let mut combined_channel_bytes = vec![0u64; upstream_channels];
            for (_, input) in &shuffle_inputs {
                let u = input.stage;
                let upstream_partitions = job.stages[u].tasks.len();
                let upstream_channels = job.graph.stages()[u].distribution.channels();
                let upstream_attempts: Vec<usize> = (0..upstream_partitions)
                    .map(|p| Self::get_latest_task_attempt(job, u, p).unwrap_or(0))
                    .collect();

                let stats = StageShuffleStats::from_disk(
                    &options.shuffle_dir,
                    job_id,
                    u,
                    upstream_partitions,
                    upstream_channels,
                    &upstream_attempts,
                );

                let skewed =
                    stats.detect_skew_partitions(options.skew_factor, options.min_skew_threshold);
                if !skewed.is_empty() {
                    debug!("job {job_id} stage {s} upstream stage {u} detected skewed channels: {skewed:?}");
                }

                if stats.should_broadcast_join(10 * 1024 * 1024) {
                    debug!("job {job_id} stage {s} upstream stage {u} candidate for broadcast join (total: {} bytes)", stats.total_bytes);
                }

                for (c, &b) in stats.channel_bytes.iter().enumerate() {
                    if c < combined_channel_bytes.len() {
                        combined_channel_bytes[c] += b;
                    }
                }
            }

            let mut ranges = crate::driver::job_scheduler::adaptive::coalesce_shuffle_partitions(
                &combined_channel_bytes,
                options.target_partition_size,
            );
            if ranges.is_empty() {
                ranges = (0..upstream_channels).map(|c| c..c + 1).collect();
            }
            let new_partition_count = ranges.len();

            if new_partition_count < upstream_channels {
                debug!(
                    "job {job_id} stage {s}: adaptively coalesced {upstream_channels} channels into {new_partition_count} partitions (ranges: {ranges:?})"
                );

                for (input_idx, _) in &shuffle_inputs {
                    job.graph.stages_mut()[s].inputs[*input_idx].partition_ranges =
                        Some(ranges.clone());
                }

                if let Ok(new_plan) = update_stage_plan_partitioning(
                    job.graph.stages()[s].plan.clone(),
                    new_partition_count,
                ) {
                    job.graph.stages_mut()[s].plan = new_plan;
                }

                job.stages[s].tasks = (0..new_partition_count)
                    .map(|_| TaskDescriptor { attempts: vec![] })
                    .collect();

                topology_changed = true;
            }
        }

        if topology_changed {
            if let Ok(new_topology) = JobTopology::try_new(&job.graph) {
                job.topology = new_topology;
                job.regions.resize(
                    job.topology.regions.len(),
                    TaskRegionDescriptor {
                        state: TaskRegionState::Running,
                    },
                );
                Self::update_task_regions(job, options);
            }
        }
    }

    fn build_task_region(
        job_id: JobId,
        job: &JobDescriptor,
        region: &TaskRegionTopology,
    ) -> TaskRegion {
        let stages = region
            .tasks
            .iter()
            .map(|t| t.stage)
            .collect::<IndexSet<_>>();
        let mut stage_groups: IndexMap<StageGroupKey, StageGroup> = IndexMap::new();
        for s in stages {
            let stage = &job.graph.stages()[s];
            let key = StageGroupKey {
                placement: stage.placement,
                group: stage.group.clone(),
            };
            let group: &mut StageGroup = stage_groups.entry(key).or_default();
            group.stages.insert(s);
            let n = group.buckets.len();
            let p = stage.plan.output_partitioning().partition_count();
            // ensure that the number of buckets is the maximum partitions among the stages
            if p > n {
                group.buckets.resize(p, Vec::new());
            }
        }

        for t in &region.tasks {
            if let Some(attempt) = Self::get_latest_task_attempt(job, t.stage, t.partition) {
                let stage = &job.graph.stages()[t.stage];
                let output = match stage.mode {
                    OutputMode::Pipelined | OutputMode::Blocking => TaskOutputKind::Local,
                };
                let key = StageGroupKey {
                    placement: stage.placement,
                    group: stage.group.clone(),
                };
                if let Some(group) = stage_groups.get_mut(&key) {
                    let partitions = stage.plan.output_partitioning().partition_count();
                    let b = t.partition * group.buckets.len() / partitions;
                    group.buckets[b].push(TaskSetEntry {
                        key: TaskKey {
                            job_id,
                            stage: t.stage,
                            partition: t.partition,
                            attempt,
                        },
                        output,
                    });
                }
            }
        }

        let mut tasks: Vec<(TaskPlacement, TaskSet)> = vec![];
        for (key, value) in stage_groups {
            for entries in value.buckets {
                tasks.push((key.placement, TaskSet { entries }));
            }
        }

        TaskRegion { tasks }
    }

    fn extend_job_output(job_id: JobId, job: &mut JobDescriptor) -> Vec<JobAction> {
        let JobState::Running { output, .. } = &mut job.state else {
            return vec![];
        };
        let mut actions = vec![];
        let schema = job.graph.schema().clone();
        for (s, stage) in job.graph.stages().iter().enumerate() {
            if !job.topology.stages[s].consumers.is_empty() {
                // The stage is not a final stage.
                continue;
            }
            let partitions = stage.plan.output_partitioning().partition_count();
            let channels = stage.distribution.channels();
            for p in 0..partitions {
                let Some((attempt, head)) = job.stages[s].tasks[p].attempts.split_last_mut() else {
                    continue;
                };
                if !matches!(attempt.state, TaskState::Running | TaskState::Succeeded)
                    || attempt.job_output_fetched
                {
                    continue;
                }
                attempt.job_output_fetched = true;
                for c in 0..channels {
                    let key = TaskStreamKey {
                        job_id,
                        stage: s,
                        partition: p,
                        attempt: head.len(),
                        channel: c,
                    };
                    actions.push(JobAction::ExtendJobOutput {
                        handle: output.handle(),
                        key,
                        schema: schema.clone(),
                    });
                }
            }
        }

        actions
    }

    fn infer_job_failure_cause(job: &JobDescriptor) -> CommonErrorCause {
        // a map from (stage, partition) to a list of causes
        let mut causes: HashMap<(usize, usize), Vec<&CommonErrorCause>> = HashMap::new();
        for (s, stage) in job.stages.iter().enumerate() {
            for (t, task) in stage.tasks.iter().enumerate() {
                for attempt in task.attempts.iter() {
                    if matches!(attempt.state, TaskState::Failed) {
                        if let Some(cause) = &attempt.cause {
                            causes.entry((s, t)).or_default().push(cause);
                        }
                    }
                }
            }
        }
        // Get the most recent cause from the most likely failed task.
        if let Some((_, &[.., cause])) = causes.iter().map(|(k, v)| (k, v.as_slice())).min_by(
            |((s1, p1), v1), ((s2, p2), v2)| {
                // Find the task that fails the most number of times,
                // and if there is a tie, choose the one with the smallest
                // stage and partition.
                v2.len().cmp(&v1.len()).then(s1.cmp(s2)).then(p1.cmp(p2))
            },
        ) {
            cause.clone()
        } else {
            CommonErrorCause::new::<PyErrExtractor>(&ExecutionError::InternalError(
                "job failed for unknown reason".to_string(),
            ))
        }
    }

    /// Determine the actions needed in the driver to clean up the job.
    /// The method cancels all the task attempts that are not in terminal states
    /// and removes all the job output streams.
    pub fn clean_up_job(&mut self, job_id: JobId) -> Vec<JobAction> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        let mut actions = vec![];
        for (s, stage) in job.stages.iter().enumerate() {
            for (t, task) in stage.tasks.iter().enumerate() {
                for (a, attempt) in task.attempts.iter().enumerate() {
                    if !attempt.state.is_terminal() {
                        actions.push(JobAction::CancelTask {
                            key: TaskKey {
                                job_id,
                                stage: s,
                                partition: t,
                                attempt: a,
                            },
                        });
                    }
                }
            }
        }
        for stage in job.stages.iter_mut() {
            stage.state = StageState::Inactive;
            if stage.stopped_at.is_none() {
                stage.stopped_at = Some(Utc::now());
            }
        }
        actions.push(JobAction::CleanUpJob {
            job_id,
            stage: None,
        });
        if matches!(job.state, JobState::Draining) {
            job.state = JobState::Succeeded;
        } else {
            job.state = JobState::Canceled;
        }
        job.stopped_at = Some(Utc::now());
        actions
    }

    /// Builds the serialized task definition and context for the given task key.
    pub fn get_task_definition(
        &self,
        key: &TaskKey,
        assignments: &dyn TaskAssignmentGetter,
    ) -> ExecutionResult<(TaskDefinition, Arc<TaskContext>)> {
        let Some(job) = self.jobs.get(&key.job_id) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "job {} not found",
                key.job_id
            )));
        };
        let JobState::Running { context, .. } = &job.state else {
            return Err(ExecutionError::InvalidArgument(format!(
                "job {} is not running",
                key.job_id
            )));
        };
        let Some(stage) = job.graph.stages().get(key.stage) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "stage {} not found in job {}",
                key.stage, key.job_id
            )));
        };

        let plan =
            PhysicalPlanNode::try_from_physical_plan(stage.plan.clone(), self.codec.as_ref())?
                .encode_to_vec();
        let inputs = stage
            .inputs
            .iter()
            .map(|input| self.get_task_input(job, key, input, assignments))
            .collect::<ExecutionResult<Vec<_>>>()?;
        let output = self.get_task_output(job, key, stage)?;
        let definition = TaskDefinition {
            plan: Arc::from(plan),
            inputs,
            output,
        };
        Ok((definition, context.clone()))
    }

    pub fn stop(&mut self) {
        for (_, job) in self.jobs.iter_mut() {
            if matches!(job.state, JobState::Running { .. } | JobState::Draining) {
                // For running jobs, the job output is dropped here.
                // Internally, the job output manages the receiving end of the output stream.
                // So once the stream receiver is no longer available, all the running tasks
                // will ultimately find that the sink (owned by the shuffle write node) is closed.
                // The task will stop running even if the task produces an infinite stream.
                // Once the tasks are stopped, the inputs (gRPC streaming responses owned by
                // shuffle read nodes) are dropped. So the worker gRPC server will have no active
                // clients subscribing to local streams, and the server can proceed with shutdown.
                job.state = JobState::Canceled;
                job.stopped_at = Some(Utc::now());
            }
            for stage in job.stages.iter_mut() {
                if matches!(stage.state, StageState::Active) {
                    stage.state = StageState::Inactive;
                    stage.stopped_at = Some(Utc::now());
                }
                for task in stage.tasks.iter_mut() {
                    for attempt in task.attempts.iter_mut() {
                        if !attempt.state.is_terminal() {
                            attempt.state = TaskState::Canceled;
                            attempt.stopped_at = Some(Utc::now());
                        }
                    }
                }
            }
        }
    }

    fn get_task_input(
        &self,
        job: &JobDescriptor,
        key: &TaskKey,
        input: &StageInput,
        assignments: &dyn TaskAssignmentGetter,
    ) -> ExecutionResult<TaskInput> {
        let latest_attempt = |stage: usize, partition: usize| -> ExecutionResult<usize> {
            Self::get_latest_task_attempt(job, stage, partition).ok_or_else(|| {
                ExecutionError::InvalidArgument(format!(
                    "no latest task attempt found for job {} stage {} partition {}",
                    key.job_id, stage, partition
                ))
            })
        };

        let Some(producer) = job.graph.stages().get(input.stage) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "job {} input stage {} not found",
                key.job_id, input.stage
            )));
        };
        let partitions = producer.plan.output_partitioning().partition_count();
        let channels = producer.distribution.channels();
        let keys = match input.mode {
            InputMode::Forward | InputMode::Merge => (0..partitions)
                .map(|partition| {
                    (0..channels)
                        .map(|channel| {
                            Ok(TaskInputKey {
                                partition,
                                attempt: latest_attempt(input.stage, partition)?,
                                channel,
                            })
                        })
                        .collect::<ExecutionResult<Vec<_>>>()
                })
                .collect::<ExecutionResult<Vec<Vec<_>>>>()?,
            InputMode::Shuffle => {
                if let Some(ranges) = &input.partition_ranges {
                    ranges
                        .iter()
                        .map(|range| {
                            let mut keys = Vec::new();
                            for channel in range.clone() {
                                for partition in 0..partitions {
                                    keys.push(TaskInputKey {
                                        partition,
                                        attempt: latest_attempt(input.stage, partition)?,
                                        channel,
                                    });
                                }
                            }
                            Ok(keys)
                        })
                        .collect::<ExecutionResult<Vec<Vec<_>>>>()?
                } else {
                    (0..channels)
                        .map(|channel| {
                            (0..partitions)
                                .map(|partition| {
                                    Ok(TaskInputKey {
                                        partition,
                                        attempt: latest_attempt(input.stage, partition)?,
                                        channel,
                                    })
                                })
                                .collect::<ExecutionResult<Vec<_>>>()
                        })
                        .collect::<ExecutionResult<Vec<Vec<_>>>>()?
                }
            }
            InputMode::Broadcast => {
                let keys = (0..partitions)
                    .flat_map(|partition| {
                        (0..channels).map(move |channel| {
                            Ok(TaskInputKey {
                                partition,
                                attempt: latest_attempt(input.stage, partition)?,
                                channel,
                            })
                        })
                    })
                    .collect::<ExecutionResult<Vec<_>>>()?;
                vec![keys]
            }
        };
        let locator = match producer.mode {
            OutputMode::Pipelined | OutputMode::Blocking => match producer.placement {
                TaskPlacement::Driver => {
                    keys.iter().flatten().try_for_each(|k| {
                        match assignments.get(
                            &TaskKey {
                                job_id: key.job_id,
                                stage: input.stage,
                                partition: k.partition,
                                attempt: k.attempt,
                            }
                        ) {
                            Some(TaskAssignment::Driver) => Ok(()),
                            _ => Err(ExecutionError::InternalError(format!(
                                "job {} input stage {} partition {} attempt {} is not assigned to driver",
                                key.job_id, input.stage, k.partition, k.attempt
                            ))),
                        }
                    })?;
                    TaskInputLocator::Driver {
                        stage: input.stage,
                        keys,
                    }
                }
                TaskPlacement::Worker => {
                    let keys = keys.into_iter().map(|keys| {
                        keys.into_iter().map(|k| {
                            let Some(TaskAssignment::Worker { worker_id, slot: _ }) = assignments.get(
                                &TaskKey {
                                    job_id: key.job_id,
                                    stage: input.stage,
                                    partition: k.partition,
                                    attempt: k.attempt,
                                }
                            ) else {
                                return Err(ExecutionError::InternalError(format!(
                                    "job {} input stage {} partition {} attempt {} is not assigned to worker",
                                    key.job_id, input.stage, k.partition, k.attempt
                                )));
                            };
                            Ok((*worker_id, k))
                        }).collect::<ExecutionResult<Vec<_>>>()
                    }).collect::<ExecutionResult<Vec<Vec<_>>>>()?;
                    TaskInputLocator::Worker {
                        stage: input.stage,
                        keys,
                    }
                }
            },
        };
        Ok(TaskInput { locator })
    }

    fn get_task_output(
        &self,
        job: &JobDescriptor,
        key: &TaskKey,
        stage: &Stage,
    ) -> ExecutionResult<TaskOutput> {
        let replicas = job.graph.replicas(key.stage);
        let distribution = match &stage.distribution {
            OutputDistribution::Hash { keys, channels } => {
                let keys = keys
                    .iter()
                    .map(|expr| {
                        let expr =
                            serialize_physical_expr(expr, self.codec.as_ref())?.encode_to_vec();
                        Ok(Arc::from(expr))
                    })
                    .collect::<ExecutionResult<Vec<Arc<[u8]>>>>()?;
                TaskOutputDistribution::Hash {
                    keys,
                    channels: *channels,
                }
            }
            OutputDistribution::RoundRobin { channels } => TaskOutputDistribution::RoundRobin {
                channels: *channels,
            },
        };
        let locator = match stage.mode {
            OutputMode::Pipelined => TaskOutputLocator::Local { replicas },
            OutputMode::Blocking => TaskOutputLocator::LocalDisk,
        };
        Ok(TaskOutput {
            distribution,
            locator,
        })
    }

    fn get_latest_task_attempt(
        job: &JobDescriptor,
        stage: usize,
        partition: usize,
    ) -> Option<usize> {
        job.stages
            .get(stage)
            .and_then(|stage| stage.tasks.get(partition))
            .and_then(|task| task.attempts.split_last().map(|(_, head)| head.len()))
    }
}

#[derive(PartialEq, Eq, Hash)]
struct StageGroupKey {
    placement: TaskPlacement,
    group: String,
}

#[derive(Default)]
struct StageGroup {
    stages: IndexSet<usize>,
    buckets: Vec<Vec<TaskSetEntry>>,
}

fn update_stage_plan_partitioning(
    plan: Arc<dyn ExecutionPlan>,
    new_partition_count: usize,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
    use datafusion::physical_plan::PlanProperties;

    let result = plan.transform_up(|node| {
        if let Some(placeholder) = node.as_any().downcast_ref::<StageInputExec<usize>>() {
            let old_props = placeholder.properties();
            let new_partitioning =
                datafusion::physical_plan::Partitioning::UnknownPartitioning(new_partition_count);
            let new_props = Arc::new(PlanProperties::new(
                old_props.equivalence_properties().clone(),
                new_partitioning,
                old_props.emission_type,
                old_props.boundedness,
            ));
            let new_node = StageInputExec::new(*placeholder.input(), new_props);
            return Ok(Transformed::yes(
                Arc::new(new_node) as Arc<dyn ExecutionPlan>
            ));
        }
        if let Some(repartition) =
            node.as_any()
                .downcast_ref::<datafusion::physical_plan::repartition::RepartitionExec>()
        {
            let new_partitioning = match repartition.partitioning() {
                datafusion::physical_plan::Partitioning::RoundRobinBatch(_) => {
                    datafusion::physical_plan::Partitioning::RoundRobinBatch(new_partition_count)
                }
                datafusion::physical_plan::Partitioning::Hash(exprs, _) => {
                    datafusion::physical_plan::Partitioning::Hash(
                        exprs.clone(),
                        new_partition_count,
                    )
                }
                datafusion::physical_plan::Partitioning::UnknownPartitioning(_) => {
                    datafusion::physical_plan::Partitioning::UnknownPartitioning(
                        new_partition_count,
                    )
                }
            };
            let new_repartition = datafusion::physical_plan::repartition::RepartitionExec::try_new(
                repartition.input().clone(),
                new_partitioning,
            )?;
            return Ok(Transformed::yes(
                Arc::new(new_repartition) as Arc<dyn ExecutionPlan>
            ));
        }
        Ok(Transformed::no(node))
    });
    Ok(result.data()?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::Partitioning;

    use super::*;

    #[test]
    fn test_adaptive_disk_shuffle_coalescing_workflow() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir =
            std::env::temp_dir().join(format!("sail_test_aqe_{}", rand::random::<u64>()));
        let job_id = JobId::from(42);
        let stage_dir = temp_dir.join(format!("{job_id}")).join("0");
        std::fs::create_dir_all(&stage_dir)?;

        // Map partition 0, attempt 0: channels 0..4 (each 100 bytes)
        for c in 0..4 {
            std::fs::write(
                stage_dir.join(format!("shuffle_0_0_{c}.index")),
                "100\n10\n",
            )?;
        }
        // Map partition 1, attempt 0: channels 0..4 (each 100 bytes)
        for c in 0..4 {
            std::fs::write(
                stage_dir.join(format!("shuffle_1_0_{c}.index")),
                "100\n10\n",
            )?;
        }

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let empty = Arc::new(EmptyExec::new(schema));
        let repartition = Arc::new(RepartitionExec::try_new(
            empty,
            Partitioning::RoundRobinBatch(4),
        )?);

        let graph = JobGraph::try_new_with_mode(repartition, OutputMode::Blocking)?;
        assert_eq!(graph.stages().len(), 2);
        assert_eq!(graph.stages()[0].mode.to_string(), "Blocking");
        assert_eq!(
            graph.stages()[1]
                .plan
                .output_partitioning()
                .partition_count(),
            4
        );

        let mut job = JobDescriptor::try_new(graph, JobState::Draining)?;
        // 1 partition for Stage 0 + 4 partitions for Stage 1 = 5 regions
        assert_eq!(job.topology.regions.len(), 5);

        // Mark upstream tasks (Stage 0) as succeeded
        let stage0_partitions = job.stages[0].tasks.len();
        for p in 0..stage0_partitions {
            job.stages[0].tasks[p].attempts.push(TaskAttemptDescriptor {
                state: TaskState::Succeeded,
                messages: vec![],
                cause: None,
                job_output_fetched: false,
                created_at: Utc::now(),
                stopped_at: None,
            });
        }

        let options = JobSchedulerOptions::default()
            .with_shuffle_mode(OutputMode::Blocking)
            .with_shuffle_dir(temp_dir.clone())
            .with_adaptive_enabled(true)
            .with_target_partition_size(250); // Each channel is 100 bytes. Target 250 -> [0..2, 2..4] (2 partitions)

        JobScheduler::update_task_regions(&mut job, &options);
        assert!(matches!(job.regions[0].state, TaskRegionState::Succeeded));

        let actions = JobScheduler::schedule_task_regions(&options, job_id, &mut job);
        assert_eq!(actions.len(), 2);
        assert_eq!(job.topology.regions.len(), 3); // 1 for Stage 0 + 2 for Stage 1

        assert_eq!(job.stages[1].tasks.len(), 2);
        assert_eq!(
            job.graph.stages()[1]
                .plan
                .output_partitioning()
                .partition_count(),
            2
        );
        assert_eq!(
            job.graph.stages()[1].inputs[0].partition_ranges,
            Some(vec![0..2, 2..4])
        );

        let _ = std::fs::remove_dir_all(temp_dir);
        Ok(())
    }

    #[test]
    fn test_adaptive_disabled_preserves_partitions() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir =
            std::env::temp_dir().join(format!("sail_test_aqe_off_{}", rand::random::<u64>()));
        let job_id = JobId::from(43);
        let stage_dir = temp_dir.join(format!("{job_id}")).join("0");
        std::fs::create_dir_all(&stage_dir)?;

        for c in 0..4 {
            std::fs::write(stage_dir.join(format!("shuffle_0_0_{c}.index")), "10\n1\n")?;
        }

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let empty = Arc::new(EmptyExec::new(schema));
        let repartition = Arc::new(RepartitionExec::try_new(
            empty,
            Partitioning::RoundRobinBatch(4),
        )?);

        let graph = JobGraph::try_new_with_mode(repartition, OutputMode::Blocking)?;
        let mut job = JobDescriptor::try_new(graph, JobState::Draining)?;
        assert_eq!(job.topology.regions.len(), 5);

        for p in 0..job.stages[0].tasks.len() {
            job.stages[0].tasks[p].attempts.push(TaskAttemptDescriptor {
                state: TaskState::Succeeded,
                messages: vec![],
                cause: None,
                job_output_fetched: false,
                created_at: Utc::now(),
                stopped_at: None,
            });
        }

        let options = JobSchedulerOptions::default()
            .with_shuffle_mode(OutputMode::Blocking)
            .with_shuffle_dir(temp_dir.clone())
            .with_adaptive_enabled(false); // Adaptive disabled!

        JobScheduler::update_task_regions(&mut job, &options);
        let actions = JobScheduler::schedule_task_regions(&options, job_id, &mut job);
        // Stage 1 remains 4 partitions, each scheduled in its own region!
        assert_eq!(actions.len(), 4);

        assert_eq!(job.stages[1].tasks.len(), 4);
        assert_eq!(
            job.graph.stages()[1]
                .plan
                .output_partitioning()
                .partition_count(),
            4
        );
        assert_eq!(job.graph.stages()[1].inputs[0].partition_ranges, None);

        let _ = std::fs::remove_dir_all(temp_dir);
        Ok(())
    }

    struct MockAssignment {
        assignment: TaskAssignment,
    }
    impl TaskAssignmentGetter for MockAssignment {
        fn get(&self, _key: &TaskKey) -> Option<&TaskAssignment> {
            Some(&self.assignment)
        }
    }

    #[test]
    fn test_get_task_input_with_coalesced_ranges() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let empty = Arc::new(EmptyExec::new(schema));
        let repartition = Arc::new(RepartitionExec::try_new(
            empty,
            Partitioning::RoundRobinBatch(4),
        )?);

        let graph = JobGraph::try_new_with_mode(repartition, OutputMode::Blocking)?;
        let mut job = JobDescriptor::try_new(graph, JobState::Draining)?;

        // Upstream stage 0 succeeded with attempt 0
        job.stages[0].tasks[0].attempts.push(TaskAttemptDescriptor {
            state: TaskState::Succeeded,
            messages: vec![],
            cause: None,
            job_output_fetched: false,
            created_at: Utc::now(),
            stopped_at: None,
        });

        // Set coalesced partition ranges: 0..2 and 2..4
        job.graph.stages_mut()[1].inputs[0].partition_ranges = Some(vec![0..2, 2..4]);

        let scheduler = JobScheduler::new(JobSchedulerOptions::default());
        let task_key = TaskKey {
            job_id: JobId::from(1),
            stage: 1,
            partition: 0,
            attempt: 0,
        };

        let mock_assignment = MockAssignment {
            assignment: TaskAssignment::Worker {
                worker_id: crate::id::WorkerId::from(1),
                slot: 0,
            },
        };
        let task_input = scheduler.get_task_input(
            &job,
            &task_key,
            &job.graph.stages()[1].inputs[0],
            &mock_assignment,
        )?;

        match task_input.locator {
            TaskInputLocator::Worker { stage, keys } => {
                assert_eq!(stage, 0);
                assert_eq!(keys.len(), 2); // 2 coalesced partitions
                                           // Partition 0 reads channels 0 and 1
                assert_eq!(keys[0].len(), 2);
                assert_eq!(keys[0][0].1.channel, 0);
                assert_eq!(keys[0][1].1.channel, 1);
                // Partition 1 reads channels 2 and 3
                assert_eq!(keys[1].len(), 2);
                assert_eq!(keys[1][0].1.channel, 2);
                assert_eq!(keys[1][1].1.channel, 3);
            }
            _ => return Err("expected Worker locator".into()),
        }

        Ok(())
    }

    #[test]
    fn test_disk_shuffle_stage_and_job_cleanup() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir =
            std::env::temp_dir().join(format!("sail_test_clean_{}", rand::random::<u64>()));
        let job_id = JobId::from(99);
        let stage0_dir = temp_dir.join(format!("{job_id}")).join("0");
        let stage1_dir = temp_dir.join(format!("{job_id}")).join("1");
        std::fs::create_dir_all(&stage0_dir)?;
        std::fs::create_dir_all(&stage1_dir)?;

        std::fs::write(stage0_dir.join("shuffle_0_0_0.data"), b"data")?;
        std::fs::write(stage1_dir.join("shuffle_0_0_0.data"), b"data")?;

        assert!(stage0_dir.exists());
        assert!(stage1_dir.exists());

        let options = crate::stream_manager::StreamManagerOptions::default()
            .with_shuffle_dir(temp_dir.clone());
        let mut sm = crate::stream_manager::StreamManager::new(options);

        // Remove only stage 0
        sm.remove_local_streams(job_id, Some(0));
        assert!(!stage0_dir.exists());
        assert!(stage1_dir.exists());

        // Remove entire job
        sm.remove_local_streams(job_id, None);
        assert!(!temp_dir.join(format!("{job_id}")).exists());

        let _ = std::fs::remove_dir_all(temp_dir);
        Ok(())
    }

    #[test]
    fn test_adaptive_multi_input_coalescing() -> Result<(), Box<dyn std::error::Error>> {
        use datafusion::common::{JoinType, NullEquality};
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        let temp_dir =
            std::env::temp_dir().join(format!("sail_test_aqe_multi_{}", rand::random::<u64>()));
        let job_id = JobId::from(77);

        // Stage 0 dir (left upstream)
        let stage0_dir = temp_dir.join(format!("{job_id}")).join("0");
        std::fs::create_dir_all(&stage0_dir)?;
        for c in 0..4 {
            std::fs::write(stage0_dir.join(format!("shuffle_0_0_{c}.index")), "50\n5\n")?;
        }

        // Stage 1 dir (right upstream)
        let stage1_dir = temp_dir.join(format!("{job_id}")).join("1");
        std::fs::create_dir_all(&stage1_dir)?;
        for c in 0..4 {
            std::fs::write(stage1_dir.join(format!("shuffle_0_0_{c}.index")), "50\n5\n")?;
        }

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let empty1 = Arc::new(EmptyExec::new(schema.clone()));
        let empty2 = Arc::new(EmptyExec::new(schema.clone()));
        let rep1 = Arc::new(RepartitionExec::try_new(
            empty1,
            Partitioning::RoundRobinBatch(4),
        )?);
        let rep2 = Arc::new(RepartitionExec::try_new(
            empty2,
            Partitioning::RoundRobinBatch(4),
        )?);

        let join = Arc::new(HashJoinExec::try_new(
            rep1,
            rep2,
            vec![(Arc::new(Column::new("x", 0)), Arc::new(Column::new("x", 0)))],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?);

        let graph = JobGraph::try_new_with_mode(join, OutputMode::Blocking)?;
        assert_eq!(graph.stages().len(), 3); // Stage 0 (left), Stage 1 (right), Stage 2 (join)
        assert_eq!(graph.stages()[2].inputs.len(), 2);

        let mut job = JobDescriptor::try_new(graph, JobState::Draining)?;

        // Succeed Stage 0 (left)
        for t in &mut job.stages[0].tasks {
            t.attempts.push(TaskAttemptDescriptor {
                state: TaskState::Succeeded,
                messages: vec![],
                cause: None,
                job_output_fetched: false,
                created_at: Utc::now(),
                stopped_at: None,
            });
        }
        // Succeed Stage 1 (right)
        for t in &mut job.stages[1].tasks {
            t.attempts.push(TaskAttemptDescriptor {
                state: TaskState::Succeeded,
                messages: vec![],
                cause: None,
                job_output_fetched: false,
                created_at: Utc::now(),
                stopped_at: None,
            });
        }

        let options = JobSchedulerOptions::default()
            .with_shuffle_mode(OutputMode::Blocking)
            .with_shuffle_dir(temp_dir.clone())
            .with_adaptive_enabled(true)
            .with_target_partition_size(250); // Combined channel sizes: 50+50=100. Target 250 -> [0..2, 2..4]

        JobScheduler::update_task_regions(&mut job, &options);

        // Schedule task regions
        let actions = JobScheduler::schedule_task_regions(&options, job_id, &mut job);
        assert_eq!(actions.len(), 2); // 2 coalesced partitions for Stage 2

        // Stage 2 tasks coalesced from 4 down to 2
        assert_eq!(job.stages[2].tasks.len(), 2);
        // Both left and right inputs must have identical coalesced partition ranges
        assert_eq!(
            job.graph.stages()[2].inputs[0].partition_ranges,
            Some(vec![0..2, 2..4])
        );
        assert_eq!(
            job.graph.stages()[2].inputs[1].partition_ranges,
            Some(vec![0..2, 2..4])
        );

        let _ = std::fs::remove_dir_all(temp_dir);
        Ok(())
    }

    #[test]
    fn test_task_failure_isolation_in_coalesced_stage() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir =
            std::env::temp_dir().join(format!("sail_test_isolation_{}", rand::random::<u64>()));
        let job_id = JobId::from(88);
        let stage_dir = temp_dir.join(format!("{job_id}")).join("0");
        std::fs::create_dir_all(&stage_dir)?;

        for c in 0..4 {
            std::fs::write(
                stage_dir.join(format!("shuffle_0_0_{c}.index")),
                "100\n10\n",
            )?;
        }

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let empty = Arc::new(EmptyExec::new(schema));
        let repartition = Arc::new(RepartitionExec::try_new(
            empty,
            Partitioning::RoundRobinBatch(4),
        )?);

        let graph = JobGraph::try_new_with_mode(repartition, OutputMode::Blocking)?;
        let mut job = JobDescriptor::try_new(graph, JobState::Draining)?;

        // Succeed upstream stage 0
        for t in &mut job.stages[0].tasks {
            t.attempts.push(TaskAttemptDescriptor {
                state: TaskState::Succeeded,
                messages: vec![],
                cause: None,
                job_output_fetched: false,
                created_at: Utc::now(),
                stopped_at: None,
            });
        }

        let options = JobSchedulerOptions::default()
            .with_shuffle_mode(OutputMode::Blocking)
            .with_shuffle_dir(temp_dir.clone())
            .with_adaptive_enabled(true)
            .with_target_partition_size(250);

        JobScheduler::update_task_regions(&mut job, &options);
        let actions = JobScheduler::schedule_task_regions(&options, job_id, &mut job);
        assert_eq!(actions.len(), 2); // 2 partitions scheduled
        assert!(matches!(
            job.stages[1].tasks[0].attempts[0].state,
            TaskState::Created
        ));
        assert!(matches!(
            job.stages[1].tasks[1].attempts[0].state,
            TaskState::Created
        ));

        // Start both tasks running
        job.stages[1].tasks[0].attempts[0].state = TaskState::Running;
        job.stages[1].tasks[1].attempts[0].state = TaskState::Running;

        // Fail partition 0
        job.stages[1].tasks[0].attempts[0].state = TaskState::Failed;

        // Cascade cancel task attempts
        let cancel_actions = JobScheduler::cascade_cancel_task_attempts(job_id, &mut job);

        // Region for partition 0 had only task (1, 0). Partition 1 must remain Running!
        assert!(matches!(
            job.stages[1].tasks[1].attempts[0].state,
            TaskState::Running
        ));
        assert!(!cancel_actions.iter().any(|action| matches!(
            action,
            JobAction::CancelTask { key } if key.stage == 1 && key.partition == 1
        )));

        let _ = std::fs::remove_dir_all(temp_dir);
        Ok(())
    }
}
