use std::ops::Range;
use std::path::Path;

use log::debug;

use crate::id::JobId;
use crate::stream_manager::shuffle_file::{MapOutputKey, ShuffleIndex};
use crate::task::stats::TaskShuffleStats;

/// Summary statistics for all partitions and channels produced by a shuffle stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageShuffleStats {
    pub channel_bytes: Vec<u64>,
    pub channel_records: Vec<usize>,
    pub total_bytes: u64,
    pub total_records: usize,
    /// The number of index files that could not be read.
    pub missing_index_files: usize,
}

impl StageShuffleStats {
    pub fn new(channels: usize) -> Self {
        Self {
            channel_bytes: vec![0; channels],
            channel_records: vec![0; channels],
            total_bytes: 0,
            total_records: 0,
            missing_index_files: 0,
        }
    }

    /// Aggregates the shuffle stats reported by the workers for each upstream task.
    pub fn from_task_stats<'a>(
        channels: usize,
        tasks: impl IntoIterator<Item = &'a TaskShuffleStats>,
    ) -> Self {
        let mut stats = Self::new(channels);
        for task in tasks {
            for (acc, b) in stats.channel_bytes.iter_mut().zip(&task.channel_bytes) {
                *acc += b;
                stats.total_bytes += b;
            }
            for (acc, r) in stats.channel_records.iter_mut().zip(&task.channel_records) {
                *acc += *r as usize;
                stats.total_records += *r as usize;
            }
        }
        stats
    }

    /// Read shuffle stats from the on-disk index files of the map outputs.
    pub fn from_disk(
        shuffle_dir: &Path,
        job_id: JobId,
        stage: usize,
        partitions: usize,
        channels: usize,
        attempts: &[usize],
    ) -> Self {
        let mut stats = Self::new(channels);
        for p in 0..partitions {
            let key = MapOutputKey {
                job_id,
                stage,
                partition: p,
                attempt: attempts.get(p).copied().unwrap_or(0),
            };
            let index_file = key.index_path(shuffle_dir);
            let index = match ShuffleIndex::read(&index_file) {
                Ok(index) if index.channels() == channels => index,
                Ok(index) => {
                    debug!(
                        "shuffle index file {index_file:?} has {} channels but {channels} are expected",
                        index.channels()
                    );
                    stats.missing_index_files += 1;
                    continue;
                }
                Err(e) => {
                    debug!("could not read shuffle index file {index_file:?}: {e}");
                    stats.missing_index_files += 1;
                    continue;
                }
            };
            for c in 0..channels {
                let bytes = index.channel_bytes(c).unwrap_or(0);
                let records = index.channel_records(c).unwrap_or(0) as usize;
                stats.channel_bytes[c] += bytes;
                stats.channel_records[c] += records;
                stats.total_bytes += bytes;
                stats.total_records += records;
            }
        }
        stats
    }

    /// Dynamically coalesce adjacent shuffle channels into partition ranges
    /// such that each range's combined size is roughly `target_partition_size` bytes.
    #[cfg(test)]
    pub fn coalesce(&self, target_partition_size: u64) -> Vec<Range<usize>> {
        coalesce_shuffle_partitions(&self.channel_bytes, target_partition_size)
    }

    /// Detect partitions that suffer from data skew.
    ///
    /// A partition is skewed if its size exceeds `median * skew_factor` and exceeds
    /// `min_skew_threshold` in bytes.
    pub fn detect_skew_partitions(&self, skew_factor: f64, min_skew_threshold: u64) -> Vec<usize> {
        if self.channel_bytes.is_empty() {
            return vec![];
        }

        let mut non_zero_sizes: Vec<u64> = self
            .channel_bytes
            .iter()
            .copied()
            .filter(|&s| s > 0)
            .collect();

        if non_zero_sizes.is_empty() {
            return vec![];
        }

        non_zero_sizes.sort_unstable();
        let median = non_zero_sizes[non_zero_sizes.len() / 2];

        let mut skewed = Vec::new();
        for (c, &size) in self.channel_bytes.iter().enumerate() {
            if size >= min_skew_threshold {
                let threshold = (median as f64) * skew_factor;
                if size as f64 > threshold {
                    skewed.push(c);
                }
            }
        }
        skewed
    }

    /// Determines if downstream join should be optimized to broadcast join.
    #[cfg(test)]
    pub fn should_broadcast_join(&self, auto_broadcast_threshold: u64) -> bool {
        self.total_bytes > 0 && self.total_bytes <= auto_broadcast_threshold
    }
}

/// Dynamic partition coalescing algorithm for shuffle channels.
///
/// Combines contiguous channels $[c_{start}, c_{end})$ into partition groups such that
/// each group has an aggregated byte size approximating `target_partition_size`.
pub fn coalesce_shuffle_partitions(
    channel_bytes: &[u64],
    target_partition_size: u64,
) -> Vec<Range<usize>> {
    if channel_bytes.is_empty() {
        return vec![];
    }
    if target_partition_size == 0 {
        return (0..channel_bytes.len()).map(|i| i..i + 1).collect();
    }

    let mut ranges = Vec::new();
    let mut start = 0;
    let mut current_size = 0u64;

    for (i, &size) in channel_bytes.iter().enumerate() {
        if current_size > 0 && current_size + size > target_partition_size {
            debug!("coalesced partition range {start}..{i} with size {current_size} bytes");
            ranges.push(start..i);
            start = i;
            current_size = size;
        } else {
            current_size += size;
        }
    }

    if start < channel_bytes.len() {
        debug!(
            "coalesced partition range {start}..{} with size {current_size} bytes",
            channel_bytes.len()
        );
        ranges.push(start..channel_bytes.len());
    }

    ranges
}

/// Returns the target partition size for coalescing shuffle partitions,
/// taking the desired minimum parallelism into account (similar to Spark AQE
/// with `spark.sql.adaptive.coalescePartitions.parallelismFirst`).
///
/// The target size is reduced so that roughly `min_parallelism` partitions are
/// produced, but it never goes below `min_partition_size` (to avoid tiny tasks)
/// or above `target_partition_size`.
pub fn effective_target_partition_size(
    total_bytes: u64,
    target_partition_size: u64,
    min_partition_size: u64,
    min_parallelism: usize,
) -> u64 {
    if min_parallelism <= 1 || target_partition_size == 0 {
        return target_partition_size;
    }
    let per_slot = total_bytes / min_parallelism as u64;
    target_partition_size.min(per_slot.max(min_partition_size))
}

/// Splits map partitions into sub-ranges for reading a skewed channel across multiple tasks.
#[cfg(test)]
#[expect(clippy::single_range_in_vec_init)]
pub fn split_skew_partition(num_map_partitions: usize, num_splits: usize) -> Vec<Range<usize>> {
    if num_map_partitions == 0 || num_splits <= 1 {
        return vec![0..num_map_partitions];
    }

    let splits = num_splits.min(num_map_partitions);
    let chunk_size = num_map_partitions.div_ceil(splits);
    let mut ranges = Vec::with_capacity(splits);

    let mut start = 0;
    while start < num_map_partitions {
        let end = (start + chunk_size).min(num_map_partitions);
        ranges.push(start..end);
        start = end;
    }

    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_manager::shuffle_file::write_test_shuffle_index;

    #[test]
    fn test_effective_target_partition_size() {
        const MIB: u64 = 1024 * 1024;
        // No parallelism floor.
        assert_eq!(
            effective_target_partition_size(1000 * MIB, 64 * MIB, MIB, 1),
            64 * MIB
        );
        // Large data: the configured target size wins.
        assert_eq!(
            effective_target_partition_size(10_000 * MIB, 64 * MIB, MIB, 64),
            64 * MIB
        );
        // Medium data: reduce the target size to keep 64 partitions.
        assert_eq!(
            effective_target_partition_size(640 * MIB, 64 * MIB, MIB, 64),
            10 * MIB
        );
        // Small data: never go below the minimum partition size.
        assert_eq!(
            effective_target_partition_size(8 * MIB, 64 * MIB, MIB, 64),
            MIB
        );
    }

    #[test]
    fn test_coalesce_empty() {
        assert_eq!(
            coalesce_shuffle_partitions(&[], 100),
            Vec::<Range<usize>>::new()
        );
    }

    #[test]
    fn test_coalesce_target_zero() {
        let sizes = vec![10, 20, 30];
        assert_eq!(
            coalesce_shuffle_partitions(&sizes, 0),
            vec![0..1, 1..2, 2..3]
        );
    }

    #[test]
    fn test_coalesce_all_small_partitions() {
        // 5 small channels coalesced into a single partition
        let sizes = vec![10, 10, 10, 10, 10];
        assert_eq!(coalesce_shuffle_partitions(&sizes, 100), vec![0..5]);
    }

    #[test]
    fn test_coalesce_balanced_partitions() {
        let sizes = vec![20, 25, 30, 15, 45, 10];
        // Target: 50
        // [20, 25] -> 45 <= 50 (range 0..2)
        // [30, 15] -> 45 <= 50 (range 2..4)
        // [45] -> 45 <= 50 (range 4..5)
        // [10] -> 10 <= 50 (range 5..6)
        assert_eq!(
            coalesce_shuffle_partitions(&sizes, 50),
            vec![0..2, 2..4, 4..5, 5..6]
        );
    }

    #[test]
    fn test_coalesce_skewed_partition() {
        let sizes = vec![5, 5, 200, 10, 10];
        // Target: 50
        // 0..2 (10 <= 50)
        // 2..3 (200 > 50)
        // 3..5 (20 <= 50)
        assert_eq!(
            coalesce_shuffle_partitions(&sizes, 50),
            vec![0..2, 2..3, 3..5]
        );
    }

    #[test]
    fn test_from_disk_stats() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir =
            std::env::temp_dir().join(format!("sail_test_adaptive_{}", rand::random::<u64>()));
        let job_dir = temp_dir.join("1").join("0");
        std::fs::create_dir_all(&job_dir)?;

        // Map partition 0, attempt 0
        write_test_shuffle_index(&job_dir, 0, 0, &[(100, 10), (200, 20)])?;
        // Map partition 1, attempt 1
        write_test_shuffle_index(&job_dir, 1, 1, &[(300, 30), (400, 40)])?;
        // A stale attempt that must be ignored
        write_test_shuffle_index(&job_dir, 1, 0, &[(1, 1), (1, 1)])?;

        let stats = StageShuffleStats::from_disk(&temp_dir, JobId::from(1), 0, 2, 2, &[0, 1]);

        assert_eq!(stats.channel_bytes, vec![400, 600]);
        assert_eq!(stats.channel_records, vec![40, 60]);
        assert_eq!(stats.total_bytes, 1000);
        assert_eq!(stats.total_records, 100);

        assert_eq!(stats.missing_index_files, 0);

        let coalesced = stats.coalesce(1500);
        assert_eq!(coalesced, vec![0..2]);

        // Missing map outputs and channel count mismatches are reported.
        let stats = StageShuffleStats::from_disk(&temp_dir, JobId::from(1), 0, 3, 2, &[0, 1, 0]);
        assert_eq!(stats.missing_index_files, 1);
        let stats = StageShuffleStats::from_disk(&temp_dir, JobId::from(1), 0, 2, 3, &[0, 1]);
        assert_eq!(stats.missing_index_files, 2);

        let _ = std::fs::remove_dir_all(temp_dir);
        Ok(())
    }

    #[test]
    fn test_detect_skew_partitions() {
        let mut stats = StageShuffleStats::new(5);
        stats.channel_bytes = vec![10, 12, 500, 11, 10];
        stats.total_bytes = 543;

        // median is 11. Skew factor 5.0 -> threshold 55. Min skew threshold 100.
        // Channel 2 (500) > 55 and >= 100, so it is detected as skewed.
        let skewed = stats.detect_skew_partitions(5.0, 100);
        assert_eq!(skewed, vec![2]);

        // If min_skew_threshold is higher than 500, not skewed
        let skewed = stats.detect_skew_partitions(5.0, 1000);
        assert!(skewed.is_empty());
    }

    #[test]
    fn test_split_skew_partition() {
        let splits = split_skew_partition(10, 3);
        assert_eq!(splits, vec![0..4, 4..8, 8..10]);

        let single = split_skew_partition(5, 1);
        assert_eq!(single, vec![0..5]);
    }

    #[test]
    fn test_should_broadcast_join() {
        let mut stats = StageShuffleStats::new(2);
        stats.total_bytes = 5 * 1024 * 1024; // 5MB
        assert!(stats.should_broadcast_join(10 * 1024 * 1024));
        assert!(!stats.should_broadcast_join(2 * 1024 * 1024));
    }
}
