use std::ops::Range;
use std::path::Path;

use log::debug;

use crate::id::JobId;

/// Summary statistics for all partitions and channels produced by a shuffle stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageShuffleStats {
    pub channel_bytes: Vec<u64>,
    pub channel_records: Vec<usize>,
    pub total_bytes: u64,
    pub total_records: usize,
}

impl StageShuffleStats {
    pub fn new(channels: usize) -> Self {
        Self {
            channel_bytes: vec![0; channels],
            channel_records: vec![0; channels],
            total_bytes: 0,
            total_records: 0,
        }
    }

    /// Read shuffle stats from on-disk index files written by `DiskStream`.
    pub fn from_disk(
        shuffle_dir: &Path,
        job_id: JobId,
        stage: usize,
        partitions: usize,
        channels: usize,
        attempts: &[usize],
    ) -> Self {
        let mut stats = Self::new(channels);
        let stage_dir = shuffle_dir.join(format!("{job_id}")).join(format!("{stage}"));

        for (p, &attempt) in attempts.iter().enumerate().take(partitions) {
            for (c, (channel_byte_acc, channel_record_acc)) in stats
                .channel_bytes
                .iter_mut()
                .zip(stats.channel_records.iter_mut())
                .enumerate()
            {
                let index_file = stage_dir.join(format!("shuffle_{p}_{attempt}_{c}.index"));
                if let Ok(content) = std::fs::read_to_string(&index_file) {
                    let mut lines = content.lines();
                    if let (Some(bytes_str), Some(records_str)) = (lines.next(), lines.next()) {
                        if let (Ok(bytes), Ok(records)) =
                            (bytes_str.parse::<u64>(), records_str.parse::<usize>())
                        {
                            *channel_byte_acc += bytes;
                            *channel_record_acc += records;
                            stats.total_bytes += bytes;
                            stats.total_records += records;
                        }
                    }
                }
            }
        }
        stats
    }

    /// Dynamically coalesce adjacent shuffle channels into partition ranges
    /// such that each range's combined size is roughly `target_partition_size` bytes.
    pub fn coalesce(&self, target_partition_size: u64) -> Vec<Range<usize>> {
        coalesce_shuffle_partitions(&self.channel_bytes, target_partition_size)
    }

    /// Detect partitions that suffer from data skew.
    ///
    /// A partition is skewed if its size exceeds `median * skew_factor` and exceeds
    /// `min_skew_threshold` in bytes.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
            debug!(
                "coalesced partition range {start}..{i} with size {current_size} bytes"
            );
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

/// Splits map partitions into sub-ranges for reading a skewed channel across multiple tasks.
#[allow(dead_code)]
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

    #[test]
    fn test_coalesce_empty() {
        assert_eq!(coalesce_shuffle_partitions(&[], 100), Vec::<Range<usize>>::new());
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
        assert_eq!(
            coalesce_shuffle_partitions(&sizes, 100),
            vec![0..5]
        );
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
        let temp_dir = std::env::temp_dir().join(format!("sail_test_adaptive_{}", rand::random::<u64>()));
        let job_dir = temp_dir.join("1").join("0");
        std::fs::create_dir_all(&job_dir)?;

        // Map partition 0, attempt 0
        std::fs::write(job_dir.join("shuffle_0_0_0.index"), "100\n10\n")?;
        std::fs::write(job_dir.join("shuffle_0_0_1.index"), "200\n20\n")?;
        // Map partition 1, attempt 0
        std::fs::write(job_dir.join("shuffle_1_0_0.index"), "300\n30\n")?;
        std::fs::write(job_dir.join("shuffle_1_0_1.index"), "400\n40\n")?;

        let stats = StageShuffleStats::from_disk(
            &temp_dir,
            JobId::from(1),
            0,
            2,
            2,
            &[0, 0],
        );

        assert_eq!(stats.channel_bytes, vec![400, 600]);
        assert_eq!(stats.channel_records, vec![40, 60]);
        assert_eq!(stats.total_bytes, 1000);
        assert_eq!(stats.total_records, 100);

        let coalesced = stats.coalesce(1500);
        assert_eq!(coalesced, vec![0..2]);

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
