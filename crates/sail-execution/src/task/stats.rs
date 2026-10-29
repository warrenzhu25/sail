use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result, internal_datafusion_err};

const CHANNEL_BYTES_COLUMN: &str = "shuffle_channel_bytes";
const CHANNEL_RECORDS_COLUMN: &str = "shuffle_channel_records";

/// Per-channel statistics of the shuffle output produced by a single task attempt.
///
/// The statistics are collected by the worker while writing the shuffle output
/// and reported to the driver together with the task status, so that the driver
/// can make adaptive decisions (e.g. coalescing shuffle partitions) without
/// accessing the shuffle files directly.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskShuffleStats {
    /// The (in-memory) size in bytes of the data written to each output channel.
    pub channel_bytes: Vec<u64>,
    /// The number of records written to each output channel.
    pub channel_records: Vec<u64>,
}

impl TaskShuffleStats {
    pub fn new(channels: usize) -> Self {
        Self {
            channel_bytes: vec![0; channels],
            channel_records: vec![0; channels],
        }
    }

    /// Returns the schema of the record batch produced by the shuffle write plan
    /// that carries the shuffle statistics.
    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(CHANNEL_BYTES_COLUMN, DataType::UInt64, false),
            Field::new(CHANNEL_RECORDS_COLUMN, DataType::UInt64, false),
        ]))
    }

    /// Encodes the statistics as a record batch with one row per channel.
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(UInt64Array::from(self.channel_bytes.clone())),
                Arc::new(UInt64Array::from(self.channel_records.clone())),
            ],
        )?)
    }

    /// Decodes the statistics from a record batch produced by [`Self::to_record_batch`].
    /// Returns `None` if the batch does not carry shuffle statistics.
    pub fn try_from_record_batch(batch: &RecordBatch) -> Result<Option<Self>> {
        let schema = batch.schema();
        let (Ok(bytes), Ok(records)) = (
            schema.index_of(CHANNEL_BYTES_COLUMN),
            schema.index_of(CHANNEL_RECORDS_COLUMN),
        ) else {
            return Ok(None);
        };
        let column = |i: usize| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .filter(|a| a.null_count() == 0)
                .map(|a| a.values().to_vec())
                .ok_or_else(|| internal_datafusion_err!("invalid shuffle statistics column"))
        };
        Ok(Some(Self {
            channel_bytes: column(bytes)?,
            channel_records: column(records)?,
        }))
    }

    /// Merges statistics reported in multiple batches of the same task attempt.
    pub fn merge(&mut self, other: &Self) {
        if self.channel_bytes.len() < other.channel_bytes.len() {
            self.channel_bytes.resize(other.channel_bytes.len(), 0);
        }
        if self.channel_records.len() < other.channel_records.len() {
            self.channel_records.resize(other.channel_records.len(), 0);
        }
        for (acc, x) in self.channel_bytes.iter_mut().zip(&other.channel_bytes) {
            *acc += x;
        }
        for (acc, x) in self.channel_records.iter_mut().zip(&other.channel_records) {
            *acc += x;
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn test_task_shuffle_stats_round_trip() {
        let stats = TaskShuffleStats {
            channel_bytes: vec![10, 0, 30],
            channel_records: vec![1, 0, 3],
        };
        let batch = stats.to_record_batch().unwrap();
        assert_eq!(
            TaskShuffleStats::try_from_record_batch(&batch).unwrap(),
            Some(stats.clone())
        );
        let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
        assert_eq!(
            TaskShuffleStats::try_from_record_batch(&empty).unwrap(),
            None
        );

        let mut merged = TaskShuffleStats::default();
        merged.merge(&stats);
        merged.merge(&stats);
        assert_eq!(merged.channel_bytes, vec![20, 0, 60]);
        assert_eq!(merged.channel_records, vec![2, 0, 6]);
    }
}
