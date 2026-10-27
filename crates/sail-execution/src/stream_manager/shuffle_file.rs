//! The on-disk layout of blocking (disk) shuffle output.
//!
//! Each map task attempt writes exactly one data file and one index file,
//! regardless of the number of output channels (shuffle partitions):
//!
//! * `shuffle_{partition}_{attempt}.data` contains the data of all channels,
//!   ordered by channel. The data of channel `c` occupies the byte range
//!   `[offsets[c], offsets[c + 1])` and consists of zero or more concatenated
//!   Arrow IPC streams. An empty channel has a zero-length range.
//! * `shuffle_{partition}_{attempt}.index` contains the channel offsets and the
//!   number of records of each channel (see [`ShuffleIndex`]).
//!
//! The index file is written (atomically) after the data file, so the presence
//! of the index file indicates that the map output is complete.
//!
//! While the map task is running, the channel sinks share a [`MapOutputWriter`]
//! that buffers record batches in memory. When the buffered data exceeds a
//! threshold, all buffered batches are spilled to a single spill file, ordered
//! by channel. When all channels are closed, the final data file is written by
//! copying the spilled byte ranges of each channel followed by the remaining
//! in-memory batches of that channel.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Take, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use tokio::sync::watch;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::JobId;
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;

/// The buffer size for reading and writing shuffle files.
/// Hash-partitioned shuffle output often consists of many small batches,
/// so buffering avoids issuing many small read/write system calls.
const SHUFFLE_FILE_BUFFER_SIZE: usize = 256 * 1024;

/// The default size of in-memory buffered shuffle output (in bytes) of a map task
/// before the buffered data is spilled to disk.
pub(crate) const DEFAULT_SHUFFLE_SPILL_THRESHOLD: usize = 64 * 1024 * 1024;

/// The magic bytes of an index file without channel checksums.
const SHUFFLE_INDEX_MAGIC: &[u8; 8] = b"SAILSHI1";
/// The magic bytes of an index file with channel checksums.
const SHUFFLE_INDEX_MAGIC_WITH_CHECKSUMS: &[u8; 8] = b"SAILSHI2";

/// The options for writing the disk shuffle output of a map task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShuffleWriteOptions {
    /// The maximum size (in bytes) of buffered output before it is spilled to disk.
    pub spill_threshold: usize,
    /// Whether to sync the data and index files (and their directory) to stable storage.
    pub fsync: bool,
}

impl Default for ShuffleWriteOptions {
    fn default() -> Self {
        Self {
            spill_threshold: DEFAULT_SHUFFLE_SPILL_THRESHOLD,
            fsync: false,
        }
    }
}

impl ShuffleWriteOptions {
    #[cfg(test)]
    pub fn with_spill_threshold(mut self, spill_threshold: usize) -> Self {
        self.spill_threshold = spill_threshold;
        self
    }

    #[cfg(test)]
    pub fn with_fsync(mut self, fsync: bool) -> Self {
        self.fsync = fsync;
        self
    }
}

/// The key that identifies the output of a map task attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MapOutputKey {
    pub job_id: JobId,
    pub stage: usize,
    pub partition: usize,
    pub attempt: usize,
}

impl MapOutputKey {
    pub fn data_path(&self, shuffle_dir: &Path) -> PathBuf {
        shuffle_dir
            .join(format!("{}", self.job_id))
            .join(format!("{}", self.stage))
            .join(format!("shuffle_{}_{}.data", self.partition, self.attempt))
    }

    pub fn index_path(&self, shuffle_dir: &Path) -> PathBuf {
        index_path(&self.data_path(shuffle_dir))
    }
}

fn index_path(data_path: &Path) -> PathBuf {
    data_path.with_extension("index")
}

/// The index of a map output data file.
///
/// The binary format (all integers are little-endian `u64`) is:
/// the magic bytes `SAILSHI1`, the number of channels `C`,
/// `C + 1` byte offsets into the data file, and `C` record counts.
/// With the magic bytes `SAILSHI2`, the record counts are followed by `C` CRC-32
/// checksums of the channel data (stored in the low 32 bits of each value).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShuffleIndex {
    offsets: Vec<u64>,
    records: Vec<u64>,
    checksums: Option<Vec<u32>>,
}

impl ShuffleIndex {
    pub fn try_new(offsets: Vec<u64>, records: Vec<u64>) -> ExecutionResult<Self> {
        if offsets.len() != records.len() + 1 {
            return Err(ExecutionError::InternalError(format!(
                "invalid shuffle index: {} offsets for {} channels",
                offsets.len(),
                records.len()
            )));
        }
        if offsets.first() != Some(&0) || offsets.windows(2).any(|w| w[0] > w[1]) {
            return Err(ExecutionError::InternalError(
                "invalid shuffle index: offsets must start at zero and be non-decreasing"
                    .to_string(),
            ));
        }
        Ok(Self {
            offsets,
            records,
            checksums: None,
        })
    }

    /// Sets the CRC-32 checksums of the channel data.
    pub fn with_checksums(mut self, checksums: Vec<u32>) -> ExecutionResult<Self> {
        if checksums.len() != self.records.len() {
            return Err(ExecutionError::InternalError(format!(
                "invalid shuffle index: {} checksums for {} channels",
                checksums.len(),
                self.records.len()
            )));
        }
        self.checksums = Some(checksums);
        Ok(self)
    }

    /// Returns the CRC-32 checksum of the channel data, if the index has checksums.
    pub fn channel_checksum(&self, channel: usize) -> Option<u32> {
        self.checksums.as_ref()?.get(channel).copied()
    }

    /// Creates an index for consecutive channels with the given sizes.
    #[cfg(test)]
    pub fn from_channel_stats(stats: &[(u64, u64)]) -> Self {
        let mut offsets = vec![0];
        let mut records = vec![];
        for (bytes, rows) in stats {
            offsets.push(offsets[offsets.len() - 1] + bytes);
            records.push(*rows);
        }
        Self {
            offsets,
            records,
            checksums: None,
        }
    }

    pub fn channels(&self) -> usize {
        self.records.len()
    }

    pub fn channel_range(&self, channel: usize) -> Option<(u64, u64)> {
        Some((*self.offsets.get(channel)?, *self.offsets.get(channel + 1)?))
    }

    pub fn channel_bytes(&self, channel: usize) -> Option<u64> {
        self.channel_range(channel).map(|(start, end)| end - start)
    }

    pub fn channel_records(&self, channel: usize) -> Option<u64> {
        self.records.get(channel).copied()
    }

    pub fn data_size(&self) -> u64 {
        self.offsets.last().copied().unwrap_or(0)
    }

    pub fn encode(&self) -> Vec<u8> {
        let checksums = self.checksums.iter().flatten().map(|x| u64::from(*x));
        let mut out = Vec::with_capacity(
            16 + 8 * (self.offsets.len() + self.records.len() + self.records.len()),
        );
        out.extend_from_slice(if self.checksums.is_some() {
            SHUFFLE_INDEX_MAGIC_WITH_CHECKSUMS
        } else {
            SHUFFLE_INDEX_MAGIC
        });
        out.extend_from_slice(&(self.channels() as u64).to_le_bytes());
        for x in self
            .offsets
            .iter()
            .chain(self.records.iter())
            .copied()
            .chain(checksums)
        {
            out.extend_from_slice(&x.to_le_bytes());
        }
        out
    }

    pub fn decode(data: &[u8]) -> ExecutionResult<Self> {
        let invalid =
            |m: &str| ExecutionError::InternalError(format!("invalid shuffle index: {m}"));
        let has_checksums = if data.starts_with(SHUFFLE_INDEX_MAGIC_WITH_CHECKSUMS) {
            true
        } else if data.starts_with(SHUFFLE_INDEX_MAGIC) {
            false
        } else {
            return Err(invalid("bad magic"));
        };
        let mut values = data
            .get(8..)
            .ok_or_else(|| invalid("bad magic"))?
            .chunks(8)
            .map(|x| x.try_into().map(u64::from_le_bytes));
        let channels = match values.next() {
            Some(Ok(x)) => usize::try_from(x).map_err(|_| invalid("bad channel count"))?,
            _ => return Err(invalid("missing channel count")),
        };
        let expected = channels
            .checked_mul(if has_checksums { 3 } else { 2 })
            .and_then(|x| x.checked_add(1))
            .ok_or_else(|| invalid("bad channel count"))?;
        if data.len() != 16 + 8 * expected {
            return Err(invalid("unexpected size"));
        }
        let values = values
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid("truncated"))?;
        let (offsets, rest) = values.split_at(channels + 1);
        let (records, checksums) = rest.split_at(channels);
        let index = Self::try_new(offsets.to_vec(), records.to_vec())?;
        if has_checksums {
            let checksums = checksums
                .iter()
                .map(|x| u32::try_from(*x).map_err(|_| invalid("bad checksum")))
                .collect::<ExecutionResult<Vec<_>>>()?;
            index.with_checksums(checksums)
        } else {
            Ok(index)
        }
    }

    /// Reads the index file. A missing index file means that the map output
    /// does not exist (e.g. it has been lost or cleaned up), which is an error.
    pub fn read(path: &Path) -> ExecutionResult<Self> {
        match std::fs::read(path) {
            Ok(data) => Self::decode(&data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(ExecutionError::InternalError(format!(
                    "shuffle output not found: {}",
                    path.display()
                )))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Writes the index file atomically.
    /// If `fsync` is `true`, the file is synced to stable storage before it is renamed.
    /// The caller is responsible for syncing the directory after the rename.
    pub fn write(&self, path: &Path, fsync: bool) -> std::io::Result<()> {
        let tmp = path.with_extension("index.tmp");
        let mut file = File::create(&tmp)?;
        file.write_all(&self.encode())?;
        if fsync {
            file.sync_all()?;
        }
        drop(file);
        std::fs::rename(&tmp, path)
    }
}

/// Writes an index file for a map task in tests.
#[cfg(test)]
pub(crate) fn write_test_shuffle_index(
    stage_dir: &Path,
    partition: usize,
    attempt: usize,
    stats: &[(u64, u64)],
) -> std::io::Result<()> {
    ShuffleIndex::from_channel_stats(stats).write(
        &stage_dir.join(format!("shuffle_{partition}_{attempt}.index")),
        false,
    )
}

/// A writer wrapper that tracks the number of bytes written.
struct CountingWriter<W> {
    inner: W,
    position: u64,
    /// The checksum of the bytes written since the last call to [`Self::take_checksum`].
    hasher: crc32fast::Hasher,
}

impl<W> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            position: 0,
            hasher: crc32fast::Hasher::new(),
        }
    }

    /// Returns the checksum of the bytes written since the last call, and resets it.
    fn take_checksum(&mut self) -> u32 {
        std::mem::take(&mut self.hasher).finalize()
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.position += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Encodes the batches as one Arrow IPC stream.
fn encode_batches<W: Write>(writer: &mut W, batches: &[RecordBatch]) -> Result<(), ArrowError> {
    let Some(first) = batches.first() else {
        return Ok(());
    };
    let mut writer = StreamWriter::try_new(writer, &first.schema())?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()
}

fn copy_range(
    file: &mut File,
    offset: u64,
    length: u64,
    out: &mut impl Write,
    buffer: &mut [u8],
) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    let mut remaining = length;
    while remaining > 0 {
        let n = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..n])?;
        out.write_all(&buffer[..n])?;
        remaining -= n as u64;
    }
    Ok(())
}

/// The byte ranges `(offset, length)` of each channel in a file.
type ChannelRanges = Vec<Vec<(u64, u64)>>;

struct SpillFile {
    writer: CountingWriter<BufWriter<File>>,
    /// The byte ranges of each channel in the spill file.
    ranges: ChannelRanges,
}

impl SpillFile {
    fn create(path: &Path, channels: usize) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            writer: CountingWriter::new(BufWriter::with_capacity(SHUFFLE_FILE_BUFFER_SIZE, file)),
            ranges: vec![vec![]; channels],
        })
    }

    fn spill(&mut self, buffers: &[Vec<RecordBatch>]) -> Result<()> {
        for (c, batches) in buffers.iter().enumerate() {
            if batches.is_empty() {
                continue;
            }
            let start = self.writer.position;
            encode_batches(&mut self.writer, batches)?;
            self.ranges[c].push((start, self.writer.position - start));
        }
        self.writer.flush()?;
        Ok(())
    }

    fn into_parts(self) -> Result<(File, ChannelRanges)> {
        let file = self
            .writer
            .inner
            .into_inner()
            .map_err(|e| DataFusionError::IoError(e.into_error()))?;
        Ok((file, self.ranges))
    }
}

/// The status of a map output, observed by readers that subscribe to a channel
/// before the map output is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MapOutputStatus {
    Writing,
    Finalized,
    Failed(String),
}

struct MapOutputState {
    data_path: PathBuf,
    spill_path: PathBuf,
    spill_threshold: usize,
    fsync: bool,
    buffers: Vec<Vec<RecordBatch>>,
    buffered_bytes: usize,
    records: Vec<u64>,
    spill: Option<SpillFile>,
    closed: Vec<bool>,
    num_closed: usize,
    finalized: bool,
    /// The memory reservation for the buffered batches, if the writer is registered
    /// with a memory pool.
    reservation: Option<MemoryReservation>,
}

impl MapOutputState {
    fn take_buffers(&mut self) -> Vec<Vec<RecordBatch>> {
        self.buffered_bytes = 0;
        std::mem::replace(&mut self.buffers, vec![vec![]; self.closed.len()])
    }

    async fn spill(&mut self) -> Result<()> {
        let spill = self.spill.take();
        let spill_path = self.spill_path.clone();
        let channels = self.closed.len();
        let buffers = self.take_buffers();
        let spill = tokio::task::spawn_blocking(move || {
            let mut spill = match spill {
                Some(x) => x,
                None => SpillFile::create(&spill_path, channels)?,
            };
            spill.spill(&buffers)?;
            Ok::<_, DataFusionError>(spill)
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;
        self.spill = Some(spill);
        self.free_memory();
        Ok(())
    }

    fn free_memory(&mut self) {
        if let Some(reservation) = self.reservation.as_mut() {
            reservation.free();
        }
    }

    async fn finalize(&mut self) -> Result<()> {
        let buffers = self.take_buffers();
        let spill = self.spill.take();
        let records = std::mem::take(&mut self.records);
        let data_path = self.data_path.clone();
        let spill_path = self.spill_path.clone();
        let fsync = self.fsync;
        tokio::task::spawn_blocking(move || {
            write_map_output(&data_path, &spill_path, buffers, spill, records, fsync)
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;
        self.finalized = true;
        self.free_memory();
        Ok(())
    }
}

impl Drop for MapOutputState {
    fn drop(&mut self) {
        if !self.finalized {
            // The map task did not complete, so we clean up the partial output.
            // The spill file may be large, so it is removed in the background.
            let paths = [
                self.spill_path.clone(),
                self.data_path.with_extension("data.tmp"),
                index_path(&self.data_path).with_extension("index.tmp"),
            ];
            let spill = self.spill.take();
            run_blocking_in_background(move || {
                drop(spill);
                for path in paths {
                    let _ = std::fs::remove_file(path);
                }
            });
        }
    }
}

/// Runs blocking work (e.g. file system operations) in the background,
/// off the asynchronous worker threads if there is a Tokio runtime.
/// The work is run inline if there is no Tokio runtime.
pub(crate) fn run_blocking_in_background(f: impl FnOnce() + Send + 'static) {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            // The task is detached; the join handle is not needed.
            drop(handle.spawn_blocking(f));
        }
        Err(_) => f(),
    }
}

/// Runs blocking work that the caller must wait for. If the caller runs on a worker
/// thread of a multi-threaded Tokio runtime, the runtime is informed so that other
/// tasks scheduled on this thread can be moved to other worker threads.
pub(crate) fn run_blocking_in_place<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

fn write_map_output(
    data_path: &Path,
    spill_path: &Path,
    buffers: Vec<Vec<RecordBatch>>,
    spill: Option<SpillFile>,
    records: Vec<u64>,
    fsync: bool,
) -> Result<()> {
    let tmp_data_path = data_path.with_extension("data.tmp");
    let file = File::create(&tmp_data_path)?;
    let mut out = CountingWriter::new(BufWriter::with_capacity(SHUFFLE_FILE_BUFFER_SIZE, file));
    let mut spill = spill.map(SpillFile::into_parts).transpose()?;
    let mut copy_buffer = if spill.is_some() {
        vec![0u8; SHUFFLE_FILE_BUFFER_SIZE]
    } else {
        vec![]
    };
    let mut offsets = Vec::with_capacity(buffers.len() + 1);
    offsets.push(0);
    let mut checksums = Vec::with_capacity(buffers.len());
    for (c, batches) in buffers.iter().enumerate() {
        if let Some((file, ranges)) = spill.as_mut() {
            for (offset, length) in &ranges[c] {
                copy_range(file, *offset, *length, &mut out, &mut copy_buffer)?;
            }
        }
        encode_batches(&mut out, batches)?;
        offsets.push(out.position);
        checksums.push(out.take_checksum());
    }
    drop(buffers);
    let file = out
        .inner
        .into_inner()
        .map_err(|e| DataFusionError::IoError(e.into_error()))?;
    if fsync {
        file.sync_all()?;
    }
    drop(file);
    if spill.take().is_some() {
        std::fs::remove_file(spill_path)?;
    }
    std::fs::rename(&tmp_data_path, data_path)?;
    let index = ShuffleIndex::try_new(offsets, records)
        .and_then(|x| x.with_checksums(checksums))
        .map_err(|e| DataFusionError::Internal(e.to_string()))?;
    index.write(&index_path(data_path), fsync)?;
    if fsync {
        // Make the renames durable.
        if let Some(dir) = data_path.parent() {
            File::open(dir)?.sync_all()?;
        }
    }
    Ok(())
}

/// The writer of the output of a map task, shared by the sinks of all its channels.
pub(crate) struct MapOutputWriter {
    state: tokio::sync::Mutex<MapOutputState>,
    status: watch::Sender<MapOutputStatus>,
    /// Whether the map output has been aborted, after which no more data is accepted.
    aborted: AtomicBool,
}

impl MapOutputWriter {
    pub fn try_new(
        data_path: PathBuf,
        channels: usize,
        options: ShuffleWriteOptions,
    ) -> Result<Self> {
        if let Some(parent) = data_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let spill_path = data_path.with_extension("spill");
        let (status, _) = watch::channel(MapOutputStatus::Writing);
        Ok(Self {
            state: tokio::sync::Mutex::new(MapOutputState {
                data_path,
                spill_path,
                spill_threshold: options.spill_threshold,
                fsync: options.fsync,
                buffers: vec![vec![]; channels],
                buffered_bytes: 0,
                records: vec![0; channels],
                spill: None,
                closed: vec![false; channels],
                num_closed: 0,
                finalized: false,
                reservation: None,
            }),
            status,
            aborted: AtomicBool::new(false),
        })
    }

    /// Tracks the buffered batches in the memory pool (e.g. the memory pool of the task
    /// that writes the map output). The buffered batches are spilled to disk when the
    /// memory pool cannot accommodate more data. This has no effect if the writer has
    /// already been registered with a memory pool.
    pub async fn register_memory_pool(&self, pool: &Arc<dyn MemoryPool>) {
        let mut state = self.state.lock().await;
        if state.reservation.is_some() {
            return;
        }
        let name = state
            .data_path
            .file_name()
            .map(|x| x.to_string_lossy().to_string())
            .unwrap_or_default();
        let reservation = MemoryConsumer::new(format!("ShuffleWriter[{name}]"))
            .with_can_spill(true)
            .register(pool);
        // Account for batches buffered before registration if possible. Otherwise,
        // they are spilled together with the next batch that cannot be accommodated.
        let _ = reservation.try_grow(state.buffered_bytes);
        state.reservation = Some(reservation);
    }

    pub fn subscribe_status(&self) -> watch::Receiver<MapOutputStatus> {
        self.status.subscribe()
    }

    fn fail(&self, e: DataFusionError) -> DataFusionError {
        self.abort(&e.to_string());
        e
    }

    /// Aborts the map output if it is not finalized yet (e.g. because the map task has
    /// failed or has been canceled). Readers waiting for the map output are notified
    /// immediately, and no more data is accepted. The partial output is cleaned up
    /// when the writer is dropped.
    pub fn abort(&self, reason: &str) {
        self.aborted.store(true, Ordering::Release);
        self.status.send_if_modified(|status| {
            if *status == MapOutputStatus::Writing {
                *status = MapOutputStatus::Failed(reason.to_string());
                true
            } else {
                false
            }
        });
        // Release the buffered data early if the writer is not in use.
        if let Ok(mut state) = self.state.try_lock() {
            if !state.finalized {
                let _ = state.take_buffers();
                state.free_memory();
            }
        }
    }

    fn check_aborted(&self) -> Result<()> {
        if self.aborted.load(Ordering::Acquire) {
            return Err(DataFusionError::Execution(
                "the shuffle output has been aborted".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn write(&self, channel: usize, batch: RecordBatch) -> Result<()> {
        let mut state = self.state.lock().await;
        self.check_aborted()?;
        if state.closed.get(channel) != Some(&false) {
            return Err(DataFusionError::Internal(format!(
                "cannot write to closed or invalid shuffle channel {channel}"
            )));
        }
        let size = batch.get_array_memory_size();
        state.records[channel] += batch.num_rows() as u64;
        state.buffered_bytes += size;
        state.buffers[channel].push(batch);
        // The batch stays buffered even if the memory pool cannot accommodate it,
        // but then all the buffered batches are spilled immediately.
        let reserved = state
            .reservation
            .as_mut()
            .is_none_or(|r| r.try_grow(size).is_ok());
        if !reserved || state.buffered_bytes >= state.spill_threshold {
            state.spill().await.map_err(|e| self.fail(e))?;
        }
        Ok(())
    }

    /// Closes the channel. The map output is finalized when all channels are closed.
    pub async fn close(&self, channel: usize) -> Result<()> {
        let mut state = self.state.lock().await;
        self.check_aborted()?;
        match state.closed.get_mut(channel) {
            Some(closed @ false) => *closed = true,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "cannot close closed or invalid shuffle channel {channel}"
                )));
            }
        }
        state.num_closed += 1;
        if state.num_closed == state.closed.len() {
            state.finalize().await.map_err(|e| self.fail(e))?;
            self.status.send_if_modified(|status| {
                if *status == MapOutputStatus::Writing {
                    *status = MapOutputStatus::Finalized;
                    true
                } else {
                    false
                }
            });
        }
        Ok(())
    }
}

/// A reader of a byte range of a data file, which may be shared by multiple
/// consecutive Arrow IPC stream readers.
/// A reader that computes the CRC-32 checksum of the bytes read.
struct ChecksumReader<R> {
    inner: R,
    hasher: crc32fast::Hasher,
}

impl<R: Read> Read for ChecksumReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

#[derive(Clone)]
struct RegionReader(Arc<Mutex<BufReader<ChecksumReader<Take<File>>>>>);

impl RegionReader {
    /// Returns the checksum of the bytes read from the file so far.
    fn checksum(&self) -> std::io::Result<u32> {
        let reader = self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("poisoned shuffle region reader"))?;
        Ok(reader.get_ref().hasher.clone().finalize())
    }

    fn is_exhausted(&self) -> std::io::Result<bool> {
        let mut reader = self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("poisoned shuffle region reader"))?;
        Ok(reader.fill_buf()?.is_empty())
    }
}

impl Read for RegionReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("poisoned shuffle region reader"))?
            .read(buf)
    }
}

struct RegionStreamState {
    region: RegionReader,
    /// The expected checksum of the region, if the index has checksums.
    checksum: Option<u32>,
    current: Option<StreamReader<RegionReader>>,
    done: bool,
}

impl RegionStreamState {
    fn next_batch(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        if self.done {
            return None;
        }
        let out = self.read_next();
        // The stream ends after the region is exhausted or after the first error.
        if !matches!(out, Some(Ok(_))) {
            self.done = true;
            self.current = None;
        }
        out
    }

    fn verify_checksum(&self) -> Result<(), ArrowError> {
        let Some(expected) = self.checksum else {
            return Ok(());
        };
        let actual = self.region.checksum()?;
        if actual != expected {
            return Err(ArrowError::IoError(
                format!(
                    "shuffle data checksum mismatch: expected {expected:08x}, found {actual:08x}"
                ),
                std::io::Error::new(std::io::ErrorKind::InvalidData, "checksum mismatch"),
            ));
        }
        Ok(())
    }

    fn read_next(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        loop {
            let reader = match self.current.as_mut() {
                Some(x) => x,
                None => {
                    match self.region.is_exhausted() {
                        // All the bytes of the region have been read, so the checksum
                        // can be verified.
                        Ok(true) => return self.verify_checksum().err().map(Err),
                        Ok(false) => {}
                        Err(e) => return Some(Err(e.into())),
                    }
                    match StreamReader::try_new(self.region.clone(), None) {
                        Ok(x) => self.current.insert(x),
                        Err(e) => return Some(Err(e)),
                    }
                }
            };
            match reader.next() {
                Some(x) => return Some(x),
                None => self.current = None,
            }
        }
    }
}

/// The maximum number of record batches decoded in one blocking read operation.
const SHUFFLE_READ_MAX_BATCHES: usize = 16;

/// The maximum (in-memory) size of record batches decoded in one blocking read operation.
const SHUFFLE_READ_MAX_BYTES: usize = 4 * 1024 * 1024;

/// Opens one channel of a completed map output for reading.
/// Returns `None` if the channel is empty.
fn open_map_output_channel(
    data_path: &Path,
    channel: usize,
) -> ExecutionResult<Option<RegionStreamState>> {
    let index = ShuffleIndex::read(&index_path(data_path))?;
    let (start, end) = index.channel_range(channel).ok_or_else(|| {
        ExecutionError::InternalError(format!(
            "shuffle channel {channel} out of range for {} channels: {}",
            index.channels(),
            data_path.display()
        ))
    })?;
    let mut file = match File::open(data_path) {
        Ok(x) => x,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ExecutionError::InternalError(format!(
                "shuffle output not found: {}",
                data_path.display()
            )));
        }
        Err(e) => return Err(e.into()),
    };
    if file.metadata()?.len() < index.data_size() {
        return Err(ExecutionError::InternalError(format!(
            "shuffle data file is truncated: {}",
            data_path.display()
        )));
    }
    if start == end {
        return Ok(None);
    }
    file.seek(SeekFrom::Start(start))?;
    let reader = BufReader::with_capacity(
        SHUFFLE_FILE_BUFFER_SIZE.min((end - start) as usize),
        ChecksumReader {
            inner: file.take(end - start),
            hasher: crc32fast::Hasher::new(),
        },
    );
    Ok(Some(RegionStreamState {
        region: RegionReader(Arc::new(Mutex::new(reader))),
        checksum: index.channel_checksum(channel),
        current: None,
        done: false,
    }))
}

/// The blocking reader of one channel of a completed map output.
enum ChannelReader {
    Unopened { data_path: PathBuf, channel: usize },
    Open(RegionStreamState),
    Done,
}

impl ChannelReader {
    fn is_done(&self) -> bool {
        matches!(self, ChannelReader::Done)
    }

    /// Reads the next few record batches, which performs blocking I/O.
    /// The reader is done after the channel is exhausted or after the first error.
    fn read_chunk(&mut self) -> Vec<TaskStreamResult<RecordBatch>> {
        if let ChannelReader::Unopened { data_path, channel } = self {
            match open_map_output_channel(data_path, *channel) {
                Ok(Some(state)) => *self = ChannelReader::Open(state),
                Ok(None) => {
                    *self = ChannelReader::Done;
                    return vec![];
                }
                Err(e) => {
                    *self = ChannelReader::Done;
                    return vec![Err(TaskStreamError::Unknown(e.to_string()))];
                }
            }
        }
        let ChannelReader::Open(state) = self else {
            return vec![];
        };
        let mut out = vec![];
        let mut bytes = 0;
        let mut done = false;
        while out.len() < SHUFFLE_READ_MAX_BATCHES && bytes < SHUFFLE_READ_MAX_BYTES {
            match state.next_batch() {
                Some(Ok(batch)) => {
                    bytes += batch.get_array_memory_size();
                    out.push(Ok(batch));
                }
                Some(Err(e)) => {
                    out.push(Err(TaskStreamError::Unknown(e.to_string())));
                    done = true;
                    break;
                }
                None => {
                    done = true;
                    break;
                }
            }
        }
        if done {
            *self = ChannelReader::Done;
        }
        out
    }
}

/// Reads the data of one channel of a completed map output.
///
/// The returned stream is lazy. Opening the files and reading and decoding the data
/// happen in blocking tasks, off the asynchronous worker threads, and only when the
/// stream is polled, so the consumer controls the pace of reading (backpressure).
/// Errors (e.g. missing shuffle output) are returned as stream items.
pub(crate) fn read_map_output_channel(data_path: PathBuf, channel: usize) -> TaskStreamSource {
    let reader = ChannelReader::Unopened { data_path, channel };
    let stream = futures::stream::unfold(
        (Some(reader), VecDeque::new()),
        |(mut reader, mut pending)| async move {
            loop {
                if let Some(item) = pending.pop_front() {
                    return Some((item, (reader, pending)));
                }
                let mut r = reader.take().filter(|r| !r.is_done())?;
                match tokio::task::spawn_blocking(move || {
                    let chunk = r.read_chunk();
                    (r, chunk)
                })
                .await
                {
                    Ok((r, chunk)) => {
                        reader = Some(r);
                        pending.extend(chunk);
                    }
                    Err(e) => {
                        let error = TaskStreamError::Unknown(format!("shuffle read failed: {e}"));
                        return Some((Err(error), (None, pending)));
                    }
                }
            }
        },
    );
    Box::pin(stream)
}
