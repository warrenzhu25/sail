# 10 · Shuffle and Streams: Moving Data Between Tasks

When the driver cuts a plan into stages (see
[09](09-local-and-cluster-execution.md)), data has to flow from tasks in one
stage to tasks in the next. This doc explains the data plane that moves it: what
a stream is, how the shuffle write and read operators work, how readers and
writers find each other even when they start in the wrong order, how slow
consumers are kept from exhausting memory, how workers fetch data from each
other, and where Sail's disk-based shuffle stands.

```text
 Stage 0 task (partition p)                         Stage 1 task (partition q)
 ┌───────────────────────────┐                      ┌────────────────────────────┐
 │ … plan fragment …         │                      │ shuffle read                │
 │ shuffle write             │  channel q of p      │   reads channel q from      │
 │   hash(k) → channels 0..N ├────────────────────► │   every upstream partition  │
 └───────────────────────────┘  (memory or Flight)  │ … plan fragment …           │
                                                    └────────────────────────────┘
```

---

## Everything is a named stream

Every piece of data exchanged between tasks is a **stream** of Arrow record
batches, and every stream has a name made of five parts: the job, the stage, the
partition, the attempt, and the channel. "Channel 3 from attempt 0 of partition
5 of stage 2 of job 17" names exactly one stream.

Two parts of that name deserve a comment. The *channel* is one slice of a task's
output: when a task's rows are split by hash into eight buckets, each bucket is
a channel. The *attempt* means that when a task is retried, its new output gets
new names. A retry can never accidentally mix its data with that of the attempt
that failed.

## Why streams, why Arrow

Sail never converts data to rows between stages. Batches move as Arrow arrays in
memory, or as Arrow's binary IPC format over the network. There's no custom
serialization format to design or maintain, and almost no parsing cost on the
receiving side.

By default, data also never touches disk. In the default **pipelined** mode, a
downstream task reads its input *while* the upstream task is still producing
it. That gives low latency, which suits interactive queries. The trade-off, as
[09](09-local-and-cluster-execution.md) explains, is that producers and
consumers must run at the same time, and losing a task means rerunning its whole
connected region.

## Writing: the shuffle writer

At the root of every upstream task, the task runner places a **shuffle writer**.
It knows, for its partition, where each output channel should go. When it runs,
it opens a destination for every channel, then reads batches from the plan
fragment below it. It splits each batch according to the stage's
distribution, either by hashing the key columns into the number of channels, or
round-robin, and sends each slice to its channel.

A destination can report that it's *closed*, which happens when the consumer no
longer needs data, for example because a LIMIT has already been satisfied. The
writer then skips that channel, and if every channel is closed, it stops early
instead of computing rows nobody wants. When the input ends, the writer closes
all its destinations. Its own output is empty, because the real output is in the
streams.

## Reading: the shuffle reader

On the other side, placeholders in the downstream plan are replaced with
**shuffle readers**. Each one knows which streams it needs, and that depends on
how the stage was cut. After a hash repartition, partition q reads channel q
from every upstream partition, which is the classic shuffle. When stages are
linked one-to-one, partition q reads everything from upstream partition q.
Before an order-preserving merge, the single reader reads everything from every
upstream partition. For a broadcast, every downstream partition reads every
upstream partition's complete output.

The reader opens all of its streams at once and merges them, passing along
batches in whatever order they arrive. Rows from different upstream tasks are
interleaved, which is fine because operators like aggregation don't care about
order. Operators that do care, such as an order-preserving merge, sort as they
read.

## Where the bytes live

Each write destination says where its stream should live. A **memory** stream is
kept in the producing process's stream manager. Readers in the same process take
it directly, and readers elsewhere fetch it over the network. A **disk** stream is
written to a file, so it can be read later and more than once, even after the
producer has exited; that's the basis of the blocking shuffle described at the
end. A third kind, **remote** storage such as an object store, is defined but not
implemented yet: asking for it returns "not implemented: remote stream".

A memory stream can be read a fixed number of times, because the job graph
calculates in advance how many consumers each stage's output has. Usually that's
one reader per stream, but for a broadcast it's one per downstream partition.
The stream keeps that many *replicas*. Because Arrow batches are
reference-counted, a replica costs a pointer copy, not a data copy.

## When the reader arrives before the writer

In pipelined mode, a downstream task can easily ask for a stream before the
upstream task has created it, since everything starts at roughly the same time.
The stream manager on each driver and worker handles this with three states.
When a reader asks for a stream that doesn't exist yet, the manager creates a
**pending** entry, hands the reader a channel immediately, and parks it there.
When the producer publishes the stream, the entry becomes **created** and the
parked reader is connected. If the producer fails, the entry becomes **failed**
and readers receive the error.

To avoid waiting forever, the manager also schedules a check when it creates a
pending entry:

```text
t=0ms    consumer asks worker A for stream K   → no K yet: create PENDING(K), park the reader
t=0ms    A schedules a check of K after cluster.task_stream_creation_timeout_secs
t=40ms   producer on A publishes K              → PENDING becomes CREATED; the reader is connected
t=41ms   batches start flowing
```

If the producer never shows up, for example because its task crashed before
starting, the check fires, the waiting reader gets an error, and the consumer
task fails. That failure makes the driver reschedule the region (see
[09](09-local-and-cluster-execution.md)).

## Keeping slow consumers from exhausting memory

What if a producer is fast and its consumer is slow? Each memory stream is a
bounded channel holding at most cluster.task_stream_buffer batches, 16 by
default. With the default batch size of 8,192 rows, that's roughly 130,000 rows
in flight per stream. When the buffer is full, the producer's next write simply
waits until the consumer catches up. A slow consumer slows the pipeline down
instead of making memory grow without limit. This is called backpressure, and
it falls out of the design for free.

## Between machines: Arrow Flight

Every worker, and the driver, runs a small Arrow Flight service. When a reading
task needs a stream that lives on another worker, its worker uses a client for
that peer (it learns peer addresses from the driver) and makes a Flight request
whose ticket encodes the stream's five-part name. The other worker's Flight
service asks its stream manager for that stream, waiting in the pending state if
needed, and sends the batches back as Arrow IPC messages. On the receiving side,
the Flight response becomes an ordinary stream of batches, merged with the
reader's other inputs. The driver uses the same mechanism to read the final
stage's output from workers.

## Following one shuffle

Suppose stage 0 has three partitions, P0, P1, and P2, and stage 1 has two, Q0
and Q1, with rows hashed by k into two channels. Each producer splits its partial
results into channel 0 and channel 1, so there are six streams in total. Q0
reads channel 0 from P0, P1, and P2 and merges them, and Q1 reads channel 1 from
all three. If P1 and Q0 happen to run on the same worker, Q0 reads P1's stream
straight from memory, while P0's and P2's streams arrive over Flight from their
workers. If Q1 is slow, its three input streams fill up to 16 batches each and
the producers pause on that channel until it catches up.

When the job ends, or when every consumer of a stage has finished, the driver
tells workers to clean up that job's streams, and their stream managers release
the memory (and delete the files, for disk streams).

## The disk-based "blocking" shuffle

Pipelined shuffle is fast but fragile, and it needs every stage's tasks running
at once. Sail also has a **blocking** mode: upstream tasks write each channel to
disk and exit, and downstream stages start only after the upstream stage has
finished. That frees capacity between stages, and it unlocks two features. One is
resizing downstream stages based on how much data was actually produced
(adaptive coalescing). The other is recovering from a lost worker by recomputing
only the output it held.

On this branch, the disk stream itself is implemented and tested. Files are
written under a temporary name and renamed into place, with a small index file
recording byte and row counts. Adaptive coalescing based on those counts is
implemented and used in tests. But blocking mode can't be turned on from
configuration: the driver always uses pipelined mode.

The disk-shuffle-hardening branch makes blocking shuffle usable. It adds the
cluster.shuffle_mode, cluster.shuffle_dir, and adaptive coalescing settings,
has workers report shuffle statistics to the driver, retries stages when a
worker is lost or shuffle data can't be fetched, gives each process its own
shuffle directory, and buffers disk I/O. Those changes are described in
docs/concepts/architecture-deep-dive.md and
docs/concepts/architecture/adaptive-disk-shuffle-design.md.

---

## Common problems

If a reader fails while waiting on a pending stream, the upstream task never
started or crashed; check its status and its worker's logs, and raise
cluster.task_stream_creation_timeout_secs if workers start slowly. "Not
implemented: remote stream" means something requested object-store stream
storage, which doesn't exist yet. Memory growing on a worker during a big shuffle
reflects pipelined streams holding data until it's read; fewer partitions, a
bounded memory pool, or the disk shuffle on the hardening branch help. And a
retried job should never produce duplicates, because the attempt is part of every
stream's name; if you see duplicates, report them along with the task attempts
from system.execution.tasks.

## Where to look in the code

Everything is in crates/sail-execution. The shuffle write and read operators and
the stage-input placeholder are under src/plan/ (shuffle_write.rs,
shuffle_read.rs, stage_input.rs). Stream names are defined in src/id.rs. Read
and write locations, and the merging of several input streams, are under
src/stream/ (reader.rs, writer.rs, merge.rs). The stream manager with its
pending, created, and failed states is src/stream_manager/core.rs, and the memory
and disk streams are in src/stream_manager/local.rs. Choosing between local and
remote access is under src/stream_accessor/, and the Arrow Flight service under
src/stream_service/. The input and output locations recorded in task definitions
are in src/task/definition.rs. The disk shuffle design is in
docs/concepts/architecture/adaptive-disk-shuffle-design.md.
