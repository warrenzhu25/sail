# 14 · Object Storage: Reaching S3, GCS, Azure, and More

All of Sail's file I/O goes through one abstraction: the object store interface
from the Apache Arrow project. Listing directories, reading byte ranges of
Parquet files, writing output files, committing Delta logs, and reading Iceberg
metadata all use it. This doc explains how Sail turns a URL into a working
storage client, which kinds of storage are supported, where credentials and
regions come from, and why every client is wrapped in a few small layers.

---

## The problem: Spark users expect any path to just work

A Spark user writes `spark.read.parquet("s3://any-bucket/path")` and expects it
to work, with no setup per bucket. DataFusion's default approach is the
opposite: every storage location must be registered before it can be used. That
doesn't fit a Spark-compatible engine, where a single query may read from one
bucket and write to another, or even cross clouds.

Sail's answer is a **dynamic registry**. When DataFusion needs storage for a URL,
it asks the registry, which looks at the URL's scheme and authority (for
example s3 plus a bucket name). If it has already built a client for that pair,
it returns it. If not, it builds one, remembers it, and returns it. The lookup
and insertion happen as one atomic step, so two tasks asking for the same bucket
at the same moment always share one client.

## What's supported

Local files (plain paths or file:// URLs) are always available. Amazon S3 works
with s3://, s3a://, and HTTPS URLs on amazonaws.com, and so do S3-compatible
services such as MinIO and Cloudflare R2 once you set an endpoint. Google Cloud
Storage uses gs://. Azure Blob Storage and Data Lake Storage use az://,
abfs://, abfss://, adl://, and HTTPS URLs on Azure's domains. Other http:// and
https:// URLs are treated as generic web servers, which in practice is
read-only. Hugging Face datasets are reachable as
hf://datasets/user/dataset@revision/path; only the datasets repository type is
supported. HDFS works when Sail is built with the hdfs feature. An in-memory
store exists for tests.

## Three layers around every client

Each client the registry creates is wrapped in three layers, and each solves a
practical problem.

The innermost layer makes cloud clients **lazy**. Building an S3 client can
mean loading AWS configuration, refreshing credentials, or even making a network
call to find out which region a bucket lives in. If that happened as soon as a
plan mentioned a path, planning would be slow and paths that are never actually
read would still cost something. With the lazy layer, the real client is built
the first time it's used for I/O.

The middle layer **logs** every storage operation (reads, writes, listings) at
debug level. When you need to know exactly what a query read, turning on debug
logging for this crate tells you.

The outer layer runs the client's creation and all of its I/O on Sail's
dedicated **I/O thread pool** (see [02](02-entry-points-and-server.md)). Network
clients are often tied to the runtime that created them, and keeping storage
I/O separate means slow storage can't starve query execution of threads.

## Amazon S3 in detail

S3 is the most common case, so its setup is the most complete. Sail starts
from settings in environment variables such as AWS_ENDPOINT, AWS_ALLOW_HTTP,
and the access key variables. It then loads the AWS SDK's default configuration
once per process. That's the same credential chain the AWS command-line tool
uses. It tries environment variables, then the shared credentials and config
files (selected with AWS_PROFILE), then single sign-on and web identity (which
is how EKS service accounts work), then the container or instance role. If that
chain finds credentials, Sail uses them through a caching provider that refreshes
them automatically, so temporary credentials keep working through long jobs.

Next, Sail works out the bucket from the URL. It understands the plain
s3://bucket/key form as well as both styles of HTTPS URL: virtual-hosted, where
the bucket is part of the host name, and the older path style, where the bucket
is the first part of the path.

Finally, it needs a region. If the URL contains one, or AWS_REGION is set, or
the AWS profile names one, that's used. Otherwise Sail asks S3 where the bucket
lives, which costs one extra request the first time and is then remembered with
the client. Setting AWS_REGION skips that request.

For an S3-compatible service, point Sail at it with environment variables:

```bash
export AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio123
export AWS_ENDPOINT=http://minio:9000 AWS_ALLOW_HTTP=true AWS_REGION=us-east-1
sail spark server
```

## Google Cloud Storage, Azure, HTTP, and Hugging Face

The other cloud clients are configured from the environment variables that the
object store library understands, for example GOOGLE_APPLICATION_CREDENTIALS
for GCS, and AZURE_STORAGE_ACCOUNT_NAME with an account key or SAS token for
Azure. Generic HTTP stores read their client options from matching environment
variables. For Hugging Face, Sail parses the user, dataset, optional revision,
and path from the URL and talks to the Hugging Face Hub API, so public datasets
can be queried directly:

```python
spark.read.parquet("hf://datasets/user/dataset/data/*.parquet").show()
```

## Who uses the registry

Every table format uses it to list, read, and write files (see
[12](12-data-sources-and-formats.md)), and so do the Delta and Iceberg
implementations for their logs and metadata (see
[13](13-lakehouse-delta-and-iceberg.md)). Every session has its own registry,
and so does every worker session, which is why cluster workers read data
directly from storage rather than through the driver.

## Following a first read

The first time a query touches s3://analytics/events/, the listing step asks the
registry for storage for s3 plus analytics. There's nothing cached, so the
registry builds the three-layer stack around a lazy S3 client and remembers it.
No network calls have happened yet. The first listing request triggers the lazy
layer, which, on the I/O thread pool, loads the AWS configuration, sets up the
credential provider, parses the URL, and resolves the region if needed. The
listing returns the files, and Parquet footers are then read with byte-range
requests, each one logged at debug level. Later queries in the same session reuse
the same client and credentials.

---

## Common problems

"S3 bucket name must be specified in url" means the URL is malformed; use
s3://bucket/key. Access denied usually means the credential chain found nothing
suitable, so check AWS_PROFILE, the environment variables, and the instance or
pod role. A slow first access to a bucket is usually region detection or a
credential refresh, which setting AWS_REGION avoids. "Unsupported object store
URL" means the scheme isn't supported, or HDFS support wasn't compiled in.
"Unsupported repository type" on an hf:// URL means it isn't under
hf://datasets/. And to see what a query actually read, turn on debug logging for
the object store crate.

## Where to look in the code

Everything lives in crates/sail-object-store. The dynamic registry and the
choice of client for each scheme are in src/registry.rs. S3 credentials, URL
parsing, and region detection are in src/s3.rs, and the Hugging Face client is
in src/hugging_face.rs. The lazy, logging, and thread-pool layers are under
src/layers/. The registry is created for each session in
crates/sail-session/src/runtime.rs.
