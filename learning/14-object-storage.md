# 14 · Object Storage: Reaching S3, GCS, Azure, HDFS, HTTP, and More

> **You'll learn:** how Sail turns a URL like `s3://bucket/path` into a storage
> client, which schemes are supported, where credentials and regions come from,
> and how laziness, logging, and runtime isolation are layered around every
> store.
>
> **Crate:** `sail-object-store`


## In One Minute

- All file I/O goes through one **dynamic registry** that creates a storage
  client the first time it sees a `(scheme, bucket)` pair, then caches it.
- Supported: local files, S3 (and S3-compatible), GCS, Azure, HTTP, Hugging Face
  datasets, and HDFS (optional build feature).
- Clients are created **lazily**, on first actual I/O, and their I/O runs on the
  dedicated **io** thread pool.
- S3 credentials come from the AWS default chain. The region is detected
  automatically if not set.
- Every storage call can be logged at debug level, so you can see exactly what
  a query read.

---

## 1. What This Component Does

All file I/O in Sail goes through the Apache Arrow `object_store` interface:
listing, reading byte ranges of Parquet files, writing output files, committing
Delta logs, and reading Iceberg metadata. Sail provides a **dynamic object
store registry** that DataFusion consults whenever it sees a URL:

| URL scheme | Backend | Notes |
|------------|---------|-------|
| `file://` or plain paths | Local file system | Always registered |
| `memory://` | In-memory store | Tests |
| `s3://`, `s3a://`, `https://…amazonaws.com/…` | Amazon S3 (and S3-compatible stores) | Credentials from the AWS default chain. The region is detected automatically. |
| `gs://` | Google Cloud Storage | Configured from environment variables |
| `az://`, `abfs://`, `abfss://`, `adl://`, `https://…blob.core.windows.net/…` | Azure Blob and ADLS Gen2 | Configured from environment variables |
| `http://`, `https://` (other hosts) | Generic HTTP or WebDAV | Read-only in practice |
| `hf://datasets/<user>/<dataset>[@rev]/<path>` | Hugging Face datasets | Only the `datasets` repository type |
| `hdfs://` | HDFS | Only when built with the `hdfs` feature |

---

## 2. Why It's Built This Way

**No up-front registration.** Spark users expect `spark.read.parquet("s3://any-bucket/…")`
to just work. DataFusion's default registry needs every bucket registered
before use. Sail's registry **creates a store on demand** the first time a
`(scheme, authority)` pair is seen (for example `s3` + `my-bucket`), then
caches it. A single query can read from one bucket and write to another, or
cross clouds, with no setup.

**Clients are created lazily.** Building an S3 client may load AWS
configuration, refresh credentials, or make a network call to find the bucket's
region. Sail wraps cloud stores in a **lazy** layer, so this work happens on
first use, not when a plan merely mentions a path. Planning stays fast, and
unused paths cost nothing.

**Storage I/O runs on its own runtime.** Each store is wrapped in a **runtime
layer** that creates the client and runs its I/O on Sail's dedicated **io**
Tokio runtime (see [02](02-entry-points-and-server.md)). HTTP clients are tied
to the runtime that created them, and slow storage shouldn't starve query
threads.

**Every call is logged.** A **logging layer** records object store operations
(get, put, list, …) at debug level, which is very useful when tracing what a
query actually read.

---

## 3. How It Works

### 3.1 Getting a store

```text
DataFusion needs url  ─►  registry.get_store(url)
                              │ key = (scheme, authority)
                              ├─ in cache? → return it
                              └─ no → build:  Runtime layer ( Logging layer ( Lazy layer ( real client ) ) )
                                     insert into cache (atomically) → return
```

The cache is a concurrent map, and get-or-insert is atomic, so two tasks asking
for the same bucket at the same moment get the same store.

### 3.2 Amazon S3

1. Start from environment-based settings (`AWS_*` variables such as
   `AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, `AWS_ACCESS_KEY_ID`, …).
2. Load the **AWS SDK default configuration** once per process: profiles, SSO,
   web identity, instance metadata, and so on. If it provides credentials, use
   them through a caching credential provider so they refresh automatically.
3. **Parse the URL** to find the bucket. Supported forms include `s3://bucket/key`,
   `s3a://bucket/key`, virtual-hosted `https://bucket.s3.region.amazonaws.com/key`,
   and path-style `https://s3.region.amazonaws.com/bucket/key`.
4. **Find the region**: from the URL or environment, else from the AWS
   configuration, else by asking S3 where the bucket lives.

This works with S3-compatible services such as MinIO or R2 when you set the
endpoint (and `AWS_ALLOW_HTTP=true` for plain HTTP).

### 3.3 Google Cloud Storage, Azure, and HTTP

These are built from environment variables supported by the `object_store`
crate, for example `GOOGLE_SERVICE_ACCOUNT` / `GOOGLE_APPLICATION_CREDENTIALS`
for GCS, `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, or
`AZURE_STORAGE_SAS_KEY` for Azure, plus the URL. HTTP stores read client
options from matching environment variables.

### 3.4 Hugging Face

`hf://datasets/user/dataset@revision/path/*.parquet` is parsed into user,
dataset, optional revision, and path. Listing and reading go through the
Hugging Face Hub API, so public datasets can be queried directly.

### 3.5 Where the registry is used

- **Scans and writes** in every table format (see [12](12-data-sources-and-formats.md), [13](13-lakehouse-delta-and-iceberg.md))
- **Delta logs and Iceberg metadata**, which use the same stores
- **Workers**: each worker session builds its own registry (see
  [06](06-session-and-runtime.md)), so workers read data directly from storage
  rather than through the driver.

---

## 4. Examples

```bash
# Static credentials and a custom S3-compatible endpoint (for example MinIO)
export AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio123
export AWS_ENDPOINT=http://minio:9000 AWS_ALLOW_HTTP=true AWS_REGION=us-east-1
sail spark server
```

```python
spark.read.parquet("s3://analytics/events/2024/").count()
spark.read.csv("gs://public-bucket/data/*.csv", header=True).show()
spark.read.parquet("abfss://container@account.dfs.core.windows.net/path/").show()
spark.read.parquet("hf://datasets/user/dataset/data/*.parquet").show()
df.write.parquet("s3://output-bucket/results/")    # a different bucket, no extra setup
```

### 4.1 Where S3 credentials come from

The AWS default chain is tried in order, and the first match wins:

| Source | Example |
|--------|---------|
| Environment variables | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` |
| Shared config and credentials files | `~/.aws/credentials`, selected by `AWS_PROFILE` |
| SSO and web identity | `aws sso login`, or EKS IRSA (`AWS_WEB_IDENTITY_TOKEN_FILE`) |
| Container and instance metadata | ECS task role, EC2 instance profile |

Credentials are cached and refreshed automatically. Temporary credentials keep
working through long jobs.

### 4.2 How the region is chosen

| Situation | Region used |
|-----------|-------------|
| URL is `https://bucket.s3.eu-west-1.amazonaws.com/…` | `eu-west-1`, from the URL |
| `AWS_REGION=us-east-2` is set | `us-east-2` |
| AWS profile has a region | That region |
| None of the above | Sail asks S3 for the bucket's region (one extra request, then cached with the store) |

---

## 5. End-to-End: First Read of `s3://analytics/events/`

| # | What happens |
|---|--------------|
| 1 | The listing step asks the registry for a store for `s3://analytics`. |
| 2 | Cache miss. The registry builds runtime → logging → lazy(S3) and caches it under `(s3, analytics)`. No network calls yet. |
| 3 | The first `list` call triggers the lazy initializer on the io runtime: load the AWS configuration, set up the credential provider, parse the URL, and resolve the region if needed. |
| 4 | Listing returns object metadata. Parquet footers are read with ranged GETs. Each call is logged at debug level. |
| 5 | Later queries, and other sessions in the same process, reuse the cached store and credentials. |

---

## 6. Common Problems

| Symptom | Likely cause and fix |
|---------|----------------------|
| "S3 bucket name must be specified in url" | Malformed URL. Use `s3://bucket/key`. |
| Access denied on S3 | Credentials not found by the AWS chain. Check `AWS_PROFILE`, environment variables, and the instance role. |
| Very slow first access to a bucket | Region detection or credential refresh. Set `AWS_REGION` to skip detection. |
| "unsupported object store URL" | The scheme isn't supported, or HDFS support wasn't compiled in. |
| "unsupported repository type" with `hf://` | Only `hf://datasets/...` is supported. |
| Need to see what was read | Turn on debug logs for the object store crate (the logging layer). |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Dynamic registry and scheme dispatch | `crates/sail-object-store/src/registry.rs` |
| S3 (credentials, URL parsing, region detection) | `crates/sail-object-store/src/s3.rs` |
| Hugging Face store | `crates/sail-object-store/src/hugging_face.rs` |
| Lazy, logging, and runtime layers | `crates/sail-object-store/src/layers/` |
| Where the registry is created for each session | `crates/sail-session/src/runtime.rs` |
