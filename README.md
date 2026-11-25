# open-arrow-web-crawl

Runnable Rust examples demonstrating how to use
[**open-arrow-access**](https://github.com/dentiny/open-arrow-access) and
[**open-arrow-sink**](https://github.com/dentiny/open-arrow-sink) together to ingest
public datasets into Lance.

```text
Data source → open-arrow-access → Arrow RecordBatch stream → open-arrow-sink → Lance
```

`open-arrow-access` discovers source files, reads and decompresses bytes, decodes
records into typed Arrow batches, and executes reads through `CrawlAccessor`.
`open-arrow-sink` consumes those batches, applies write policies such as column
compression or blob downloads, and commits the destination dataset. Arrow batches
are the interface between the two crates.

Each example has a small `main.rs` for arguments and a second file containing
the pipeline. Start with the pipeline files to see how the library APIs compose:

| Example | Pipeline | What it demonstrates |
| --- | --- | --- |
| Common Crawl | [common_crawl.rs](src/common_crawl/common_crawl.rs) | Segment discovery, gzip WARC decoding, and LZ4 compression of binary `body` values. |
| PMC | [pmc.rs](src/pmc/pmc.rs) | Metadata discovery, batch coalescing, field remapping, and downloading article content into Lance blobs. |
| BHL | [bhl.rs](src/bhl/bhl.rs) | Native item/part discovery, shared read-only metadata and one OCR page per row. |
| arXiv DuckDB | [arxiv.rs](src/arxiv-duckdb/arxiv.rs) | Stream an official TAR shard with indexed local DuckDB metadata, without OAI requests. |
| arXiv | [arxiv.rs](src/arxiv/arxiv.rs) | Stream one official source/PDF TAR shard into typed content with automatic OAI metadata. |
| S2ORC v2 | [s2orc.rs](src/s2orc/s2orc.rs) | Authenticated release discovery, signed download URLs, typed JSONL decoding, and LZ4 compression of `text`. |

## The shared pattern

After constructing a source accessor and its request, the examples connect it
to a sink with the same pattern:

```rust
use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, EmissionPolicy,
    crawl::{CrawlAccessor, CrawlOptions},
    into_stream,
};
use open_arrow_sink::{SinkExt, WriteOptions, lance::LanceDatasetSink};

let crawl = CrawlAccessor::new(
    source,
    CrawlOptions::default().with_emission(EmissionPolicy::Unordered),
);
let reader = crawl.open(request).await?;
let schema = reader.metadata().schema.clone();
let batches = into_stream(reader).map_err(open_arrow_sink::Error::backend);

let summary = LanceDatasetSink::default()
    .write_all(destination, schema, WriteOptions::default(), batches)
    .await?;
```

Here `source` and `request` are provider-specific, and `destination` is a local
`file://` URL. `into_stream` connects the accessor's reader to `SinkExt::write_all`;
the error mapping preserves source errors at the sink boundary. The sink pulls
batches with backpressure and commits at EOF, returning a `CommitSummary`.

The conversion examples use unordered emission and bounded parallel reading. Common
Crawl and S2ORC configure per-column `CompressionSpec::Lz4Raw` through
`WriteOptions::with_compression`. PMC composes `SchemaEvolutionLayer` and
`DownloadBlobLayer` around the Lance sink. These are variations on the same
read → Arrow batches → write pipeline.

## Build and try an example

Use a Rust toolchain supporting edition 2024, a C/C++ toolchain, and `protoc` on
`PATH` for the Lance dependency. [Cargo.toml](Cargo.toml) lists the enabled source
and sink features. The `crawl` feature is enabled explicitly because these
examples compose `CrawlAccessor` and the built-in parallel/coalescing layers.

```sh
cargo build --locked --bins
```

For a small first run, convert one public PMC article, including its PDF:

```sh
cargo run --locked --bin pmc -- ./article.lance metadata/PMC10009416.1.json
```

All examples accept a local destination path or `file://` URL and use Create
mode, so choose a new destination for each run. They convert the complete selected
input without file or record limits. Common Crawl and PMC need no credentials;
S2ORC requires `S2_API_KEY`. The programs return errors directly and do not print
conversion progress.

## Common Crawl

```sh
cargo run --locked --bin common_crawl -- ./captures.lance CC-MAIN-2026-43 1791170053823.21
```

Arguments: `[DESTINATION] [CRAWL] [SEGMENT]`. Defaults are
`./common-crawl.lance`, `CC-MAIN-2026-43`, and `1791170053823.21`.

`CommonCrawlSegmentAccessor` selects the segment's files from the crawl-wide
`warc.paths.gz`. Inventory and archive gzip decoding are configured separately.
`CrawlAccessor` reads all selected WARC files, and the Lance sink stores the
binary `body` column with LZ4 compression.

Each output row is a physical WARC record, including requests, responses,
metadata and revisits. Bodies can contain HTTP headers and HTML; text extraction
and filtering can be added downstream. See [main.rs](src/common_crawl/main.rs)
for the entry point.

## PMC

```sh
cargo run --locked --bin pmc -- ./article.lance metadata/PMC10009416.1.json
cargo run --locked --bin pmc -- ./articles.lance metadata/
```

Arguments: `[DESTINATION] [SOURCE_KEY] [MANIFEST_KEY]`. Defaults are
`./pmc.lance`, `metadata/PMC10009416.1.json`, and no manifest. Keys are relative to
the public `pmc-oa-opendata` bucket.

`PmcDatasetAccessor` reads one metadata object or discovers a metadata directory.
With a manifest, `PmcInventoryShardAccessor` reads the selected inventory shard
and the article metadata it names:

```sh
cargo run --locked --bin pmc -- ./shard.lance "$PMC_SHARD_KEY" "$PMC_MANIFEST_KEY"
```

Set these two keys from a published PMC inventory manifest and use a separate
output for each shard.

The example coalesces metadata batches to 64 rows, renames `content_url` to
`content`, and selects that column with `DownloadBlobLayer`. The same anonymous
PMC S3 operator is registered with the sink for PDF downloads. Metadata fields
are retained, while `content` becomes a Lance blob containing the PDF bytes.
Missing or failed PDF downloads skip their rows using `DownloadFailurePolicy::SkipRow`.
See [main.rs](src/pmc/main.rs) for the entry point.

## S2ORC v2

Set `S2_API_KEY` to your Semantic Scholar Datasets API key, then run:

```sh
cargo run --locked --bin s2orc -- ./s2orc.lance latest part-00000
cargo run --locked --bin s2orc -- ./release-part.lance 2026-11-10 part-00001
```

Arguments: `[DESTINATION] [RELEASE] [FILE_NAME]`. Defaults are `./s2orc.lance`,
`latest`, and `part-00000`.

`SemanticScholarService::dataset` resolves the release and binds signed URLs to
logical names such as `part-00000`, assigned in manifest order. Gzip decoding is
explicit because these names have no compression suffix. `S2orcV2Accessor`
decodes the selected file into seven typed columns: `corpusid`, `openaccessinfo`,
`title`, `authors`, `body`, `bibliography`, and `text`.

Body text is extracted to top-level `text` and stored with LZ4 compression.
Nested metadata, lists and annotation spans retain their Arrow types in Lance.
Each invocation converts one complete file. Pin a release ID and use separate
outputs when converting multiple files; `latest` resolves once per invocation.
See [main.rs](src/s2orc/main.rs) for the entry point.

## arXiv with open-arrow-access 0.20.1

Convert one complete official arXiv TAR shard using your normal AWS credential
configuration. The bucket requires signed requester-pays access:

```sh
cargo run --locked --bin arxiv -- ./arxiv-source.lance src/arXiv_src_9107_001.tar latex
cargo run --locked --bin arxiv -- ./arxiv-pdf.lance pdf/arXiv_pdf_9107_001.tar pdf
```

The source route emits one row per selected TeX file; PDF emits one row per
article. Both retain automatic typed OAI metadata, whose official requests share
the upstream three-second limiter. Explicit shards are streamed sequentially,
without range-based re-downloads. Legacy standalone TeX and GNU TAR long names
are supported. Missing LaTeX (PDF/PostScript/DVI-only packages) and corrupt source
packages remain errors in this strict example; no PDF fallback or silent skip is
configured.

Existing Common Crawl, PMC and S2ORC pipelines keep their OpenDAL/planning paths.
Provider Connection downloads in 0.20.1 are sequential; formats requiring random
access need an OpenDAL-backed or custom ranged source.

## arXiv with local DuckDB metadata

The `arxiv-duckdb` example uses open-arrow-access 0.21.2 and a completed local
DuckDB snapshot instead of online OAI lookups. See
[the example guide](src/arxiv-duckdb/README.md) for snapshot details.

```sh
cargo run --locked --bin arxiv-duckdb -- ./arxiv-source.lance src/arXiv_src_2601_001.tar latex
cargo run --locked --bin arxiv-duckdb -- ./arxiv-pdf.lance pdf/arXiv_pdf_2601_001.tar pdf
```

Arguments: `DESTINATION SHARD [latex|pdf] [DATABASE]`. The default database is
`src/arxiv-duckdb/arxiv.duckdb` relative to the Cargo manifest, independent of the
working directory. If it is missing, the first run verifies and unpacks the complete snapshot from
the Git LFS-tracked gzip parts included in the checkout. Subsequent runs reuse
the local database. Install Git LFS before cloning, or run `git lfs pull` in an
existing checkout. Pass a fourth argument to use another
local snapshot.
The same signed AWS requester-pays access and strict content conversion rules
as the original arXiv example apply. Metadata setup runs on a blocking worker;
article readers share the read-only database and query its article ID index.
Missing or mismatched metadata fails the scan without an OAI fallback.

## BHL with the native shard accessor

Create a complete metadata database once, then reuse it for each item/part shard:

```sh
cargo run --locked --bin bhl -- create-metadata ./bhl-metadata.duckdb
cargo run --locked --bin bhl -- ./bhl-item-295621.lance ocr/item-295621/ ./bhl-metadata.duckdb
```

Creation imports all 13 official tables and closes the writers before returning.
Use a new path; discard incomplete files after a failed import before retrying.
Shard scans use `BhlShardAccessor::from_database` and `BhlShardReadRequest` from
open-arrow-access 0.20.1. The library opens the completed file read-only, validates
the prefix, discovers every listing page, excludes the aggregate TXT, and joins
metadata. CrawlAccessor owns concurrency and backpressure. The example contains
no custom listing, identity parser or metadata reconciliation callback.

OCR bytes, empty pages and all metadata lists retain their original values.
Missing/conflicting metadata fails under the library's strict default. The
metadata fingerprint labels the rows; it does not freeze the live OCR bucket.
Multiple jobs can share a completed snapshot mounted at their local database path.
