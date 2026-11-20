# open-arrow-web-crawl

Convert Common Crawl's archived WARC captures into a Lance dataset using
[Open Arrow Access](https://github.com/dentiny/open-arrow-access) and
[Open Arrow Sink](https://github.com/dentiny/open-arrow-sink).

## Common Crawl → Lance

```sh
cargo run --locked --bin common_crawl
cargo run --locked --bin common_crawl -- ./captures.lance CC-MAIN-2026-43 1791170053823.21
```

[`src/common_crawl/main.rs`](src/common_crawl/main.rs) handles the CLI.
[`src/common_crawl/common_crawl.rs`](src/common_crawl/common_crawl.rs) composes:

1. `CommonCrawlSegmentAccessor` resolves segment membership from the crawl-wide
   `warc.paths.gz`.
2. `CrawlAccessor::new` wraps the service accessor and executes the segment's
   WARC files with bounded parallel reading and unordered emission. Gzip decoding
   is explicit for both the inventory and the archives.
3. `open_arrow_sink::lance::LanceDatasetSink::write_all` streams batches into
   Lance and commits the dataset at EOF. It aborts best-effort on read/write errors.

Arguments are the local destination path (or `file://` URL), crawl ID, and segment
ID. Defaults are `./common-crawl.lance`,
`CC-MAIN-2026-43`, and `1791170053823.21`. Every file and record in the selected
segment is converted. The sink uses Create mode, so use a new
destination for each run. Public HTTPS input needs no credentials.

Batches stream from the accessor to the sink without collecting the dataset in
memory. The conversion returns the sink's commit summary.

One row is one physical WARC record, including requests, responses, metadata and
revisits. Headers and binary bodies are preserved in Lance. The `body` column
uses LZ4 compression. Bodies can contain
HTTP headers and HTML; text extraction, page filtering and deduplication remain
separate downstream steps.

Building the Lance dependency requires a C/C++ toolchain and `protoc` on `PATH`.

## PMC → Lance

```sh
cargo run --locked --bin pmc
cargo run --locked --bin pmc -- ./article.lance metadata/PMC10009416.1.json
cargo run --locked --bin pmc -- ./articles.lance metadata/
```

[`src/pmc/main.rs`](src/pmc/main.rs) reads the destination and bucket-relative
source key. [`src/pmc/pmc.rs`](src/pmc/pmc.rs) follows the conversion service's
PMC pipeline: `PmcDatasetAccessor` → unordered `CrawlAccessor` → rename
`content_url` to `content` → `DownloadBlobLayer` → `LanceDatasetSink`.
Metadata batches coalesce to 64 rows. The public PMC bucket is accessed
anonymously in `us-east-1` and registered with the sink for article downloads.

The default input is one article's metadata JSON, with PDF selected as in the
service's default configuration. A `metadata/` prefix reads every matching
article. All upstream metadata columns are retained; `content_url` becomes the
Lance `content` blob containing the downloaded PDF, rather than a URL reference.
Rows with missing or failed PDF downloads are skipped, matching the service's
default recovery policy. Create mode requires a new destination.

For an inventory shard, pass its bucket-relative `.csv.gz` key as the source
and its manifest key as the third argument:

```sh
cargo run --locked --bin pmc -- ./shard.lance "$PMC_SHARD_KEY" "$PMC_MANIFEST_KEY"
```

Set these keys from a published PMC inventory manifest. This uses
`PmcInventoryShardAccessor` to read only that shard and the metadata it names;
the library verifies shard membership. Use a distinct output for each shard.
