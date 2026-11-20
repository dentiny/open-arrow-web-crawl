# open-arrow-web-crawl

Convert Common Crawl's archived WARC captures into a Lance dataset using
[Open Arrow Access](https://github.com/dentiny/open-arrow-access) and
[Open Arrow Sink](https://github.com/dentiny/open-arrow-sink).

## Common Crawl → Lance

```sh
cargo run --locked --bin common_crawl
cargo run --locked --bin common_crawl -- ./captures.lance CC-MAIN-2026-43 1791170053823.21 2 1000
cargo run --locked --bin common_crawl -- ./crawl-sample.lance CC-MAIN-2026-43 all 1 100
```

[`src/common_crawl/main.rs`](src/common_crawl/main.rs) handles the CLI.
[`src/common_crawl/common_crawl.rs`](src/common_crawl/common_crawl.rs) composes:

1. `CommonCrawlWarcAccessor` / `CommonCrawlSegmentAccessor` resolve crawl or
   segment membership from the crawl-wide `warc.paths.gz`.
2. `CrawlAccessor::files` executes the selected WARC files with bounded parallel
   reading and ordered emission. Gzip decoding is explicit for both the inventory
   and the archives.
3. A scan-wide record limit slices the final Arrow batch.
4. `open_arrow_sink::lance::LanceDatasetSink::write_all` streams batches into
   Lance and commits the dataset at EOF. It aborts best-effort on read/write errors.

Arguments are the local destination path (or `file://` URL), crawl ID, segment
ID (`all` for the full crawl), positive file limit, and positive total record
limit. Defaults are `./common-crawl.lance`, `CC-MAIN-2026-43`,
`1791170053823.21`, one file and 100 records. The sink uses Create mode, so use a
new destination for each run. Public HTTPS input needs no credentials.

The full inventory is resolved before applying the file limit. Record limits
bound committed rows, not exact download bytes, and are not resumable shards.
Batches stream from the accessor to the sink without collecting the dataset in
memory. The program prints the sink's actual committed row count.

One row is one physical WARC record, including requests, responses, metadata and
revisits. Headers and binary bodies are preserved in Lance. Bodies can contain
HTTP headers and HTML; text extraction, page filtering and deduplication remain
separate downstream steps.

Building the Lance dependency requires a C/C++ toolchain and `protoc` on `PATH`.
