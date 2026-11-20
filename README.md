# open-arrow-web-crawl

Convert Common Crawl's archived WARC captures into a Lance dataset using
[Open Arrow Access](https://github.com/dentiny/open-arrow-access) and
[Open Arrow Sink](https://github.com/dentiny/open-arrow-sink).

## Common Crawl → Lance

```sh
cargo run --locked --bin common_crawl
cargo run --locked --bin common_crawl -- ./captures.lance CC-MAIN-2026-43 1791170053823.21
cargo run --locked --bin common_crawl -- ./crawl.lance CC-MAIN-2026-43 all
```

[`src/common_crawl/main.rs`](src/common_crawl/main.rs) handles the CLI.
[`src/common_crawl/common_crawl.rs`](src/common_crawl/common_crawl.rs) composes:

1. `CommonCrawlWarcAccessor` / `CommonCrawlSegmentAccessor` resolve crawl or
   segment membership from the crawl-wide `warc.paths.gz`.
2. `CrawlAccessor::new` wraps the service accessor and executes all selected WARC files with bounded parallel
   reading and ordered emission. Gzip decoding is explicit for both the inventory
   and the archives.
3. `open_arrow_sink::lance::LanceDatasetSink::write_all` streams batches into
   Lance and commits the dataset at EOF. It aborts best-effort on read/write errors.

Arguments are the local destination path (or `file://` URL), crawl ID, and segment
ID (`all` for the full crawl). Defaults are `./common-crawl.lance`,
`CC-MAIN-2026-43`, and `1791170053823.21`. Every file and record in the selected
segment or crawl is converted. The sink uses Create mode, so use a new
destination for each run. Public HTTPS input needs no credentials.

Batches stream from the accessor to the sink without collecting the dataset in
memory. The conversion returns the sink's commit summary.

One row is one physical WARC record, including requests, responses, metadata and
revisits. Headers and binary bodies are preserved in Lance. Bodies can contain
HTTP headers and HTML; text extraction, page filtering and deduplication remain
separate downstream steps.

Building the Lance dependency requires a C/C++ toolchain and `protoc` on `PATH`.
