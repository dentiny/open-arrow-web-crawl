# open-arrow-web-crawl

Read Common Crawl's archived WARC records as Apache Arrow batches using
[Open Arrow Access](https://github.com/dentiny/open-arrow-access).

## Common Crawl example

```sh
cargo run --example common_crawl
cargo run --example common_crawl -- CC-MAIN-2026-43 1791170053823.21 2 1000
cargo run --example common_crawl -- CC-MAIN-2026-43 all 1 100
```

[`examples/common_crawl.rs`](examples/common_crawl.rs) follows ml-infra's
`lance/lance_migration/crates/inspector/src/provider/warc.rs`: resolve the
crawl-wide `warc.paths.gz`, optionally filter an exact segment prefix, then
decompress selected `.warc.gz` files and stream their records as Arrow batches.
Public HTTPS access requires no credentials.

Arguments are the crawl ID, segment ID (`all` for all segments), positive file
limit, and positive total record limit. Defaults read at most one archive and
100 records. The full inventory is resolved before applying the file limit;
record limits bound emitted rows, not exact download bytes or a resumable shard.
The example prints schema/batch counts; replace the indicated line with a sink
to persist data.

One row is one physical WARC record, including requests, responses, metadata and
revisits. Bodies remain binary and can include HTTP headers and HTML. Text
extraction, page filtering and deduplication are separate downstream steps.
