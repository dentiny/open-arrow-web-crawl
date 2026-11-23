# arxiv-duckdb

One official arXiv TAR shard → typed PDF/LaTeX rows enriched from local DuckDB
metadata → Lance. The original OAI example remains in `../arxiv/`.

Run from the repository root with your usual AWS requester-pays credentials:

```sh
cargo run --locked --bin arxiv-duckdb -- ./arxiv-source.lance src/arXiv_src_2601_001.tar latex
cargo run --locked --bin arxiv-duckdb -- ./arxiv-pdf.lance pdf/arXiv_pdf_2601_001.tar pdf
cargo run --locked --bin arxiv-duckdb -- ./custom.lance src/arXiv_src_2601_001.tar latex /path/to/arxiv.duckdb
```

Arguments: `DESTINATION SHARD [latex|pdf] [DATABASE]`. Destination is a new local
path or `file://` URL. Format defaults to `latex`; the database defaults to
`arxiv.duckdb` beside this guide. Conversion reads the complete shard.

`arxiv.rs` opens the existing database read-only using `DuckDbDatabase::open`
on a blocking worker, then supplies shared `ArxivMetadataDatabase` connections
to `with_metadata_database`. Local metadata access requires no extension
download or OAI request. Missing files/records and schema conflicts are errors;
there is no online fallback. The database needs `main.arxiv_metadata` with an
indexed, normalized `article_id` and typed `oai:header` / `oai:metadata` structs.
The output `metadata` struct follows the library's content conversion schema.

The full local snapshot is intentionally Git-ignored. A fresh clone must supply
its own completed snapshot at this path, or pass `DATABASE` explicitly.

## Local snapshot provenance

- Source: [Cornell/Kaggle arXiv snapshot, version 313](https://www.kaggle.com/datasets/Cornell-University/arxiv/versions/313).
- Records: 3,261,651 in each of `arxiv_metadata_raw` and `arxiv_metadata`.
- Also contains the `arxiv_oai_records` view and an article ID index.
- Size: 12,255,506,432 bytes.
- SHA-256: `0826e5ceda7a1bc3f9c7ac59f6b762ac2ad93d3c6f045fe93f09ede6ba6c0e72`.

This is a Cornell/Kaggle metadata snapshot, not a complete OAI harvest. It retains
the source JSON; OAI-only fields and deletion coverage unavailable in the source
are not reconstructed. Newer articles may require a newer snapshot.
