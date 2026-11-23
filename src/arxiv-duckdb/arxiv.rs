use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, EmissionPolicy,
    crawl::{CrawlAccessor, CrawlOptions},
    into_stream,
    service::arxiv::{
        ArxivContentFormat, ArxivKind, ArxivMetadataDatabase, ArxivReadRequest, ArxivService,
    },
    service::duckdb::DuckDbDatabase,
    storage::{OpenDalAccessor, opendal},
};
use open_arrow_sink::{
    CommitSummary, SinkExt, WriteOptions,
    lance::{LanceDatasetSink, Url},
};
use std::{error::Error, path::PathBuf, sync::Arc};

pub async fn run(
    destination: Url,
    shard: &str,
    format: &str,
    database: PathBuf,
) -> Result<CommitSummary, Box<dyn Error>> {
    let (kind, format) = match format {
        "latex" => (ArxivKind::Source, ArxivContentFormat::Latex),
        "pdf" => (ArxivKind::Pdf, ArxivContentFormat::Pdf),
        _ => return Err("format must be latex or pdf".into()),
    };
    // Open the completed local snapshot read-only, off the async runtime.
    // The explicit database backend never falls back to OAI.
    let metadata = tokio::task::spawn_blocking(move || {
        DuckDbDatabase::open(database).map(ArxivMetadataDatabase::new)
    })
    .await??;
    opendal::install_default();
    // Use the caller's AWS configuration, including requester-pays signing.
    let operator = opendal::Operator::new(
        opendal::services::S3::default()
            .bucket("arxiv")
            .region("us-east-1")
            .enable_request_payer(),
    )?;
    let source = ArxivService::new(OpenDalAccessor::new(operator))
        .records()
        .content(format)
        .with_metadata_database(Arc::new(metadata));
    let reader = CrawlAccessor::new(
        source,
        CrawlOptions::default().with_emission(EmissionPolicy::Unordered),
    )
    .open(ArxivReadRequest::new(kind).with_shard(shard))
    .await?;
    let schema = reader.metadata().schema.clone();
    let batches = into_stream(reader).map_err(open_arrow_sink::Error::backend);
    Ok(LanceDatasetSink::default()
        .write_all(destination, schema, WriteOptions::default(), batches)
        .await?)
}
