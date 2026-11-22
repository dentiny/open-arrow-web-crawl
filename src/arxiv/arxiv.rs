use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, EmissionPolicy,
    crawl::{CrawlAccessor, CrawlOptions},
    into_stream,
    service::arxiv::{ArxivContentFormat, ArxivKind, ArxivReadRequest, ArxivService},
    storage::{OpenDalAccessor, opendal},
};
use open_arrow_sink::{
    CommitSummary, SinkExt, WriteOptions,
    lance::{LanceDatasetSink, Url},
};
use std::error::Error;

pub async fn run(
    destination: Url,
    shard: &str,
    format: &str,
) -> Result<CommitSummary, Box<dyn Error>> {
    let (kind, format) = match format {
        "latex" => (ArxivKind::Source, ArxivContentFormat::Latex),
        "pdf" => (ArxivKind::Pdf, ArxivContentFormat::Pdf),
        _ => return Err("format must be latex or pdf".into()),
    };
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
        .content(format);
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
