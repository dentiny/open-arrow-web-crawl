//! A native BHL shard accessor handles discovery and metadata; CrawlAccessor executes it.
use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, EmissionPolicy,
    crawl::{CrawlAccessor, CrawlOptions},
    into_stream,
    service::bhl::{BhlShardAccessor, BhlShardReadRequest, open_data_bucket_operator},
    storage::OpenDalAccessor,
};
use open_arrow_sink::{
    CommitSummary, SinkExt, WriteOptions,
    lance::{LanceDatasetSink, Url},
};
use std::{error::Error, path::PathBuf};

pub async fn run(
    destination: Url,
    prefix: &str,
    database: PathBuf,
) -> Result<CommitSummary, Box<dyn Error>> {
    let request = BhlShardReadRequest::new(prefix)?;
    let source = BhlShardAccessor::from_database(
        OpenDalAccessor::new(open_data_bucket_operator()?),
        database,
    )
    .await?;
    let reader = CrawlAccessor::new(
        source,
        CrawlOptions::default().with_emission(EmissionPolicy::Unordered),
    )
    .open(request)
    .await?;
    let schema = reader.metadata().schema.clone();
    let batches = into_stream(reader).map_err(open_arrow_sink::Error::backend);
    Ok(LanceDatasetSink::default()
        .write_all(destination, schema, WriteOptions::default(), batches)
        .await?)
}
