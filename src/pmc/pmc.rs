//! PMC metadata → CrawlAccessor → article content download → Lance.

use std::{error::Error, num::NonZeroUsize};

use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, EmissionPolicy, ReadRequest,
    crawl::{CrawlAccessor, CrawlOptions},
    into_stream,
    layers::{CoalesceBatchesLayer, ParallelReadLayer},
    service::pmc::{
        BUCKET_URL, PmcDatasetAccessor, PmcInventoryShardAccessor, PmcInventoryShardRequest,
        bucket_operator,
    },
    storage::OpenDalAccessor,
};
use open_arrow_sink::{
    CommitSummary, DownloadFailurePolicy, SinkExt, WriteOptions,
    lance::{LanceDatasetSink, Url},
    layer::{DownloadBlobLayer, FieldRemap, SchemaEvolutionLayer},
    storage::Storage,
};

pub async fn run(
    destination: Url,
    source: &str,
    manifest: Option<&str>,
) -> Result<CommitSummary, Box<dyn Error>> {
    let operator = bucket_operator()?;
    let storage = OpenDalAccessor::new(operator.clone());
    let options = CrawlOptions::default()
        .with_emission(EmissionPolicy::Unordered)
        .with_parallel_read(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .with_coalesce(CoalesceBatchesLayer::rows(NonZeroUsize::new(64).unwrap()));
    let reader = if let Some(manifest) = manifest {
        CrawlAccessor::new(PmcInventoryShardAccessor::new(storage), options)
            .open(PmcInventoryShardRequest::new(manifest, source))
            .await?
    } else {
        CrawlAccessor::new(PmcDatasetAccessor::new(storage), options)
            .open(ReadRequest::new(source))
            .await?
    };
    let schema = reader.metadata().schema.clone();
    let content = schema
        .field_with_name("content_url")?
        .clone()
        .with_name("content");
    let batches = into_stream(reader).map_err(open_arrow_sink::Error::backend);
    let sink = LanceDatasetSink::default()
        .with_source_storage(Storage::new(operator, Url::parse(BUCKET_URL)?)?)
        .layer(
            DownloadBlobLayer::new(["content".to_owned()].into())
                .with_failure_policy(DownloadFailurePolicy::SkipRow),
        )
        .layer(SchemaEvolutionLayer::new(vec![FieldRemap::new(
            "content_url",
            content,
            Ok,
        )]));
    let summary = sink
        .write_all(destination, schema, WriteOptions::default(), batches)
        .await?;
    Ok(summary)
}
