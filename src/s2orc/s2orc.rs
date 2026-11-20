//! Semantic Scholar S2ORC v2 → CrawlAccessor → Lance.

use std::{error::Error, num::NonZeroUsize};

use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, AccessorExt, EmissionPolicy, ReadRequest,
    crawl::{CrawlAccessor, CrawlOptions},
    decompression::{DecompressionLayer, gzip::Gzip},
    into_stream,
    layers::ParallelReadLayer,
    service::semantic_scholar::{S2orcV2Accessor, SemanticScholarService},
};
use open_arrow_sink::{
    CommitSummary, SinkExt, WriteOptions,
    lance::{LanceDatasetSink, Url},
};

pub async fn run(
    destination: Url,
    service: SemanticScholarService,
    release: &str,
    file_name: &str,
) -> Result<CommitSummary, Box<dyn Error>> {
    let files = service.dataset(release, "s2orc_v2").await?;
    let bytes = files.layer(DecompressionLayer::new(Gzip));
    let source = S2orcV2Accessor::new(bytes);
    let options = CrawlOptions::default()
        .with_emission(EmissionPolicy::Unordered)
        .with_parallel_read(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
    let reader = CrawlAccessor::new(source, options)
        .open(ReadRequest::new(file_name))
        .await?;
    let schema = reader.metadata().schema.clone();
    let batches = into_stream(reader).map_err(open_arrow_sink::Error::backend);
    let summary = LanceDatasetSink::default()
        .write_all(destination, schema, WriteOptions::default(), batches)
        .await?;
    Ok(summary)
}
