//! Common Crawl service discovery → CrawlAccessor → open-arrow-sink → Lance.

use std::{error::Error, num::NonZeroUsize};

use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, AccessorExt, EmissionPolicy,
    crawl::{CrawlAccessor, CrawlOptions},
    decompression::{DecompressionLayer, gzip::Gzip},
    file_format::warc::WarcArrowAccessor,
    into_stream,
    layers::ParallelReadLayer,
    service::commoncrawl::{
        CommonCrawlSegmentAccessor, CommonCrawlSegmentRequest, create_http_accessor,
    },
};
use open_arrow_sink::{
    CommitSummary, SinkExt, WriteOptions,
    lance::{LanceDatasetSink, Url},
};

const ORIGIN: &str = "https://data.commoncrawl.org";

pub async fn run(
    destination: Url,
    crawl: &str,
    segment: &str,
) -> Result<CommitSummary, Box<dyn Error>> {
    let storage = create_http_accessor(ORIGIN)?;
    let manifests = storage.clone().layer(DecompressionLayer::new(Gzip));
    let records = WarcArrowAccessor::new(storage.layer(DecompressionLayer::new(Gzip)));
    let options = CrawlOptions::default()
        .with_emission(EmissionPolicy::Unordered)
        .with_parallel_read(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
    let source = CommonCrawlSegmentAccessor::new(manifests, records).with_file_planning();
    let reader = CrawlAccessor::new(source, options)
        .open(CommonCrawlSegmentRequest::new(crawl, segment)?)
        .await?;
    let schema = reader.metadata().schema.clone();
    let batches = into_stream(reader).map_err(open_arrow_sink::Error::backend);
    let summary = LanceDatasetSink::default()
        .write_all(destination, schema, WriteOptions::default(), batches)
        .await?;
    Ok(summary)
}
