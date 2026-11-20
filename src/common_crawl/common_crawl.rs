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
    // Service accessors expect decoded inventory bytes. Archive decoding is
    // configured separately on the WARC format, using the same bound storage.
    let manifests = storage.clone().layer(DecompressionLayer::new(Gzip));
    let records = WarcArrowAccessor::new(storage.layer(DecompressionLayer::new(Gzip)));
    let options = CrawlOptions::default()
        .with_emission(EmissionPolicy::Ordered)
        .with_parallel_read(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
    let source = CommonCrawlSegmentAccessor::new(manifests, records).with_file_planning();
    let reader = CrawlAccessor::new(source, options)
        .open(CommonCrawlSegmentRequest::new(crawl, segment)?)
        .await?;
    let schema = reader.metadata().schema.clone();
    // Stream directly to the sink; it pulls batches with backpressure.
    let batches = into_stream(reader).map_err(open_arrow_sink::Error::backend);
    // write_all opens once, streams batches and commits at EOF. Read/write
    // failures trigger best-effort abort. Default Create mode rejects an
    // occupied destination; this example never overwrites an existing dataset.
    let summary = LanceDatasetSink::default()
        .write_all(destination, schema, WriteOptions::default(), batches)
        .await?;
    // These are physical WARC records with binary bodies, not cleaned web text.
    Ok(summary)
}
