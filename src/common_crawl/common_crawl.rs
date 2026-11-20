//! Common Crawl service discovery → CrawlAccessor → open-arrow-sink → Lance.

use std::{error::Error, num::NonZeroUsize};

use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, AccessorExt, EmissionPolicy, ReadOptions,
    crawl::{CrawlAccessor, CrawlOptions},
    decompression::{DecompressionLayer, gzip::Gzip},
    file_format::warc::WarcArrowAccessor,
    into_stream,
    layers::{LimitLayer, ParallelReadLayer},
    service::{
        commoncrawl::{
            CommonCrawlSegmentAccessor, CommonCrawlSegmentRequest, create_http_accessor,
        },
        warc::{CommonCrawlWarcAccessor, CommonCrawlWarcRequest},
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
    max_files: NonZeroUsize,
    max_records: NonZeroUsize,
) -> Result<CommitSummary, Box<dyn Error>> {
    println!("Lance destination: {destination}");
    let storage = create_http_accessor(ORIGIN)?;
    // Service accessors expect decoded inventory bytes. Archive decoding is
    // configured separately on the WARC format, using the same bound storage.
    let manifests = storage.clone().layer(DecompressionLayer::new(Gzip));
    let records = WarcArrowAccessor::new(storage.layer(DecompressionLayer::new(Gzip)));
    let mut files = if segment == "all" {
        CommonCrawlWarcAccessor::new(manifests, records.clone())
            .resolve(CommonCrawlWarcRequest::new(crawl)?)
            .await?
    } else {
        CommonCrawlSegmentAccessor::new(manifests, records.clone())
            .resolve(CommonCrawlSegmentRequest::new(crawl, segment)?)
            .await?
    };
    println!("Selected {} WARC files", files.objects.len());
    // Limit explicit membership before execution; the service still resolves
    // the full inventory. File limits and scan-wide record limits are distinct.
    files.objects.truncate(max_files.get());
    files = files.with_options(ReadOptions {
        batch_size: NonZeroUsize::new(max_records.get().min(100)).unwrap(),
        ..ReadOptions::default()
    });
    let crawl = CrawlAccessor::files(
        records,
        CrawlOptions::default()
            .with_emission(EmissionPolicy::Ordered)
            .with_parallel_read(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            )),
    );
    let reader = crawl
        .layer(LimitLayer::new(max_records.get().try_into()?))
        .open(files)
        .await?;
    let schema = reader.metadata().schema.clone();
    // LimitLayer is outside the crawl, so its budget covers the merged scan.
    // Stream directly to the sink; it pulls batches with backpressure.
    let batches = into_stream(reader).map_err(open_arrow_sink::Error::backend);
    // write_all opens once, streams batches and commits at EOF. Read/write
    // failures trigger best-effort abort. Default Create mode rejects an
    // occupied destination; this example never overwrites an existing dataset.
    let summary = LanceDatasetSink::default()
        .write_all(destination, schema, WriteOptions::default(), batches)
        .await?;
    println!("Committed {} WARC records to Lance", summary.rows_committed);
    // These are physical WARC records with binary bodies, not cleaned web text.
    Ok(summary)
}
