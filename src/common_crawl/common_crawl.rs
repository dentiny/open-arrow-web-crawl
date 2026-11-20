//! Resolve Common Crawl manifests and stream archived WARC records as Arrow batches.

use std::{error::Error, num::NonZeroUsize};

use open_arrow_access::{
    Accessor, Layer, ReadOptions, ReadRequest,
    dataset::manifest::{DetectedFormat, Mismatch, PathsManifest},
    decompression::AutoDecompressionLayer,
    file_format::warc::WarcArrowAccessor,
    storage::OpenDalAccessor,
};

const ORIGIN: &str = "https://data.commoncrawl.org";

pub async fn run(
    crawl: &str,
    segment: &str,
    max_files: NonZeroUsize,
    max_records: NonZeroUsize,
) -> Result<(), Box<dyn Error>> {
    let max_files = max_files.get();
    let max_records = max_records.get();
    // Public HTTPS access needs no S3 credentials. Bind storage to the origin,
    // because warc.paths.gz lists bucket-relative keys, not manifest-relative keys.
    opendal::install_default();
    let operator = opendal::Operator::new(opendal_service_http::Http::default().endpoint(ORIGIN))?;
    let storage = OpenDalAccessor::new(operator);
    let manifest_key = format!("crawl-data/{crawl}/warc.paths.gz");
    let mut manifest = PathsManifest::new(manifest_key.clone())
        .with_formats(&[DetectedFormat::Warc], Mismatch::Error);
    if segment != "all" {
        // There is no separate paths manifest per segment. The trailing slash
        // keeps similarly named segments out of this selection.
        manifest = manifest.with_prefix(format!("crawl-data/{crawl}/segments/{segment}/warc/"));
    }
    // PathsManifest decompresses the inventory itself; pass raw storage here.
    let files = manifest.resolve(&storage).await?;
    println!("Manifest: {ORIGIN}/{manifest_key}");
    println!(
        "Selected {} WARC files; reading at most {max_files}",
        files.objects.len()
    );

    // Archive decompression is separate from manifest decompression. Common Crawl
    // .warc.gz files contain concatenated gzip members, decoded by this layer.
    let warc = WarcArrowAccessor::new(AutoDecompressionLayer::new().layer(storage));
    let mut total = 0;
    'files: for object in files.objects.into_iter().take(max_files) {
        println!("Reading {ORIGIN}/{}", object.locator());
        let mut reader = warc
            .open(ReadRequest::new(object).with_options(ReadOptions {
                batch_size: NonZeroUsize::new(max_records.min(100)).unwrap(),
                ..ReadOptions::default()
            }))
            .await?;
        while let Some(batch) = reader.next().await? {
            // Slice the final batch so the total output respects the row limit.
            let batch = batch.slice(0, batch.num_rows().min(max_records - total));
            if total == 0 {
                println!("Arrow schema: {:?}", batch.schema());
            }
            total += batch.num_rows();
            // Replace this summary with an Arrow/Lance/Parquet sink as needed.
            println!(
                "Batch: {} records, {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            if total == max_records {
                break 'files;
            }
        }
    }
    println!("Read {total} physical WARC records");
    // Each row preserves WARC headers and binary body bytes. Response bodies
    // can still contain HTTP headers and HTML; requests/metadata/revisits also
    // produce rows. This does not extract text, filter pages, or deduplicate.
    Ok(())
}
