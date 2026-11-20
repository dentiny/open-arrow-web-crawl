//! Read archived Common Crawl captures as Arrow batches, following ml-infra's
//! lance_migration WARC provider. This reads existing captures, not live websites.
//!
//! cargo run --example common_crawl -- CC-MAIN-2026-43 1791170053823.21 1 100
//! Arguments: crawl ID, segment ID (or `all`), max files, max records total.
//! Defaults: the crawl/segment above, one file, 100 records. Limits are positive.

use std::{env, error::Error, io, num::NonZeroUsize};

use open_arrow_access::{
    Accessor, Layer, ReadOptions, ReadRequest,
    dataset::manifest::{DetectedFormat, Mismatch, PathsManifest},
    decompression::AutoDecompressionLayer,
    file_format::warc::WarcArrowAccessor,
    storage::OpenDalAccessor,
};

const ORIGIN: &str = "https://data.commoncrawl.org";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.len() > 4 || args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: cargo run --example common_crawl -- [CRAWL] [SEGMENT|all] [MAX_FILES] [MAX_RECORDS]"
        );
        if args.len() > 4 {
            return Err(io::Error::other("too many arguments").into());
        }
        return Ok(());
    }
    let crawl = args
        .first()
        .map(String::as_str)
        .unwrap_or("CC-MAIN-2026-43");
    let segment = args
        .get(1)
        .map(String::as_str)
        .unwrap_or("1791170053823.21");
    let max_files = limit(args.get(2), 1)?;
    let max_records = limit(args.get(3), 100)?;
    // IDs are single path components, not URLs or arbitrary prefixes.
    if !crawl.starts_with("CC-MAIN-") || !valid_id(crawl) || !valid_id(segment) {
        return Err(
            io::Error::other("expected a CC-MAIN crawl ID and a segment ID (or all)").into(),
        );
    }

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

fn limit(value: Option<&String>, default: usize) -> Result<usize, Box<dyn Error>> {
    Ok(match value {
        Some(value) => value.parse::<NonZeroUsize>()?.get(),
        None => default,
    })
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.')
        && value != "."
        && value != ".."
}
