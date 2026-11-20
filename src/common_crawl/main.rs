//! Read archived Common Crawl captures as Arrow batches, following ml-infra's
//! lance_migration WARC provider. This reads existing captures, not live websites.
//!
//! cargo run --bin common_crawl -- CC-MAIN-2026-43 1791170053823.21 1 100
//! Arguments: crawl ID, segment ID (or `all`), max files, max records total.
//! Defaults: the crawl/segment above, one file, 100 records. Limits are positive.

mod common_crawl;

use std::{env, error::Error, io, num::NonZeroUsize};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.len() > 4 || args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: cargo run --bin common_crawl -- [CRAWL] [SEGMENT|all] [MAX_FILES] [MAX_RECORDS]"
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

    common_crawl::run(crawl, segment, max_files, max_records).await
}

fn limit(value: Option<&String>, default: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    Ok(match value {
        Some(value) => value.parse::<NonZeroUsize>()?,
        None => NonZeroUsize::new(default).unwrap(),
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
