//! Convert archived Common Crawl captures to a Lance dataset, following ml-infra's
//! lance_migration WARC provider. This reads existing captures, not live websites.
//!
//! cargo run --bin common_crawl -- ./common-crawl.lance CC-MAIN-2026-43 1791170053823.21
//! Arguments: destination path/file URL, crawl ID, segment ID (or `all`).
//! Defaults: ./common-crawl.lance, the crawl/segment above.

mod common_crawl;

use open_arrow_sink::lance::Url;
use std::{
    env,
    error::Error,
    io::{self, Write},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.len() > 3 || args.iter().any(|arg| arg == "--help" || arg == "-h") {
        io::stdout().write_all(
            b"Usage: cargo run --bin common_crawl -- [DESTINATION] [CRAWL] [SEGMENT|all]\n",
        )?;
        if args.len() > 3 {
            return Err(io::Error::other("too many arguments").into());
        }
        return Ok(());
    }
    let destination = args
        .first()
        .map(String::as_str)
        .unwrap_or("./common-crawl.lance");
    let destination = if destination.starts_with("file://") {
        Url::parse(destination)?
    } else {
        let url = Url::from_directory_path(std::path::absolute(destination)?)
            .map_err(|_| io::Error::other("invalid destination path"))?;
        // Normalize parent components before passing the URL to storage/Lance.
        Url::parse(url.as_str())?
    };
    let crawl = args.get(1).map(String::as_str).unwrap_or("CC-MAIN-2026-43");
    let segment = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("1791170053823.21");
    // IDs are single path components, not URLs or arbitrary prefixes.
    if !crawl.starts_with("CC-MAIN-") || !valid_id(crawl) || !valid_id(segment) {
        return Err(
            io::Error::other("expected a CC-MAIN crawl ID and a segment ID (or all)").into(),
        );
    }

    common_crawl::run(destination, crawl, segment).await?;
    Ok(())
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.')
        && value != "."
        && value != ".."
}
