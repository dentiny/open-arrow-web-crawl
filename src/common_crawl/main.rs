//! Convert archived Common Crawl captures to a Lance dataset, following ml-infra's
//! lance_migration WARC provider. This reads existing captures, not live websites.
//!
//! cargo run --bin common_crawl -- ./common-crawl.lance CC-MAIN-2026-43 1791170053823.21
//! Arguments: destination path/file URL, crawl ID, segment ID.
//! Defaults: ./common-crawl.lance, the crawl/segment above.

mod common_crawl;

use open_arrow_sink::lance::Url;
use std::{env, error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    let destination = args
        .first()
        .map(String::as_str)
        .unwrap_or("./common-crawl.lance");
    let destination = if destination.starts_with("file://") {
        Url::parse(destination)?
    } else {
        let url = Url::from_directory_path(std::path::absolute(destination)?).unwrap();
        // Normalize parent components before passing the URL to storage/Lance.
        Url::parse(url.as_str())?
    };
    let crawl = args.get(1).map(String::as_str).unwrap_or("CC-MAIN-2026-43");
    let segment = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("1791170053823.21");
    common_crawl::run(destination, crawl, segment).await?;
    Ok(())
}
