//! Create metadata once, then convert one complete OCR item/part directory.
mod bhl;

use open_arrow_access::service::bhl::{metadata::BhlMetadata, open_data_bucket_operator};
use open_arrow_access::storage::OpenDalAccessor;
use open_arrow_sink::lance::Url;
use std::{env, error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("create-metadata") && args.len() == 2 {
        BhlMetadata::create(OpenDalAccessor::new(open_data_bucket_operator()?), &args[1]).await?;
        return Ok(());
    }
    if args.len() != 3 {
        return Err("usage: bhl create-metadata DATABASE | bhl DESTINATION PREFIX DATABASE".into());
    }
    let destination = if args[0].starts_with("file://") {
        Url::parse(&args[0])?
    } else {
        Url::from_directory_path(std::path::absolute(&args[0])?)
            .map_err(|_| "invalid destination")?
    };
    bhl::run(destination, &args[1], std::path::PathBuf::from(&args[2])).await?;
    Ok(())
}
