//! cargo run --bin pmc -- ./pmc.lance metadata/PMC10009416.1.json

mod pmc;

use open_arrow_sink::lance::Url;
use std::{env, error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    let destination = args.first().map(String::as_str).unwrap_or("./pmc.lance");
    let destination = if destination.starts_with("file://") {
        Url::parse(destination)?
    } else {
        let url = Url::from_directory_path(std::path::absolute(destination)?).unwrap();
        Url::parse(url.as_str())?
    };
    let source = args
        .get(1)
        .map(String::as_str)
        .unwrap_or("metadata/PMC10009416.1.json");
    let manifest = args.get(2).map(String::as_str);
    pmc::run(destination, source, manifest).await?;
    Ok(())
}
