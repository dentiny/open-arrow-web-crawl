//! S2_API_KEY=... cargo run --bin s2orc -- ./s2orc.lance latest part-00000

mod s2orc;

use open_arrow_access::service::semantic_scholar::SemanticScholarService;
use open_arrow_sink::lance::Url;
use std::{env, error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    let destination = args.first().map(String::as_str).unwrap_or("./s2orc.lance");
    let destination = if destination.starts_with("file://") {
        Url::parse(destination)?
    } else {
        let url = Url::from_directory_path(std::path::absolute(destination)?).unwrap();
        Url::parse(url.as_str())?
    };
    let release = args.get(1).map(String::as_str).unwrap_or("latest");
    let file_name = args.get(2).map(String::as_str).unwrap_or("part-00000");
    let service = SemanticScholarService::new(&env::var("S2_API_KEY")?)?;
    s2orc::run(destination, service, release, file_name).await?;
    Ok(())
}
