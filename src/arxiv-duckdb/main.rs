//! One official bulk shard + local DuckDB metadata → typed article content → Lance.
mod arxiv;
mod metadata;

use open_arrow_sink::lance::Url;
use std::{env, error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if !(2..=4).contains(&args.len()) {
        return Err("usage: arxiv-duckdb DESTINATION SHARD [latex|pdf] [DATABASE]".into());
    }
    if args
        .get(2)
        .is_some_and(|format| format != "latex" && format != "pdf")
    {
        return Err("format must be latex or pdf".into());
    }
    let destination = if args[0].starts_with("file://") {
        Url::parse(&args[0])?
    } else {
        Url::from_directory_path(std::path::absolute(&args[0])?)
            .map_err(|_| "invalid destination")?
    };
    let database = match args.get(3) {
        Some(path) => std::path::PathBuf::from(path),
        None => tokio::task::spawn_blocking(metadata::prepare_default).await??,
    };
    arxiv::run(
        destination,
        &args[1],
        args.get(2).map(String::as_str).unwrap_or("latex"),
        database,
    )
    .await?;
    Ok(())
}
