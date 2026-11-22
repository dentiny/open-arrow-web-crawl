use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, EmissionPolicy, ListRequest,
    crawl::{CrawlAccessor, CrawlOptions},
    into_stream,
    service::{
        FileSetRequest,
        bhl::{
            BhlOpenData, BhlPageAccessor, BhlPageIdentity, metadata::BhlMetadata,
            open_data_bucket_operator,
        },
    },
    storage::OpenDalAccessor,
};
use open_arrow_sink::{
    CommitSummary, SinkExt, WriteOptions,
    lance::{LanceDatasetSink, Url},
};
use std::{error::Error, path::PathBuf, sync::Arc};

pub async fn run(
    destination: Url,
    prefix: &str,
    database: PathBuf,
) -> Result<CommitSummary, Box<dyn Error>> {
    let group = prefix
        .strip_prefix("ocr/")
        .and_then(|p| p.strip_suffix('/'))
        .ok_or("prefix must select one OCR item or part directory")?;
    let id = group
        .strip_prefix("item-")
        .or_else(|| group.strip_prefix("part-"))
        .ok_or("prefix must select an item or part")?;
    if id.is_empty()
        || !id.bytes().all(|b| b.is_ascii_digit())
        || !id.parse::<u64>().is_ok_and(|n| n > 0)
    {
        return Err("prefix must contain a positive item or part ID".into());
    }
    let storage = OpenDalAccessor::new(open_data_bucket_operator()?);
    let aggregate = format!("{prefix}{group}.txt");
    let mut keys = Vec::new();
    for object in BhlOpenData::new(storage.clone())
        .discover(ListRequest::new(prefix))
        .await?
    {
        if object.locator() == aggregate {
            continue;
        }
        BhlPageIdentity::parse(object.locator())?;
        keys.push(object);
    }
    if keys.is_empty() {
        return Err("BHL directory contains no pages".into());
    }
    // Import has already completed. All readers open the same file read-only.
    let metadata =
        Arc::new(tokio::task::spawn_blocking(move || BhlMetadata::open(database)).await??);
    let source = BhlPageAccessor::from_metadata(
        storage,
        metadata.info.fingerprint.clone(),
        metadata,
        |page| {
            if page.missing.is_empty() && page.conflicts.is_empty() {
                return Ok(());
            }
            Err(open_arrow_access::Error::new(
                open_arrow_access::ErrorKind::InvalidInput,
                "bhl_example",
                format!(
                    "{}: missing {:?}; conflicts {:?}",
                    page.object.locator(),
                    page.missing,
                    page.conflicts
                ),
            ))
        },
    )?;
    let reader = CrawlAccessor::new(
        source,
        CrawlOptions::default().with_emission(EmissionPolicy::Unordered),
    )
    .open(FileSetRequest::new(keys))
    .await?;
    let schema = reader.metadata().schema.clone();
    let batches = into_stream(reader).map_err(open_arrow_sink::Error::backend);
    Ok(LanceDatasetSink::default()
        .write_all(destination, schema, WriteOptions::default(), batches)
        .await?)
}
