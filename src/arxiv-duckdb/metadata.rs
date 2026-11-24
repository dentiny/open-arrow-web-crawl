//! Reconstruct the complete snapshot from the Git LFS-tracked gzip parts.
use flate2::read::GzDecoder;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, Read, Write},
    path::PathBuf,
};

#[derive(Deserialize)]
struct Manifest {
    bytes: u64,
    sha256: String,
    parts: Vec<Part>,
}

#[derive(Deserialize)]
struct Part {
    name: String,
    bytes: u64,
    sha256: String,
}

pub fn prepare_default() -> io::Result<PathBuf> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/arxiv-duckdb");
    let destination = directory.join("arxiv.duckdb");
    if destination.exists() {
        return Ok(destination);
    }
    let manifest: Manifest = serde_json::from_str(include_str!("metadata-manifest.json"))?;
    let mut compressed: Box<dyn Read> = Box::new(io::empty());
    for part in manifest.parts {
        let path = directory.join(&part.name);
        let mut file = File::open(&path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "{}: {error}; install Git LFS and run `git lfs pull`",
                    path.display()
                ),
            )
        })?;
        if file.metadata()?.len() != part.bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} is not the full snapshot part; install Git LFS and run `git lfs pull`",
                    path.display()
                ),
            ));
        }
        eprintln!("Verifying {}", part.name);
        copy_checked(&mut file, io::sink(), part.bytes, &part.sha256)?;
        compressed = Box::new(compressed.chain(File::open(path)?));
    }
    eprintln!("Unpacking and verifying arxiv.duckdb...");
    let mut database = tempfile::NamedTempFile::new_in(&directory)?;
    copy_checked(
        GzDecoder::new(compressed),
        database.as_file_mut(),
        manifest.bytes,
        &manifest.sha256,
    )?;
    database.as_file().sync_all()?;
    database
        .persist_noclobber(&destination)
        .map_err(io::Error::other)?;
    eprintln!("Metadata ready: {}", destination.display());
    Ok(destination)
}

// Bound output size, verify all bytes, and leave installation to the caller only
// after the digest matches. Temporary files are removed on every failure.
fn copy_checked(
    input: impl Read,
    mut output: impl Write,
    bytes: u64,
    expected: &str,
) -> io::Result<()> {
    let mut input = input.take(bytes + 1);
    let mut digest = Sha256::new();
    let mut size = 0;
    let mut buffer = [0; 64 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
        digest.update(&buffer[..read]);
        size += read as u64;
    }
    if size != bytes || hex(digest.finalize()) != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata snapshot size or SHA-256 mismatch",
        ));
    }
    Ok(())
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
