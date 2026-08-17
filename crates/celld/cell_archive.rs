// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Supported cell export and offline import commands.
//!
//! Export is a point-in-time view of the newest durable LTX epoch and does not
//! claim the cell. Import creates a brand-new lineage only. Its staging marker
//! makes upgraded nodes fail closed while LTX is being built; the offline gate
//! is still mandatory because older nodes do not understand that marker.

use crate::bucket::Bucket;
use crate::fleet;
use crate::ltx_repl::LtxRepl;
use crate::ownership_store::{now_ms, BucketOwnership, NodeLeaseWire};
use anyhow::{bail, Context};
use celld_logic::CasOutcome;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

const ARCHIVE_VERSION: u32 = 1;
const IMPORT_EPOCH: u64 = 1;
const IMPORT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);
const IMPORT_ATTEMPT_LEASE_MS: u64 = 31 * 60 * 1_000;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ImportPhase {
    Staging,
    Ready,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ImportMarker {
    version: u32,
    phase: ImportPhase,
    source_sha256: String,
    attempt_id: String,
    #[serde(default)]
    attempt_expires_ms: Option<u64>,
    #[serde(default)]
    durable_txid: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ExportManifest<'a> {
    version: u32,
    cell: &'a str,
    source_epoch: u64,
    source_txid: u64,
    database_sha256: String,
}

#[derive(Debug)]
struct StorageOptions {
    bucket: String,
    endpoint: Option<String>,
    region: String,
}

#[derive(Debug)]
enum Command {
    Export {
        cell: String,
        output: PathBuf,
        storage: StorageOptions,
    },
    Import {
        cell: String,
        input: PathBuf,
        storage: StorageOptions,
        offline: bool,
        resume: bool,
    },
    Help,
}

pub async fn ensure_import_ready(bucket: &Bucket, cell: &str) -> anyhow::Result<()> {
    let key = format!("cells/{cell}/import.json");
    let Some((bytes, _)) = bucket.get(&key).await? else {
        return Ok(());
    };
    let marker: ImportMarker = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode import marker for {cell}"))?;
    validate_marker(&marker, cell)?;
    anyhow::ensure!(
        matches!(marker.phase, ImportPhase::Ready),
        "cell {cell} has an incomplete offline import; resume the import before activation"
    );
    Ok(())
}

fn validate_marker(marker: &ImportMarker, cell: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        marker.version == ARCHIVE_VERSION,
        "cell {cell} has unsupported import marker version {}",
        marker.version
    );
    anyhow::ensure!(
        marker.source_sha256.len() == 64
            && marker
                .source_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        "cell {cell} import marker has an invalid source SHA-256"
    );
    anyhow::ensure!(
        marker.attempt_id.len() == 32
            && marker
                .attempt_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        "cell {cell} import marker has an invalid attempt ID"
    );
    match marker.phase {
        ImportPhase::Staging => {
            anyhow::ensure!(
                marker.durable_txid.is_none() && marker.attempt_expires_ms.is_some(),
                "cell {cell} staging import marker has invalid attempt state"
            );
        }
        ImportPhase::Ready => {
            anyhow::ensure!(
                marker.durable_txid.is_some_and(|txid| txid > 0)
                    && marker.attempt_expires_ms.is_none(),
                "cell {cell} ready import marker has invalid durable state"
            );
        }
    }
    Ok(())
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    match parse(arguments)? {
        Command::Help => {
            print_help();
            Ok(())
        }
        Command::Export {
            cell,
            output,
            storage,
        } => export(&cell, &output, &storage).await,
        Command::Import {
            cell,
            input,
            storage,
            offline,
            resume,
        } => import(&cell, &input, &storage, offline, resume).await,
    }
}

fn parse(arguments: Vec<String>) -> anyhow::Result<Command> {
    if arguments.is_empty() || matches!(arguments[0].as_str(), "help" | "-h" | "--help") {
        return Ok(Command::Help);
    }
    let operation = arguments[0].clone();
    if !matches!(operation.as_str(), "export" | "import") {
        bail!("unknown cell command {operation:?}; run `celld cell --help` for usage");
    }
    let cell = arguments
        .get(1)
        .filter(|value| !value.starts_with('-'))
        .cloned()
        .context("cell export/import requires CELL")?;
    anyhow::ensure!(
        celld_logic::cell::valid_cell_scope(&cell),
        "invalid cell scope {cell:?}"
    );

    let mut bucket = None;
    let mut endpoint = None;
    let mut region = None;
    let mut input = None;
    let mut output = None;
    let mut offline = false;
    let mut resume = false;
    let mut args = arguments.into_iter().skip(2);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--bucket" => bucket = Some(next_value(&mut args, "--bucket")?),
            "--endpoint" => endpoint = Some(next_value(&mut args, "--endpoint")?),
            "--region" => region = Some(next_value(&mut args, "--region")?),
            "--input" => input = Some(PathBuf::from(next_value(&mut args, "--input")?)),
            "--output" => output = Some(PathBuf::from(next_value(&mut args, "--output")?)),
            "--offline" => offline = true,
            "--resume" => resume = true,
            other => bail!("unknown cell {operation} option {other:?}"),
        }
    }
    let env = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };
    let storage = StorageOptions {
        bucket: bucket
            .or_else(|| env("CELLD_BUCKET"))
            .context("cell export/import requires --bucket or CELLD_BUCKET")?,
        endpoint: endpoint.or_else(|| env("S3_ENDPOINT")),
        region: region
            .or_else(|| env("AWS_REGION"))
            .or_else(|| env("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|| "us-east-1".to_string()),
    };
    match operation.as_str() {
        "export" => Ok(Command::Export {
            cell,
            output: output.context("cell export requires --output DATABASE")?,
            storage,
        }),
        "import" => Ok(Command::Import {
            cell,
            input: input.context("cell import requires --input DATABASE")?,
            storage,
            offline,
            resume,
        }),
        _ => unreachable!(),
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, option: &str) -> anyhow::Result<String> {
    args.next()
        .with_context(|| format!("{option} requires a value"))
}

fn print_help() {
    println!(
        r#"celld cell — portable cell disaster recovery

USAGE:
  celld cell export CELL --output DATABASE --bucket [s3://|gs://]NAME[/PREFIX]
  celld cell import CELL --input DATABASE --bucket [s3://|gs://]NAME[/PREFIX] --offline [--resume]

Export reads the newest durable LTX epoch without claiming the cell and writes
DATABASE plus DATABASE.manifest.json. Import creates only a brand-new cell. It
requires every node from an older release to be stopped, rejects live node
leases, and is crash-resumable with the same input plus --resume after the
staging attempt's bounded lease expires."#
    );
}

fn open_bucket(storage: &StorageOptions) -> anyhow::Result<Bucket> {
    fleet::bucket_client(
        &storage.bucket,
        storage.endpoint.as_deref(),
        &storage.region,
    )
}

fn start_replication(
    bucket: &Bucket,
    storage: &StorageOptions,
    watch: &Path,
) -> anyhow::Result<LtxRepl> {
    LtxRepl::start(
        watch,
        bucket.backend(),
        bucket.name.clone(),
        bucket.prefix.clone(),
        storage.endpoint.clone(),
        storage.region.clone(),
        None,
    )
}

async fn export(cell: &str, output: &Path, storage: &StorageOptions) -> anyhow::Result<()> {
    anyhow::ensure!(
        !output.exists(),
        "refusing to overwrite {}",
        output.display()
    );
    let manifest_path = manifest_path(output);
    anyhow::ensure!(
        !manifest_path.exists(),
        "refusing to overwrite {}",
        manifest_path.display()
    );
    let bucket = open_bucket(storage)?;
    fleet::validate_bucket(&bucket).await?;
    let work = tempfile::tempdir()?;
    let replication = start_replication(&bucket, storage, work.path())?;
    let snapshot = replication
        .restore_snapshot(cell)
        .await?
        .with_context(|| format!("cell {cell} has no durable snapshot"))?;
    crate::replication::sqlite_snapshot(snapshot.path(), output)?;
    validate_sqlite(output)?;
    let manifest = ExportManifest {
        version: ARCHIVE_VERSION,
        cell,
        source_epoch: snapshot.epoch,
        source_txid: snapshot
            .txid
            .context("durable snapshot did not report its transaction")?,
        database_sha256: sha256_file(output)?,
    };
    write_private_new(&manifest_path, &serde_json::to_vec_pretty(&manifest)?)?;
    println!(
        "exported {cell} epoch {} to {}",
        snapshot.epoch,
        output.display()
    );
    println!("manifest {}", manifest_path.display());
    Ok(())
}

async fn import(
    cell: &str,
    input: &Path,
    storage: &StorageOptions,
    offline: bool,
    resume: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(offline, "cell import requires --offline acknowledgement");
    anyhow::ensure!(
        input.is_file(),
        "SQLite archive does not exist: {}",
        input.display()
    );
    // Hash and replicate one private backup, not the caller's main database
    // file. The backup API includes its committed WAL and prevents a writer
    // from changing the bytes between the import identity and LTX capture.
    let normalized_dir = tempfile::tempdir()?;
    let normalized = normalized_dir.path().join("database.sqlite");
    crate::replication::sqlite_snapshot(input, &normalized)
        .context("create consistent import archive")?;
    validate_sqlite(&normalized)?;
    let source_sha256 = sha256_file(&normalized)?;
    let bucket = open_bucket(storage)?;
    fleet::validate_bucket(&bucket).await?;
    ensure_no_live_nodes(&bucket).await?;

    let marker_key = format!("cells/{cell}/import.json");
    let owner_key = format!("cells/{cell}/own.json");
    let existing_marker = bucket.get(&marker_key).await?;
    let (claimed_marker, marker_token) = match existing_marker {
        Some((bytes, token)) => {
            let marker: ImportMarker = serde_json::from_slice(&bytes)?;
            validate_marker(&marker, cell)?;
            anyhow::ensure!(
                marker.version == ARCHIVE_VERSION && marker.source_sha256 == source_sha256,
                "cell {cell} has an import from a different archive; refusing to replace it"
            );
            if matches!(marker.phase, ImportPhase::Ready) {
                println!("cell {cell} import is already ready");
                return Ok(());
            }
            anyhow::ensure!(
                resume,
                "cell {cell} has an incomplete import; retry with --resume after its attempt lease expires"
            );
            anyhow::ensure!(
                marker
                    .attempt_expires_ms
                    .is_some_and(|expires| expires <= now_ms()),
                "cell {cell} import attempt {} is still active",
                marker.attempt_id
            );
            let claimed = staging_marker(source_sha256.clone());
            let token = bucket
                .put_cas(&marker_key, serde_json::to_vec(&claimed)?, Some(&token))
                .await?
                .with_context(|| format!("another process resumed cell {cell} first"))?;
            (claimed, token)
        }
        None => {
            anyhow::ensure!(!resume, "cell {cell} has no incomplete import to resume");
            anyhow::ensure!(
                bucket.get(&owner_key).await?.is_none(),
                "cell {cell} already has an owner lineage"
            );
            anyhow::ensure!(
                !bucket.list_any(&format!("cells/{cell}/ltx/")).await?,
                "cell {cell} already has replicated history"
            );
            let marker = staging_marker(source_sha256.clone());
            let token = bucket
                .put_cas(&marker_key, serde_json::to_vec(&marker)?, None)
                .await?
                .with_context(|| format!("another import raced for cell {cell}"))?;
            (marker, token)
        }
    };

    ensure_no_live_nodes(&bucket).await?;
    let owner = BucketOwnership::new(
        bucket.clone(),
        bucket.clone(),
        "cell-import".into(),
        String::new(),
    );
    if let Some((bytes, _)) = bucket.get(&owner_key).await? {
        #[derive(Deserialize)]
        struct Owner {
            node: String,
            epoch: u64,
        }
        let owner: Owner = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            owner.node.is_empty() && owner.epoch == IMPORT_EPOCH,
            "cell {cell} became owned during import"
        );
    } else {
        let work = tempfile::tempdir()?;
        let replication = start_replication(&bucket, storage, work.path())?;
        let txid = tokio::time::timeout(
            IMPORT_ATTEMPT_TIMEOUT,
            replication.seed_import_epoch(cell, IMPORT_EPOCH, &normalized),
        )
        .await
        .with_context(|| format!("cell {cell} import exceeded its 30-minute attempt limit"))??;
        ensure_no_live_nodes(&bucket).await?;
        anyhow::ensure!(
            claimed_marker
                .attempt_expires_ms
                .is_some_and(|expires| expires > now_ms()),
            "cell {cell} import attempt lease expired before publication"
        );
        let publication_token = bucket
            .put_cas(
                &marker_key,
                serde_json::to_vec(&claimed_marker)?,
                Some(&marker_token),
            )
            .await?
            .with_context(|| format!("cell {cell} import attempt lost its staging claim"))?;
        anyhow::ensure!(
            owner.create_import_owner(cell, IMPORT_EPOCH).await? == CasOutcome::Applied,
            "cell {cell} acquired an owner lineage while import was staging"
        );
        let ready = ImportMarker {
            version: ARCHIVE_VERSION,
            phase: ImportPhase::Ready,
            source_sha256,
            attempt_id: claimed_marker.attempt_id,
            attempt_expires_ms: None,
            durable_txid: Some(txid),
        };
        anyhow::ensure!(
            bucket
                .put_cas(
                    &marker_key,
                    serde_json::to_vec(&ready)?,
                    Some(&publication_token),
                )
                .await?
                .is_some(),
            "import marker changed while publishing cell {cell}"
        );
        println!("imported {cell} at epoch {IMPORT_EPOCH}, durable txid {txid}");
        return Ok(());
    }

    let txid = {
        let work = tempfile::tempdir()?;
        let replication = start_replication(&bucket, storage, work.path())?;
        replication.epoch_max_txid(cell, IMPORT_EPOCH).await?
    };
    anyhow::ensure!(
        txid.is_some(),
        "cell {cell} owner exists but imported LTX cannot be restored"
    );
    let ready = ImportMarker {
        version: ARCHIVE_VERSION,
        phase: ImportPhase::Ready,
        source_sha256,
        attempt_id: claimed_marker.attempt_id,
        attempt_expires_ms: None,
        durable_txid: txid,
    };
    anyhow::ensure!(
        bucket
            .put_cas(
                &marker_key,
                serde_json::to_vec(&ready)?,
                Some(&marker_token),
            )
            .await?
            .is_some(),
        "import marker changed while resuming cell {cell}"
    );
    println!("resumed import for {cell}");
    Ok(())
}

fn staging_marker(source_sha256: String) -> ImportMarker {
    ImportMarker {
        version: ARCHIVE_VERSION,
        phase: ImportPhase::Staging,
        source_sha256,
        attempt_id: format!("{:032x}", rand::random::<u128>()),
        attempt_expires_ms: Some(now_ms().saturating_add(IMPORT_ATTEMPT_LEASE_MS)),
        durable_txid: None,
    }
}

async fn ensure_no_live_nodes(bucket: &Bucket) -> anyhow::Result<()> {
    let mut live = Vec::new();
    for object in bucket.list("nodes/").await? {
        let key = object.location.as_ref();
        let Some((bytes, _)) = bucket.get(key).await? else {
            continue;
        };
        let lease: NodeLeaseWire =
            serde_json::from_slice(&bytes).with_context(|| format!("decode node lease {key}"))?;
        if lease.expires_ms > now_ms() {
            live.push(lease.node);
        }
    }
    live.sort();
    anyhow::ensure!(
        live.is_empty(),
        "offline import refused: live celld node lease(s): {}",
        live.join(", ")
    );
    Ok(())
}

fn validate_sqlite(path: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        path.is_file(),
        "SQLite archive does not exist: {}",
        path.display()
    );
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    anyhow::ensure!(
        integrity == "ok",
        "SQLite integrity check failed: {integrity}"
    );
    Ok(())
}

fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let bytes = std::fs::read(path)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn manifest_path(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push(".manifest.json");
    PathBuf::from(path)
}

fn write_private_new(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_import_only_with_explicit_paths() {
        let command = parse(vec![
            "import".into(),
            "Org:test".into(),
            "--input".into(),
            "archive.sqlite".into(),
            "--bucket".into(),
            "bucket/fleet".into(),
            "--offline".into(),
            "--resume".into(),
        ])
        .unwrap();
        assert!(matches!(
            command,
            Command::Import {
                offline: true,
                resume: true,
                ..
            }
        ));
    }

    #[test]
    fn rejects_invalid_cell_before_storage_access() {
        let error = parse(vec![
            "export".into(),
            "../escape".into(),
            "--output".into(),
            "archive.sqlite".into(),
            "--bucket".into(),
            "bucket".into(),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("invalid cell scope"));
    }

    #[test]
    fn validates_and_hashes_sqlite() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("archive.sqlite");
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE values_ (value TEXT); INSERT INTO values_ VALUES ('ok');")
            .unwrap();
        drop(connection);
        validate_sqlite(&path).unwrap();
        assert_eq!(sha256_file(&path).unwrap().len(), 64);
    }

    #[test]
    fn import_markers_fail_closed_until_a_durable_transaction_is_ready() {
        let hash = "a".repeat(64);
        let staging = ImportMarker {
            version: ARCHIVE_VERSION,
            phase: ImportPhase::Staging,
            source_sha256: hash.clone(),
            attempt_id: "1".repeat(32),
            attempt_expires_ms: Some(1),
            durable_txid: None,
        };
        validate_marker(&staging, "Knowledge:test").unwrap();
        let incomplete = ImportMarker {
            version: ARCHIVE_VERSION,
            phase: ImportPhase::Ready,
            source_sha256: hash.clone(),
            attempt_id: "1".repeat(32),
            attempt_expires_ms: None,
            durable_txid: None,
        };
        assert!(validate_marker(&incomplete, "Knowledge:test").is_err());
        let ready = ImportMarker {
            version: ARCHIVE_VERSION,
            phase: ImportPhase::Ready,
            source_sha256: hash,
            attempt_id: "1".repeat(32),
            attempt_expires_ms: None,
            durable_txid: Some(1),
        };
        validate_marker(&ready, "Knowledge:test").unwrap();
    }
}
