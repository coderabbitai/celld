// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! In-process replication backend built on `celld-ltx`.
//!
//! One shared `object_store` client for the whole node, and a managed
//! `celld_ltx::Db` per resident cell that captures the cell's committed WAL
//! and uploads it on demand. No external process, no directory-watch lag — a
//! just-written cell is registered the instant it activates, so the output
//! gate can prove a fresh cell durable with no cold-start window.
//!
//! The object layout is `cells/<cell>/ltx/e<epoch>/` in the bucket, mirroring
//! the local `<watch>/<cell>/ltx/e<epoch>/db.sqlite` tree. This backend builds
//! its own object-store clients rather than going through `bucket::Bucket`, so
//! it carries the fleet's key prefix itself: without that, two fleets sharing
//! one bucket would replicate over each other.

use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use anyhow::Context;
use celld_ltx::object_store::ObjectStore;
use celld_ltx::replica;
use celld_ltx::replica_compactor::ReplicaCompactor;
use celld_ltx::Db;
use celld_ltx::ObjectStoreClient;
use celld_ltx::ObjectStoreConfig;
use celld_ltx::Replica;
use celld_ltx::ReplicaObjectCodec;
use celld_ltx::TXID;
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tracing::info;
use tracing::warn;

use crate::replication::prune_watch;
use crate::replication::sqlite_snapshot;
use crate::replication::ActivationOptions;
use crate::replication::ActivationResult;
use crate::replication::RestoredSnapshot;
use crate::replication::StorageCredentials;
use crate::replication::SyncWait;

/// Max cells uploading concurrently across the node. Caps blocking-pool threads
/// and in-flight object-store requests under high write fan-out.
const SYNC_CONCURRENCY: usize = 64;

/// Max LTX object downloads across every restore on this node. A hot cell can
/// contain thousands of L0 files, so serial reads turn a takeover into minutes
/// of terminal failures. This shared ceiling hides round-trip latency without
/// multiplying the bound by the activation count.
const RESTORE_DOWNLOAD_CONCURRENCY: usize = 64;

/// One attempt consumes at most this many source objects. This bound keeps a
/// first compaction of an old, write-hot cell from reading its complete L0
/// history into memory.
const COMPACTION_MAX_FILES: usize = 256;

/// The current `ReplicaClient` interface buffers objects, so bound the complete
/// input set until the client gains a streaming read and write surface.
const COMPACTION_MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;

const FORK_SEED_FORMAT: &str = "celld-sqlite-fork-seed-v1";
const DATABASE_OBJECT_NAME: &str = "database.sqlite";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ForkSeedManifest {
    pub format: String,
    pub checkpoint_id: String,
    pub source_cell: String,
    pub source_epoch: u64,
    pub sqlite_sha256: String,
    pub sqlite_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct CompactionConfig {
    min_txids: u64,
    concurrency: usize,
}

struct CellCompaction {
    cell: String,
    epoch: u64,
    client: ObjectStoreClient,
    local_path: PathBuf,
    queue: mpsc::UnboundedSender<CompactionWork>,
    min_txids: u64,
    compacted_txid: AtomicU64,
    queued: AtomicBool,
    cancelled: AtomicBool,
    cancel: Notify,
}

struct CompactionWork {
    cell: Weak<Cell>,
    queued_at: Instant,
}

/// One resident cell's replication state: the `celld_ltx::Db` shadowing its WAL
/// (behind a `std::sync::Mutex` because the `rusqlite` handle is `!Sync` and
/// must never cross an `.await`, so every capture+upload runs inside a
/// `spawn_blocking` closure) plus the durability tickets the output gate waits
/// on. `req_seq` counts durability requests; `synced_seq` is the highest ticket
/// a completed background sync captured. A write waits for `synced_seq >= its
/// ticket`, so concurrent writes to one cell ride a single batched upload —
/// and, because a sync credits only tickets whose writes committed before it
/// started (which the sync's `db.sync` captures), never one it did not upload.
struct Cell {
    replica: Mutex<Replica<ObjectStoreClient>>,
    req_seq: AtomicU64,
    synced_seq: AtomicU64,
    durable_txid: AtomicU64,
    /// Set while a sync for this cell is in flight, so the loop never runs two
    /// at once for one cell (they would serialize on the mutex and waste work).
    syncing: AtomicBool,
    /// Notified when `synced_seq` advances (or a sync fails), waking waiters.
    ready: Notify,
    compaction: Option<CellCompaction>,
}
type CellHandle = Arc<Cell>;

pub struct LtxRepl {
    /// Local root: cell dbs live at `watch/<cell>/ltx/e<epoch>/db.sqlite`.
    watch: PathBuf,
    bucket: String,
    /// The bucket spec's key prefix: empty, or slash-terminated.
    prefix: String,
    endpoint: Option<String>,
    region: String,
    credentials: Option<StorageCredentials>,
    /// One connection pool for the whole node, shared by every cell client.
    store: Arc<dyn ObjectStore>,
    /// Encodes only customer database bytes. Ownership, epoch seals, and other
    /// coordination metadata deliberately bypass it so bucket CAS remains
    /// independently operable.
    durability_codec: Arc<dyn ReplicaObjectCodec>,
    cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>>,
    /// Woken when a cell's `committed` advances, so the background loop syncs
    /// without polling; a slow tick backstops any missed notification.
    dirty: Arc<Notify>,
    /// Shared by every activation, so the restore bound is per node, not per
    /// cell. The generic LTX restore keeps its sequential compatibility path.
    restore_slots: Arc<Semaphore>,
    compaction_queue: Option<mpsc::UnboundedSender<CompactionWork>>,
    compaction_min_txids: u64,
}

fn snapshot_active_at(
    watch: &Path,
    source: &Path,
    cell: &str,
    epoch: u64,
) -> anyhow::Result<Option<RestoredSnapshot>> {
    if !source.is_file() {
        return Ok(None);
    }
    let directory = watch.join(format!(
        ".inspect-{cell}-e{epoch}-{:032x}",
        rand::random::<u128>()
    ));
    std::fs::create_dir_all(&directory)?;
    let path = directory.join("db.sqlite");
    if let Err(error) = sqlite_snapshot(source, &path) {
        let _ = std::fs::remove_dir_all(&directory);
        return Err(error);
    }
    Ok(Some(RestoredSnapshot::new(epoch, None, path, directory)))
}

impl LtxRepl {
    /// Private-corpus constructor over an injected store, so the epoch-seal
    /// protocol runs against an in-memory bucket instead of S3.
    #[cfg(test)]
    pub fn start_with_store_for_test(watch: &Path, store: Arc<dyn ObjectStore>) -> Self {
        Self::start_with_store_and_codec_for_test(
            watch,
            store,
            Arc::new(celld_ltx::PlaintextReplicaObjectCodec),
        )
    }

    #[cfg(test)]
    fn start_with_store_and_codec_for_test(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        durability_codec: Arc<dyn ReplicaObjectCodec>,
    ) -> Self {
        let cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>> = Arc::default();
        let dirty = Arc::new(Notify::new());
        let slots = Arc::new(Semaphore::new(SYNC_CONCURRENCY));
        tokio::spawn(sync_loop(cells.clone(), dirty.clone(), slots));
        Self {
            watch: watch.to_path_buf(),
            bucket: "test".into(),
            prefix: String::new(),
            endpoint: None,
            region: "auto".into(),
            credentials: None,
            store,
            durability_codec,
            cells,
            dirty,
            restore_slots: Arc::new(Semaphore::new(RESTORE_DOWNLOAD_CONCURRENCY)),
            compaction_queue: None,
            compaction_min_txids: 0,
        }
    }

    /// Private-corpus constructor with additive L1 compaction enabled.
    #[cfg(all(test, celld_internal_tests))]
    pub fn start_with_compaction_for_test(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        min_txids: u64,
        concurrency: usize,
    ) -> Self {
        let cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>> = Arc::default();
        let dirty = Arc::new(Notify::new());
        let sync_slots = Arc::new(Semaphore::new(SYNC_CONCURRENCY));
        tokio::spawn(sync_loop(cells.clone(), dirty.clone(), sync_slots));
        let config = CompactionConfig {
            min_txids,
            concurrency,
        };
        let queue = start_compaction_loop(config);
        Self {
            watch: watch.to_path_buf(),
            bucket: "test".into(),
            prefix: String::new(),
            endpoint: None,
            region: "auto".into(),
            credentials: None,
            store,
            durability_codec: Arc::new(celld_ltx::PlaintextReplicaObjectCodec),
            cells,
            dirty,
            restore_slots: Arc::new(Semaphore::new(RESTORE_DOWNLOAD_CONCURRENCY)),
            compaction_queue: Some(queue),
            compaction_min_txids: min_txids,
        }
    }

    pub(crate) fn start(
        watch: &Path,
        backend: crate::bucket::StorageBackend,
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: String,
        credentials: Option<StorageCredentials>,
    ) -> anyhow::Result<Self> {
        let compaction = compaction_config_from_env()?;
        let durability_codec = crate::durability_encryption::codec_from_env()?;
        // Everything downstream of the store is backend-agnostic already,
        // so the dialect decides construction and nothing else.
        let store = match backend {
            crate::bucket::StorageBackend::Gcs => crate::bucket::gcs_replica_store(&bucket)?,
            crate::bucket::StorageBackend::S3 => {
                node_config(&bucket, endpoint.as_deref(), &region, credentials.as_ref())
                    .build_store()
                    .map_err(|error| anyhow!("build shared object store: {error}"))?
            }
        };
        let cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>> = Arc::default();
        let dirty = Arc::new(Notify::new());
        // Bound how many cells upload at once so one slow cell cannot stall the
        // others and a thousand hot cells cannot open a thousand uploads.
        let slots = Arc::new(Semaphore::new(SYNC_CONCURRENCY));
        tokio::spawn(sync_loop(cells.clone(), dirty.clone(), slots));
        let compaction_queue = compaction.map(start_compaction_loop);
        Ok(Self {
            watch: watch.to_path_buf(),
            bucket,
            prefix,
            endpoint,
            region,
            credentials,
            store,
            durability_codec,
            cells,
            dirty,
            restore_slots: Arc::new(Semaphore::new(RESTORE_DOWNLOAD_CONCURRENCY)),
            compaction_queue,
            compaction_min_txids: compaction.map_or(0, |config| config.min_txids),
        })
    }

    fn db_path(&self, cell: &str, epoch: u64) -> PathBuf {
        self.watch
            .join(cell)
            .join("ltx")
            .join(format!("e{epoch}"))
            .join("db.sqlite")
    }

    /// A per-cell client over the shared store, keyed to the cell's epoch
    /// prefix. `cells/<cell>/ltx/e<epoch>` matches [`Self::db_path`]'s remote
    /// twin so the same coordinates address local and replica state.
    fn client_for(&self, cell: &str, epoch: u64) -> ObjectStoreClient {
        let mut config = node_config(
            &self.bucket,
            self.endpoint.as_deref(),
            &self.region,
            self.credentials.as_ref(),
        );
        config.path = format!("{}cells/{cell}/ltx/e{epoch}", self.prefix);
        ObjectStoreClient::with_store_and_codec(
            config,
            self.store.clone(),
            self.durability_codec.clone(),
        )
    }

    /// Highest epoch under `cells/<cell>/ltx/` that holds any LTX — the newest
    /// durable copy to restore on takeover.
    async fn highest_nonempty_epoch(&self, cell: &str) -> anyhow::Result<Option<u64>> {
        use celld_ltx::object_store::path::Path as ObjPath;
        let base = ObjPath::from(format!("{}cells/{cell}/ltx", self.prefix));
        let listing = self.store.list_with_delimiter(Some(&base)).await?;
        let mut best: Option<u64> = None;
        for prefix in listing.common_prefixes {
            if let Some(epoch) = prefix
                .filename()
                .and_then(|name| name.strip_prefix('e'))
                .and_then(|value| value.parse::<u64>().ok())
            {
                best = Some(best.map_or(epoch, |current| current.max(epoch)));
            }
        }
        Ok(best)
    }

    /// The seal for a restore-source epoch: a file *beside* the epoch
    /// directories (`cells/<cell>/ltx/e<from>.seal.json`), so nothing that
    /// lists an epoch's LTX contents parses it, and `highest_nonempty_epoch`'s
    /// prefix listing never mistakes it for an epoch.
    fn seal_key(&self, cell: &str, epoch: u64) -> String {
        format!("{}cells/{cell}/ltx/e{epoch}.seal.json", self.prefix)
    }

    fn fork_seed_key(&self, cell: &str, name: &str) -> String {
        format!("{}cells/{cell}/fork-seed/{name}", self.prefix)
    }

    fn checkpoint_key(&self, cell: &str, checkpoint: &str, name: &str) -> String {
        format!(
            "{}cells/{cell}/checkpoints/{checkpoint}/{name}",
            self.prefix
        )
    }

    async fn put_fork_seed_object(
        &self,
        cell: &str,
        name: &str,
        bytes: Vec<u8>,
    ) -> anyhow::Result<()> {
        use celld_ltx::object_store::path::Path as ObjPath;
        use celld_ltx::object_store::{PutMode, PutOptions, PutPayload};

        let key = ObjPath::from(self.fork_seed_key(cell, name));
        let stored = if name == DATABASE_OBJECT_NAME {
            self.durability_codec
                .encode(key.as_ref(), &bytes)
                .map_err(|error| anyhow!("encrypt fork seed {cell}: {error}"))?
        } else {
            bytes.clone()
        };
        let create = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&key, PutPayload::from(stored), create)
            .await
        {
            Ok(_) => Ok(()),
            Err(celld_ltx::object_store::Error::AlreadyExists { .. }) => {
                let existing = self.store.get(&key).await?.bytes().await?;
                let existing = if name == DATABASE_OBJECT_NAME {
                    self.durability_codec
                        .decode(key.as_ref(), &existing)
                        .map_err(|error| anyhow!("decrypt existing fork seed {cell}: {error}"))?
                } else {
                    existing.to_vec()
                };
                anyhow::ensure!(
                    existing == bytes,
                    "fork seed target {cell} already contains a different {name}"
                );
                Ok(())
            }
            Err(error) => Err(anyhow!("publish fork seed {cell}/{name}: {error}")),
        }
    }

    /// Publish a content-verified immutable checkpoint of the active cell.
    pub async fn publish_checkpoint(
        &self,
        source_cell: &str,
        source_epoch: u64,
        checkpoint_id: &str,
    ) -> anyhow::Result<ForkSeedManifest> {
        anyhow::ensure!(
            celld_logic::cell::valid_cell_scope(source_cell)
                && celld_logic::cell::valid_cell_scope(checkpoint_id),
            "invalid checkpoint coordinate"
        );
        let source = self.db_path(source_cell, source_epoch);
        let watch = self.watch.clone();
        let cell = source_cell.to_string();
        let sqlite = tokio::task::spawn_blocking(move || -> anyhow::Result<Option<Vec<u8>>> {
            let Some(snapshot) = snapshot_active_at(&watch, &source, &cell, source_epoch)? else {
                return Ok(None);
            };
            Ok(Some(std::fs::read(snapshot.path())?))
        })
        .await??
        .ok_or_else(|| anyhow!("fork source is not active on this node"))?;
        let manifest = ForkSeedManifest {
            format: FORK_SEED_FORMAT.to_string(),
            checkpoint_id: checkpoint_id.to_string(),
            source_cell: source_cell.to_string(),
            source_epoch,
            sqlite_sha256: format!("{:x}", Sha256::digest(&sqlite)),
            sqlite_bytes: sqlite.len() as u64,
        };
        let encoded_manifest = serde_json::to_vec(&manifest)?;
        self.put_checkpoint_object(source_cell, checkpoint_id, DATABASE_OBJECT_NAME, sqlite)
            .await?;
        self.put_checkpoint_object(
            source_cell,
            checkpoint_id,
            "manifest.json",
            encoded_manifest,
        )
        .await?;
        Ok(manifest)
    }

    async fn put_checkpoint_object(
        &self,
        cell: &str,
        checkpoint: &str,
        name: &str,
        bytes: Vec<u8>,
    ) -> anyhow::Result<()> {
        use celld_ltx::object_store::path::Path as ObjPath;
        use celld_ltx::object_store::{PutMode, PutOptions, PutPayload};

        let key = ObjPath::from(self.checkpoint_key(cell, checkpoint, name));
        let stored = if name == DATABASE_OBJECT_NAME {
            self.durability_codec
                .encode(key.as_ref(), &bytes)
                .map_err(|error| anyhow!("encrypt checkpoint {cell}/{checkpoint}: {error}"))?
        } else {
            bytes.clone()
        };
        let create = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&key, PutPayload::from(stored), create)
            .await
        {
            Ok(_) => Ok(()),
            Err(celld_ltx::object_store::Error::AlreadyExists { .. }) => {
                let existing = self.store.get(&key).await?.bytes().await?;
                let existing = if name == DATABASE_OBJECT_NAME {
                    self.durability_codec
                        .decode(key.as_ref(), &existing)
                        .map_err(|error| {
                            anyhow!("decrypt existing checkpoint {cell}/{checkpoint}: {error}")
                        })?
                } else {
                    existing.to_vec()
                };
                anyhow::ensure!(
                    existing == bytes,
                    "checkpoint {cell}/{checkpoint} already contains a different {name}"
                );
                Ok(())
            }
            Err(error) => Err(anyhow!(
                "publish checkpoint {cell}/{checkpoint}/{name}: {error}"
            )),
        }
    }

    async fn read_checkpoint(
        &self,
        source_cell: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<(ForkSeedManifest, Vec<u8>)> {
        use celld_ltx::object_store::path::Path as ObjPath;

        let manifest_key =
            ObjPath::from(self.checkpoint_key(source_cell, checkpoint_id, "manifest.json"));
        let manifest: ForkSeedManifest =
            serde_json::from_slice(&self.store.get(&manifest_key).await?.bytes().await?)?;
        anyhow::ensure!(
            manifest.format == FORK_SEED_FORMAT,
            "unsupported checkpoint format"
        );
        anyhow::ensure!(
            manifest.source_cell == source_cell && manifest.checkpoint_id == checkpoint_id,
            "checkpoint coordinates do not match its manifest"
        );
        let database_key =
            ObjPath::from(self.checkpoint_key(source_cell, checkpoint_id, DATABASE_OBJECT_NAME));
        let encoded = self.store.get(&database_key).await?.bytes().await?;
        let sqlite = self
            .durability_codec
            .decode(database_key.as_ref(), &encoded)
            .map_err(|error| {
                anyhow!("decrypt checkpoint {source_cell}/{checkpoint_id}: {error}")
            })?;
        anyhow::ensure!(
            sqlite.len() as u64 == manifest.sqlite_bytes,
            "checkpoint byte count mismatch"
        );
        anyhow::ensure!(
            format!("{:x}", Sha256::digest(&sqlite)) == manifest.sqlite_sha256,
            "checkpoint hash mismatch"
        );
        Ok((manifest, sqlite))
    }

    /// Seed a never-before-activated target from one immutable checkpoint.
    /// The ready manifest is last so first activation fails closed if copying
    /// is interrupted. Every write is create-or-verify for exact retry.
    pub async fn publish_fork_seed_from_checkpoint(
        &self,
        source_cell: &str,
        checkpoint_id: &str,
        target_cell: &str,
        target_active: bool,
    ) -> anyhow::Result<ForkSeedManifest> {
        use celld_ltx::object_store::path::Path as ObjPath;

        anyhow::ensure!(
            source_cell != target_cell,
            "fork source and target must differ"
        );
        anyhow::ensure!(
            celld_logic::cell::valid_cell_scope(source_cell)
                && celld_logic::cell::valid_cell_scope(checkpoint_id)
                && celld_logic::cell::valid_cell_scope(target_cell),
            "invalid fork coordinate"
        );
        let (manifest, sqlite) = self.read_checkpoint(source_cell, checkpoint_id).await?;
        let encoded_manifest = serde_json::to_vec(&manifest)?;
        let ready = ObjPath::from(self.fork_seed_key(target_cell, "ready.json"));
        let exact_retry = match self.store.get(&ready).await {
            Ok(result) => {
                let existing = result.bytes().await?;
                anyhow::ensure!(
                    existing.as_ref() == encoded_manifest,
                    "fork seed target {target_cell} already contains a different ready.json"
                );
                true
            }
            Err(celld_ltx::object_store::Error::NotFound { .. }) => false,
            Err(error) => return Err(anyhow!("read fork seed for {target_cell}: {error}")),
        };
        if exact_retry {
            self.put_fork_seed_object(target_cell, "reserved.json", encoded_manifest.clone())
                .await?;
            self.put_fork_seed_object(target_cell, DATABASE_OBJECT_NAME, sqlite)
                .await?;
            self.put_fork_seed_object(target_cell, "ready.json", encoded_manifest)
                .await?;
            return Ok(manifest);
        }
        anyhow::ensure!(
            !target_active,
            "fork target {target_cell} is already active"
        );
        anyhow::ensure!(
            self.highest_nonempty_epoch(target_cell).await?.is_none(),
            "fork target {target_cell} already has a durable replica"
        );
        self.put_fork_seed_object(target_cell, "reserved.json", encoded_manifest.clone())
            .await?;
        self.put_fork_seed_object(target_cell, DATABASE_OBJECT_NAME, sqlite)
            .await?;
        self.put_fork_seed_object(target_cell, "ready.json", encoded_manifest)
            .await?;
        Ok(manifest)
    }

    async fn restore_fork_seed(&self, cell: &str, destination: &Path) -> anyhow::Result<bool> {
        use celld_ltx::object_store::path::Path as ObjPath;

        let reservation = ObjPath::from(self.fork_seed_key(cell, "reserved.json"));
        match self.store.head(&reservation).await {
            Ok(_) => {}
            Err(celld_ltx::object_store::Error::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(anyhow!("read fork reservation for {cell}: {error}")),
        }
        let ready = ObjPath::from(self.fork_seed_key(cell, "ready.json"));
        let manifest: ForkSeedManifest = match self.store.get(&ready).await {
            Ok(result) => serde_json::from_slice(&result.bytes().await?)?,
            Err(celld_ltx::object_store::Error::NotFound { .. }) => {
                anyhow::bail!("fork seed for {cell} is reserved but incomplete")
            }
            Err(error) => return Err(anyhow!("read fork manifest for {cell}: {error}")),
        };
        anyhow::ensure!(
            manifest.format == FORK_SEED_FORMAT,
            "unsupported fork seed format"
        );
        let database = ObjPath::from(self.fork_seed_key(cell, DATABASE_OBJECT_NAME));
        let encoded = self.store.get(&database).await?.bytes().await?;
        let sqlite = self
            .durability_codec
            .decode(database.as_ref(), &encoded)
            .map_err(|error| anyhow!("decrypt fork seed for {cell}: {error}"))?;
        anyhow::ensure!(
            sqlite.len() as u64 == manifest.sqlite_bytes,
            "fork seed byte count mismatch"
        );
        anyhow::ensure!(
            format!("{:x}", Sha256::digest(&sqlite)) == manifest.sqlite_sha256,
            "fork seed hash mismatch"
        );
        let temporary = destination.with_extension("fork-seed.tmp");
        let destination = destination.to_path_buf();
        let expected_bytes = manifest.sqlite_bytes;
        let expected_sha256 = manifest.sqlite_sha256.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let _ = std::fs::remove_file(&temporary);
            std::fs::write(&temporary, &sqlite)?;
            // FTS5's integrity path may use SQLite's write machinery even though
            // quick_check is logically read-only. Validate a private temporary
            // copy with normal flags, then prove the main database bytes remain
            // identical to the signed manifest before activation.
            let connection = rusqlite::Connection::open(&temporary)?;
            let quick_check: String = connection
                .query_row("PRAGMA quick_check", [], |row| row.get(0))
                .map_err(|error| anyhow!("fork seed SQLite quick_check failed: {error}"))?;
            anyhow::ensure!(
                quick_check == "ok",
                "fork seed SQLite quick_check failed: {quick_check}"
            );
            drop(connection);
            let validated = std::fs::read(&temporary)?;
            anyhow::ensure!(
                validated.len() as u64 == expected_bytes
                    && format!("{:x}", Sha256::digest(&validated)) == expected_sha256,
                "fork seed SQLite validation changed the database bytes"
            );
            std::fs::rename(temporary, destination)?;
            Ok(())
        })
        .await??;
        Ok(true)
    }

    /// Read the source epoch's seal, writing it first if this activation is
    /// the first to restore from `from` (Cellarium §5.6).
    ///
    /// Restore's rule is "highest non-empty epoch", so if a takeover's new
    /// epoch stays empty (it dies before its first sync) while the fenced old
    /// owner keeps uploading into the old prefix — its PUTs are unconditional
    /// and the bucket checks nothing — the *next* activation restores the old prefix
    /// again, now including the zombie's tail: writes no client was ever
    /// acknowledged (the ownership re-read refuses the zombie's acks), and
    /// possibly writes a client was told had definitively failed, resurrect.
    ///
    /// The seal closes this by fixing, before the first restore reads a byte,
    /// the TXID every restore of this epoch may read through. Ordering makes
    /// it safe for acknowledged writes: an ack requires the ownership record
    /// to still name the writer, the takeover's record CAS precedes this seal,
    /// and the acked write's upload preceded its ownership read — so every
    /// acked write is at or below the sealed TXID. Conversely a fenced owner's
    /// later uploads land above the seal and are never restored again.
    /// First-writer-wins (conditional create) keeps every later restorer on
    /// the same cut. A seal-write failure fails the activation: restoring an
    /// unsealed prefix reopens the hole.
    async fn read_or_seal(
        &self,
        cell: &str,
        from: u64,
        by_epoch: u64,
        client: &ObjectStoreClient,
    ) -> anyhow::Result<u64> {
        use celld_ltx::object_store::path::Path as ObjPath;
        use celld_ltx::object_store::PutMode;
        use celld_ltx::object_store::PutOptions;
        use celld_ltx::object_store::PutPayload;

        #[derive(serde::Serialize, serde::Deserialize)]
        struct EpochSeal {
            max_txid: u64,
            sealed_by_epoch: u64,
        }

        let key = ObjPath::from(self.seal_key(cell, from));
        let existing = |bytes: Vec<u8>| -> anyhow::Result<u64> {
            let seal: EpochSeal = serde_json::from_slice(&bytes)?;
            Ok(seal.max_txid)
        };
        match self.store.get(&key).await {
            Ok(get) => return existing(get.bytes().await?.to_vec()),
            Err(celld_ltx::object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(anyhow!("read seal for {cell} e{from}: {error}")),
        }
        let plan = replica::calc_restore_plan(client, TXID(0))
            .await
            .map_err(|error| anyhow!("plan seal for {cell} e{from}: {error}"))?;
        let cap = plan
            .iter()
            .map(|info| info.max_txid.0)
            .max()
            .ok_or_else(|| anyhow!("seal target {cell} e{from} is empty"))?;
        let seal = EpochSeal {
            max_txid: cap,
            sealed_by_epoch: by_epoch,
        };
        let put = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&key, PutPayload::from(serde_json::to_vec(&seal)?), put)
            .await
        {
            Ok(_) => {
                info!(cell, from, to = by_epoch, cap, "sealed restore source");
                Ok(cap)
            }
            Err(celld_ltx::object_store::Error::AlreadyExists { .. }) => {
                // A racing claimant sealed first; its cut is the one every
                // restore must share.
                let get = self
                    .store
                    .get(&key)
                    .await
                    .map_err(|error| anyhow!("reread seal for {cell} e{from}: {error}"))?;
                existing(get.bytes().await?.to_vec())
            }
            Err(error) => Err(anyhow!("seal {cell} e{from}: {error}")),
        }
    }

    /// Does the bucket hold any LTX for this cell at this epoch? The fail-closed
    /// eviction gate: never delete the last local copy of state the bucket
    /// cannot restore.
    pub async fn epoch_replicated(&self, cell: &str, epoch: u64) -> bool {
        let client = self.client_for(cell, epoch);
        matches!(
            replica::calc_restore_plan(&client, TXID(0)).await,
            Ok(plan) if !plan.is_empty()
        )
    }

    pub async fn activate(
        &self,
        options: ActivationOptions<'_>,
    ) -> anyhow::Result<ActivationResult> {
        let ActivationOptions {
            cell,
            epoch,
            fresh,
            took_over,
            resume_local,
        } = options;
        let dst = self.db_path(cell, epoch);
        std::fs::create_dir_all(dst.parent().unwrap())?;

        // Reuse a preserved local eviction snapshot when it is safe to: the
        // same epoch always, the previous epoch only when we did not take the
        // cell from another node.
        // `.evicted` is the current name; `.hibernated` is what releases
        // before 2026-08-05 wrote. Accept both, so an upgrade reuses the
        // snapshots already on disk instead of restoring every cell from the
        // bucket. Writes always use the new name, so the old one dies out.
        let legacy = |path: &PathBuf| path.with_extension("hibernated");
        let same_epoch = dst.with_extension("evicted");
        let previous = celld_logic::restore::previous_epoch_reusable(epoch, took_over)
            .then(|| self.db_path(cell, epoch - 1).with_extension("evicted"));
        let is_file = |path: &PathBuf| path.is_file();
        let first_present = |path: PathBuf| {
            if path.is_file() {
                Some(path)
            } else {
                Some(legacy(&path)).filter(is_file)
            }
        };
        let local_snapshot = (!fresh && !resume_local)
            .then(|| first_present(same_epoch.clone()).or_else(|| previous.and_then(first_present)))
            .flatten();

        let mut restored = resume_local;
        if resume_local {
            anyhow::ensure!(
                dst.is_file(),
                "clean reload database is missing: {}",
                dst.display()
            );
            info!(cell, epoch, "resumed clean local replica");
        } else if let Some(snapshot) = local_snapshot {
            std::fs::rename(&snapshot, &dst)?;
            info!(cell, epoch, "reused local eviction snapshot");
            restored = true;
        } else if fresh && self.restore_fork_seed(cell, &dst).await? {
            info!(cell, epoch, "restored immutable fork seed");
            restored = true;
        } else if !fresh {
            // Restore the newest durable epoch into this epoch's path — up to
            // its seal, never past it. Sealing before reading fixes the cut
            // every later restore of this prefix shares, so a fenced owner's
            // late uploads into it can never resurrect (see `read_or_seal`).
            if let Some(from) = self.highest_nonempty_epoch(cell).await? {
                let client = self.client_for(cell, from);
                let cap = self.read_or_seal(cell, from, epoch, &client).await?;
                let _ = std::fs::remove_file(&dst);
                let stats = replica::restore_with_download_slots(
                    &client,
                    &dst,
                    TXID(cap),
                    self.restore_slots.clone(),
                )
                .await
                .map_err(|error| anyhow!("restore {cell} e{from}@{cap}: {error}"))?;
                let levels = stats
                    .by_level
                    .iter()
                    .map(|(level, count)| format!("L{level}:{count}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                info!(
                    event = "restore_plan",
                    cell,
                    epoch = from,
                    objects = stats.objects,
                    bytes = stats.bytes,
                    %levels,
                    "computed restore plan"
                );
                info!(cell, from, to = epoch, cap, "restored remote replica");
                restored = true;
            }
        }

        // Open the managed Db (creates a fresh WAL db when nothing was restored)
        // and pair it with this epoch's client. Registration is immediate: the
        // cell can be proved durable on its very first write. The just-opened
        // db's position is the replica's seed -- 0 for a fresh cell, the
        // restored max otherwise, and equal to the remote under epoch fencing --
        // so the first sync skips the `calc_pos` listing that otherwise storms a
        // rate-limiting store. On the rare decode error we leave it unseeded and
        // fall back to that listing.
        let dst_ = dst.clone();
        let (db, seed) = tokio::task::spawn_blocking(move || {
            let mut db = Db::open(&dst_)?;
            let seed = db.pos().ok();
            anyhow::Ok((db, seed))
        })
        .await?
        .map_err(|error| anyhow!("open managed db {}: {error}", dst.display()))?;
        let mut replica = Replica::new(db, self.client_for(cell, epoch));
        if let Some(pos) = seed {
            replica.seed_pos(pos);
        }
        let handle = Arc::new(Cell {
            replica: Mutex::new(replica),
            req_seq: AtomicU64::new(0),
            synced_seq: AtomicU64::new(0),
            durable_txid: AtomicU64::new(seed.map_or(0, |pos| pos.txid.0)),
            syncing: AtomicBool::new(false),
            ready: Notify::new(),
            compaction: self.compaction_queue.as_ref().map(|queue| CellCompaction {
                cell: cell.to_string(),
                epoch,
                client: self.client_for(cell, epoch),
                local_path: Db::meta_path_for_path(&dst),
                queue: queue.clone(),
                min_txids: self.compaction_min_txids,
                compacted_txid: AtomicU64::new(0),
                queued: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                cancel: Notify::new(),
            }),
        });
        self.cells
            .lock()
            .unwrap()
            .insert((cell.to_string(), epoch), handle.clone());
        if let Some(pos) = seed {
            maybe_queue_compaction(&handle, pos.txid.0);
        }

        Ok(ActivationResult {
            path: dst,
            restored,
        })
    }

    /// The output gate's primitive: take a durability ticket and return once a
    /// background sync that captured this write has completed, coalescing
    /// concurrent writes to one cell into a single upload. The write committed
    /// before this call, so any sync starting after our ticket captures it —
    /// we wait for `synced_seq >= my ticket`, not for a position, sidestepping
    /// the total_changes↔LTX-txid mismatch that a position compare would hit.
    /// Returns `position` (which the completed sync provably covered) for the
    /// core's coverage check.
    pub async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<u64> {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            anyhow::bail!("ltx cell not resident: {cell} epoch {epoch}");
        };
        let ticket = handle.req_seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.dirty.notify_one();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            // Register the waiter before checking, so a sync that completes
            // between the check and the await is not missed.
            let ready = handle.ready.notified();
            if handle.synced_seq.load(Ordering::SeqCst) >= ticket {
                return Ok(position);
            }
            if tokio::time::timeout_at(deadline, ready).await.is_err() {
                anyhow::bail!("ltx durability timed out for {cell} epoch {epoch}");
            }
        }
    }

    /// A direct, synchronous durability pass for the rare eviction
    /// gates (not the hot write path). Also advances the cell's durable position
    /// so any output-gate waiters ride it.
    pub async fn sync_wait(&self, cell: &str, epoch: u64, _timeout: Duration) -> SyncWait {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            return SyncWait::Unsupported;
        };
        match sync_cell(handle).await {
            Some(true) => SyncWait::Durable,
            Some(false) => SyncWait::Failed,
            None => SyncWait::Unsupported,
        }
    }

    pub async fn evict(&self, cell: &str, epoch: u64, preserve_local: bool) {
        // A final durability pass so no acknowledged write is stranded, then
        // drop the managed Db (releasing the WAL) before touching the file.
        let _ = self.sync_wait(cell, epoch, Duration::from_secs(10)).await;
        let removed = self
            .cells
            .lock()
            .unwrap()
            .remove(&(cell.to_string(), epoch));
        if let Some(handle) = removed {
            cancel_compaction(&handle);
        }
        let db = self.db_path(cell, epoch);
        if preserve_local {
            let preserved = db.with_extension("evicted");
            if let Err(error) = std::fs::rename(&db, &preserved) {
                warn!(cell, epoch, %error, "preserve local snapshot failed");
            }
        }
        // Clear the WAL/meta siblings and the live db regardless: a reactivation
        // restores or reuses the `.hibernated` copy.
        for suffix in ["-wal", "-shm"] {
            let mut sibling = db.clone().into_os_string();
            sibling.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(sibling));
        }
        let _ = std::fs::remove_dir_all(Db::meta_path_for_path(&db));
        if !preserve_local {
            let _ = std::fs::remove_file(&db);
        }
    }

    /// Copy the live epoch into a private read-only snapshot for inspection.
    pub fn snapshot_active(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<RestoredSnapshot>> {
        snapshot_active_at(&self.watch, &self.db_path(cell, epoch), cell, epoch)
    }

    /// Restore the newest durable replica into a private snapshot without
    /// claiming or activating the cell.
    pub async fn restore_snapshot(&self, cell: &str) -> anyhow::Result<Option<RestoredSnapshot>> {
        let Some(epoch) = self.highest_nonempty_epoch(cell).await? else {
            return Ok(None);
        };
        let directory = self.watch.join(format!(".restore-{cell}"));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory)?;
        let path = directory.join("db.sqlite");
        let stats = replica::restore_with_download_slots(
            &self.client_for(cell, epoch),
            &path,
            TXID(0),
            self.restore_slots.clone(),
        )
        .await
        .map_err(|error| anyhow!("restore snapshot {cell} e{epoch}: {error}"))?;
        Ok(Some(RestoredSnapshot::new(
            epoch,
            Some(stats.max_txid),
            path,
            directory,
        )))
    }

    /// Highest durable transaction in one epoch, or `None` when it has no
    /// restorable LTX. Used to finish a crash-interrupted import marker from
    /// the lineage that was already validated before its owner CAS.
    pub(crate) async fn epoch_max_txid(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<u64>> {
        let plan = match replica::calc_restore_plan(&self.client_for(cell, epoch), TXID(0)).await {
            Ok(plan) => plan,
            Err(celld_ltx::Error::TxNotAvailable) => return Ok(None),
            Err(error) => {
                return Err(anyhow!(error))
                    .with_context(|| format!("plan durable position for {cell} e{epoch}"));
            }
        };
        Ok(plan.iter().map(|info| info.max_txid.0).max())
    }

    /// Replace the private epoch used by an offline import, capture the input
    /// as LTX, and prove that the uploaded lineage restores cleanly.
    ///
    /// Authority policy lives in `cell_archive`: this primitive is called only
    /// while a CAS-created staging marker blocks activation and before an owner
    /// record exists. Clearing the epoch makes a retry after a process crash
    /// deterministic instead of appending to an unknown partial upload.
    pub(crate) async fn seed_import_epoch(
        &self,
        cell: &str,
        epoch: u64,
        source: &Path,
    ) -> anyhow::Result<u64> {
        use celld_ltx::object_store::path::Path as ObjPath;

        let remote_prefix = format!("{}cells/{cell}/ltx/e{epoch}", self.prefix);
        let remote = ObjPath::from(remote_prefix.clone());
        let mut listed = self.store.list(Some(&remote));
        while let Some(object) = futures_util::StreamExt::next(&mut listed).await {
            let object = object.context("list partial import epoch")?;
            self.store
                .delete(&object.location)
                .await
                .with_context(|| format!("clear partial import object {}", object.location))?;
        }

        let directory = self.watch.join(format!(".import-{cell}-e{epoch}"));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory)?;
        let path = directory.join("db.sqlite");
        sqlite_snapshot(source, &path).context("create consistent import snapshot")?;

        let client = self.client_for(cell, epoch);
        let path_for_capture = path.clone();
        let mut replica = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let mut db = Db::open(&path_for_capture).context("open import snapshot for LTX")?;
            db.sync().context("capture import snapshot as LTX")?;
            Ok(Replica::new(db, client))
        })
        .await??;
        replica.sync().await.context("upload import snapshot LTX")?;
        let txid = replica.pos().txid.0;
        anyhow::ensure!(txid > 0, "import produced no durable LTX transaction");
        drop(replica);

        let restored = directory.join("roundtrip.sqlite");
        replica::restore_with_download_slots(
            &self.client_for(cell, epoch),
            &restored,
            TXID(0),
            self.restore_slots.clone(),
        )
        .await
        .context("round-trip imported LTX")?;
        let connection = rusqlite::Connection::open_with_flags(
            &restored,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let integrity: String =
            connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        anyhow::ensure!(
            integrity == "ok",
            "import round-trip integrity check failed: {integrity}"
        );
        drop(connection);
        let expected = directory.join("expected.sqlite");
        let actual = directory.join("actual.sqlite");
        sqlite_snapshot(&path, &expected).context("normalize captured import for comparison")?;
        sqlite_snapshot(&restored, &actual).context("normalize restored import for comparison")?;
        anyhow::ensure!(
            files_equal(&expected, &actual)?,
            "import LTX round trip does not match the staged SQLite database"
        );
        let _ = std::fs::remove_dir_all(&directory);
        Ok(txid)
    }

    pub fn prune_local_cache(&self, max_bytes: u64) -> (usize, usize, u64) {
        prune_watch(&self.watch, max_bytes)
    }

    /// Close the replicator handle while retaining the live database and WAL
    /// exactly where the local path encodes them.
    pub fn close_for_reload(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        let removed = self
            .cells
            .lock()
            .unwrap()
            .remove(&(cell.to_string(), epoch));
        if let Some(handle) = removed {
            cancel_compaction(&handle);
        }
        let path = self.db_path(cell, epoch);
        anyhow::ensure!(
            path.is_file(),
            "resident database is missing: {}",
            path.display()
        );
        Ok(())
    }

    /// Enumerate live-named databases. Cached `.evicted` files are separate
    /// and remain under the ordinary cache byte limit.
    pub fn local_cells(&self) -> Vec<celld_logic::LocalCell> {
        let mut cells = Vec::new();
        let Ok(cell_dirs) = std::fs::read_dir(&self.watch) else {
            return cells;
        };
        for cell_dir in cell_dirs.flatten() {
            let Ok(kind) = cell_dir.file_type() else {
                continue;
            };
            if !kind.is_dir() {
                continue;
            }
            let Some(cell) = cell_dir.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(epochs) = std::fs::read_dir(cell_dir.path().join("ltx")) else {
                continue;
            };
            for epoch_dir in epochs.flatten() {
                let Some(epoch) = epoch_dir
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix('e'))
                    .and_then(|epoch| epoch.parse::<u64>().ok())
                else {
                    continue;
                };
                if epoch_dir.path().join("db.sqlite").is_file() {
                    cells.push(celld_logic::LocalCell {
                        id: cell.clone(),
                        epoch,
                    });
                }
            }
        }
        cells.sort();
        cells.dedup();
        cells
    }

    /// Delete stale live-named epochs after the runtime has identified and
    /// closed its exact resident set. Remote replicas remain authoritative.
    pub fn prune_stale_live(
        &self,
        keep: &std::collections::BTreeSet<(String, u64)>,
    ) -> anyhow::Result<usize> {
        let stale: Vec<_> = self
            .local_cells()
            .into_iter()
            .filter(|cell| !keep.contains(&(cell.id.clone(), cell.epoch)))
            .collect();
        for cell in &stale {
            if let Some(parent) = self.db_path(&cell.id, cell.epoch).parent() {
                std::fs::remove_dir_all(parent)?;
            }
        }
        let remaining: std::collections::BTreeSet<_> = self
            .local_cells()
            .into_iter()
            .map(|cell| (cell.id, cell.epoch))
            .collect();
        anyhow::ensure!(
            &remaining == keep,
            "clean reload inventory mismatch after pruning: expected {}, found {}",
            keep.len(),
            remaining.len()
        );
        Ok(stale.len())
    }

    /// No external process to watch: the in-process replicator is healthy as
    /// long as celld is running.
    pub fn process_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        Ok(None)
    }
}

fn files_equal(left: &Path, right: &Path) -> std::io::Result<bool> {
    if std::fs::metadata(left)?.len() != std::fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left = BufReader::new(std::fs::File::open(left)?);
    let mut right = BufReader::new(std::fs::File::open(right)?);
    let mut left_buffer = [0_u8; 64 * 1024];
    let mut right_buffer = [0_u8; 64 * 1024];
    loop {
        let left_read = left.read(&mut left_buffer)?;
        let right_read = right.read(&mut right_buffer)?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

/// One capture+upload for a cell: advance its durable position on success and
/// wake its waiters. Everything committed before the capture is durable once
/// uploaded, so the target is read before `db.sync`. The `rusqlite` handle is
/// `!Sync`, so the whole pass runs on a blocking thread with `block_on` for the
/// async upload. `Some(true)` means success, `Some(false)` means failure, and
/// `None` means that the replica lost its database.
async fn sync_cell(handle: CellHandle) -> Option<bool> {
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        // Tickets taken before the capture: their writes committed before
        // `db.sync` runs, so it captures them. Read before the capture so a
        // ticket taken during the sync is credited by the next one, not this.
        let captured = handle.req_seq.load(Ordering::SeqCst);
        let mut replica = handle.replica.lock().unwrap();
        let db = replica.db_mut()?;
        if let Err(error) = db.sync() {
            warn!(%error, "ltx wal capture failed");
            handle.ready.notify_waiters();
            return Some(false);
        }
        let durable_txid = match runtime.block_on(replica.sync()) {
            Ok(()) => Some(replica.pos().txid.0),
            Err(error) => {
                warn!(%error, "ltx upload failed");
                None
            }
        };
        drop(replica);
        if let Some(durable_txid) = durable_txid {
            handle.synced_seq.fetch_max(captured, Ordering::SeqCst);
            handle.durable_txid.store(durable_txid, Ordering::SeqCst);
            maybe_queue_compaction(&handle, durable_txid);
        }
        handle.ready.notify_waiters();
        Some(durable_txid.is_some())
    })
    .await
    .unwrap_or(Some(false))
}

fn maybe_queue_compaction(handle: &CellHandle, durable_txid: u64) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    if compaction.cancelled.load(Ordering::SeqCst)
        || durable_txid.saturating_sub(compaction.compacted_txid.load(Ordering::SeqCst))
            < compaction.min_txids
        || compaction
            .queued
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
    {
        return;
    }
    if compaction
        .queue
        .send(CompactionWork {
            cell: Arc::downgrade(handle),
            queued_at: Instant::now(),
        })
        .is_err()
    {
        compaction.queued.store(false, Ordering::SeqCst);
    }
}

fn cancel_compaction(handle: &CellHandle) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    compaction.cancelled.store(true, Ordering::SeqCst);
    compaction.cancel.notify_waiters();
}

fn start_compaction_loop(config: CompactionConfig) -> mpsc::UnboundedSender<CompactionWork> {
    let (queue, mut work) = mpsc::unbounded_channel::<CompactionWork>();
    let slots = Arc::new(Semaphore::new(config.concurrency));
    tokio::spawn(async move {
        while let Some(work) = work.recv().await {
            let Ok(permit) = slots.clone().acquire_owned().await else {
                break;
            };
            let Some(cell) = work.cell.upgrade() else {
                continue;
            };
            tokio::spawn(async move {
                let _permit = permit;
                compact_cell(cell, work.queued_at).await;
            });
        }
    });
    queue
}

async fn compact_cell(handle: CellHandle, queued_at: Instant) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    let cancelled = compaction.cancel.notified();
    tokio::pin!(cancelled);
    if compaction.cancelled.load(Ordering::SeqCst) {
        return;
    }

    let queue_ms = queued_at.elapsed().as_millis() as u64;
    let started = Instant::now();
    let compactor = ReplicaCompactor::new(&compaction.client)
        .with_verification(true)
        .with_local_path(&compaction.local_path)
        .with_limits(COMPACTION_MAX_FILES, COMPACTION_MAX_INPUT_BYTES);
    let worker = compactor.compact(1);
    tokio::pin!(worker);
    let result = tokio::select! {
        biased;
        _ = &mut cancelled => None,
        result = &mut worker => Some(result),
    };

    let mut completed = false;
    match result {
        Some(Ok(Some(output))) => {
            let info = output.info;
            compaction
                .compacted_txid
                .store(info.max_txid.0, Ordering::SeqCst);
            completed = true;
            info!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = info.level,
                min_txid = info.min_txid.0,
                max_txid = info.max_txid.0,
                input_objects = output.input_files,
                input_bytes = output.input_bytes,
                local_input_objects = output.local_input_files,
                remote_input_objects = output.input_files - output.local_input_files,
                output_bytes = info.size,
                queue_ms,
                elapsed_ms = started.elapsed().as_millis() as u64,
                result = "ok",
                "compacted an additive LTX level"
            );
        }
        Some(Ok(None)) => {
            compaction
                .compacted_txid
                .store(handle.durable_txid.load(Ordering::SeqCst), Ordering::SeqCst);
            completed = true;
            info!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = started.elapsed().as_millis() as u64,
                result = "no_work",
                "the additive LTX level is current"
            );
        }
        Some(Err(error)) => {
            warn!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = started.elapsed().as_millis() as u64,
                result = "error",
                %error,
                "additive LTX compaction failed"
            );
        }
        None => {
            info!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = started.elapsed().as_millis() as u64,
                result = "cancelled",
                "cancelled an additive LTX compaction"
            );
        }
    }
    compaction.queued.store(false, Ordering::SeqCst);

    if completed && !compaction.cancelled.load(Ordering::SeqCst) {
        // Pace consecutive rounds for one cell: a restart with a large tail
        // otherwise drains back-to-back for minutes. The pause matches the
        // round it follows (capped), so a cell compacts at half duty cycle
        // while the worker slot frees for other cells immediately — this
        // task detaches and does not hold the concurrency permit.
        let pause = started.elapsed().min(std::time::Duration::from_secs(2));
        let handle_ = handle.clone();
        tokio::spawn(async move {
            tokio::time::sleep(pause).await;
            let durable_txid = handle_.durable_txid.load(Ordering::SeqCst);
            maybe_queue_compaction(&handle_, durable_txid);
        });
    }
}

fn compaction_config_from_env() -> anyhow::Result<Option<CompactionConfig>> {
    // On by default. A mixed fleet must set `0` until every node can read
    // v0.5.2 block objects. An old reader cannot take over a cell after its
    // first L1 publication.
    let enabled = crate::env_vars::flag("CELLD_LTX_COMPACTION", true)?;
    if !enabled {
        return Ok(None);
    }

    let min_txids = crate::env_vars::with_default("CELLD_LTX_COMPACTION_MIN_TXIDS", 256)?;
    let concurrency = crate::env_vars::with_default("CELLD_LTX_COMPACTIONS", 2)?;
    anyhow::ensure!(
        min_txids >= 2,
        "CELLD_LTX_COMPACTION_MIN_TXIDS must be at least 2"
    );
    anyhow::ensure!(concurrency > 0, "CELLD_LTX_COMPACTIONS must be positive");
    Ok(Some(CompactionConfig {
        min_txids: min_txids as u64,
        concurrency,
    }))
}

/// The node's background sync loop: wake on a dirty cell (or a slow tick) and
/// launch a sync for every cell whose committed position runs ahead of its
/// durable one. Each cell's sync is an independent, self-rescheduling task —
/// the loop does *not* wait for the batch to finish — so one slow cell's upload
/// never stalls the others (a cell keeps its own cadence up to the concurrency
/// bound). A cell's writes reported between its syncs still clear on one upload:
/// the batching win, without the cross-cell head-of-line blocking.
async fn sync_loop(
    cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>>,
    dirty: Arc<Notify>,
    slots: Arc<Semaphore>,
) {
    loop {
        tokio::select! {
            _ = dirty.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
        let work: Vec<CellHandle> = {
            let map = cells.lock().unwrap();
            map.values()
                .filter(|c| c.req_seq.load(Ordering::SeqCst) > c.synced_seq.load(Ordering::SeqCst))
                .cloned()
                .collect()
        };
        for cell in work {
            // Claim the cell; skip if a sync is already in flight for it.
            if cell
                .syncing
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                continue;
            }
            let slots = slots.clone();
            let dirty = dirty.clone();
            tokio::spawn(async move {
                // Keep syncing this cell while it stays dirty, rather than
                // notifying the main loop to re-scan every completion — that made
                // the loop wake O(cells) times and starved throughput as cells
                // accumulated. This is not a busy loop: each iteration awaits an
                // object-store upload (~one round-trip). A *failed* sync would
                // not, so it backs off, keeping the only tight iterations the
                // ones that actually uploaded.
                loop {
                    let ok = {
                        let _permit = slots.acquire().await;
                        sync_cell(cell.clone()).await
                    };
                    if cell.req_seq.load(Ordering::SeqCst) <= cell.synced_seq.load(Ordering::SeqCst)
                    {
                        break;
                    }
                    if ok != Some(true) {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
                cell.syncing.store(false, Ordering::SeqCst);
                // A write landing in the clear window is picked up next tick;
                // nudge the loop so it does not wait the full interval.
                if cell.req_seq.load(Ordering::SeqCst) > cell.synced_seq.load(Ordering::SeqCst) {
                    dirty.notify_one();
                }
            });
        }
    }
}

/// Node-level object-store config (no per-cell prefix). `build_store` on this
/// yields the one shared client; per-cell clients set only `path`.
fn node_config(
    bucket: &str,
    endpoint: Option<&str>,
    region: &str,
    credentials: Option<&StorageCredentials>,
) -> ObjectStoreConfig {
    let endpoint = endpoint.unwrap_or_default().to_string();
    // Static credentials come from the managed control plane when present,
    // else the `AWS_*` env the node already carries. Without this,
    // `build_store` sees empty keys and object_store falls back to the
    // instance credential provider, which off-EC2 sends unsigned requests (R2
    // answers "404 page not found").
    let env = |key: &str| std::env::var(key).ok().filter(|value| !value.is_empty());
    let access_key_id = credentials
        .map(|c| c.access_key_id.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_ACCESS_KEY_ID"))
        .unwrap_or_default();
    let secret_access_key = credentials
        .map(|c| c.secret_access_key.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_SECRET_ACCESS_KEY"))
        .unwrap_or_default();
    // Temporary R2/STS credentials require the session token, or signing fails.
    let session_token = credentials
        .and_then(|c| c.session_token.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_SESSION_TOKEN"))
        .unwrap_or_default();
    ObjectStoreConfig {
        bucket: bucket.to_string(),
        path: String::new(),
        region: region.to_string(),
        // A custom endpoint (R2/MinIO) uses path-style addressing, matching
        // `ObjectStoreConfig::from_url`'s default for non-AWS hosts.
        force_path_style: !endpoint.is_empty(),
        endpoint,
        access_key_id,
        secret_access_key,
        session_token,
        skip_verify: false,
        part_size: 0,
    }
}

#[cfg(test)]
mod fork_seed_tests {
    use super::*;
    use base64::Engine;
    use celld_ltx::object_store::memory::InMemory;
    use celld_ltx::object_store::path::Path as ObjPath;
    use celld_ltx::object_store::PutPayload;
    use futures_util::TryStreamExt;

    fn encrypted_codec(active: &str, keys: &[(&str, u8)]) -> Arc<dyn ReplicaObjectCodec> {
        let keys = keys
            .iter()
            .map(|(id, byte)| {
                (
                    id.to_string(),
                    base64::engine::general_purpose::STANDARD.encode([*byte; 32]),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        Arc::new(
            crate::durability_encryption::Aes256GcmDurabilityCodec::parse(
                &serde_json::json!({ "active_key_id": active, "keys": keys }).to_string(),
                false,
            )
            .unwrap(),
        )
    }

    async fn raw_objects(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<(String, Vec<u8>)> {
        let prefix = ObjPath::from(prefix);
        let mut listed = store.list(Some(&prefix));
        let mut objects = Vec::new();
        while let Some(meta) = listed.try_next().await.unwrap() {
            let bytes = store
                .get(&meta.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            objects.push((meta.location.to_string(), bytes.to_vec()));
        }
        objects.sort_by(|left, right| left.0.cmp(&right.0));
        objects
    }

    fn activation<'a>(cell: &'a str, fresh: bool) -> ActivationOptions<'a> {
        ActivationOptions {
            cell,
            epoch: 1,
            fresh,
            took_over: false,
            resume_local: false,
        }
    }

    async fn plant_fork_seed(
        store: &Arc<dyn ObjectStore>,
        cell: &str,
        manifest: &ForkSeedManifest,
        sqlite: Vec<u8>,
    ) {
        let encoded = serde_json::to_vec(manifest).unwrap();
        for (name, bytes) in [
            ("reserved.json", encoded.clone()),
            (DATABASE_OBJECT_NAME, sqlite),
            ("ready.json", encoded),
        ] {
            store
                .put(
                    &ObjPath::from(format!("cells/{cell}/fork-seed/{name}")),
                    PutPayload::from(bytes),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn fork_seed_is_exact_create_only_and_independent() {
        let directory = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let replication = LtxRepl::start_with_store_for_test(directory.path(), store);
        let source = replication
            .activate(activation("source", true))
            .await
            .unwrap();
        {
            let connection = rusqlite::Connection::open(&source.path).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE state(key TEXT PRIMARY KEY, value TEXT NOT NULL);\n\
                     INSERT INTO state VALUES ('phase', 'checkpointed');",
                )
                .unwrap();
        }
        replication.await_durable("source", 1, 1).await.unwrap();

        let manifest = replication
            .publish_checkpoint("source", 1, "checkpoint-1")
            .await
            .unwrap();
        replication
            .publish_fork_seed_from_checkpoint("source", "checkpoint-1", "fork", false)
            .await
            .unwrap();
        assert_eq!(manifest.source_cell, "source");
        assert_eq!(manifest.source_epoch, 1);
        assert_eq!(manifest.checkpoint_id, "checkpoint-1");
        assert_eq!(manifest.sqlite_sha256.len(), 64);
        assert_eq!(
            replication
                .publish_fork_seed_from_checkpoint("source", "checkpoint-1", "fork", false,)
                .await
                .unwrap(),
            manifest
        );

        let source_connection = rusqlite::Connection::open(&source.path).unwrap();
        source_connection
            .execute(
                "UPDATE state SET value = 'source-advanced' WHERE key = 'phase'",
                [],
            )
            .unwrap();
        drop(source_connection);
        assert!(replication
            .publish_checkpoint("source", 1, "checkpoint-1")
            .await
            .unwrap_err()
            .to_string()
            .contains("already contains a different database.sqlite"));

        let existing = replication
            .activate(activation("existing", true))
            .await
            .unwrap();
        {
            let connection = rusqlite::Connection::open(&existing.path).unwrap();
            connection
                .execute("CREATE TABLE occupied(value TEXT)", [])
                .unwrap();
        }
        replication.await_durable("existing", 1, 1).await.unwrap();
        let existing_target = replication
            .publish_fork_seed_from_checkpoint("source", "checkpoint-1", "existing", false)
            .await
            .unwrap_err();
        assert!(existing_target
            .to_string()
            .contains("already has a durable replica"));

        let fork = replication
            .activate(activation("fork", true))
            .await
            .unwrap();
        assert!(fork.restored);
        let connection = rusqlite::Connection::open(&fork.path).unwrap();
        let value: String = connection
            .query_row("SELECT value FROM state WHERE key = 'phase'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(value, "checkpointed");
        connection
            .execute("UPDATE state SET value = 'forked' WHERE key = 'phase'", [])
            .unwrap();
        drop(connection);
        replication.await_durable("fork", 1, 1).await.unwrap();
        assert_eq!(
            replication
                .publish_fork_seed_from_checkpoint("source", "checkpoint-1", "fork", true,)
                .await
                .unwrap(),
            manifest
        );

        let source_connection = rusqlite::Connection::open(&source.path).unwrap();
        let source_value: String = source_connection
            .query_row("SELECT value FROM state WHERE key = 'phase'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(source_value, "source-advanced");
    }

    #[tokio::test]
    async fn encrypted_ltx_checkpoint_and_fork_survive_rotation_and_restore() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old_directory = tempfile::tempdir().unwrap();
        let old = LtxRepl::start_with_store_and_codec_for_test(
            old_directory.path(),
            store.clone(),
            encrypted_codec("old", &[("old", 1)]),
        );
        let source = old.activate(activation("source", true)).await.unwrap();
        {
            let connection = rusqlite::Connection::open(&source.path).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE state(key TEXT PRIMARY KEY, value TEXT NOT NULL);\n\
                     INSERT INTO state VALUES ('phase', 'encrypted-old');",
                )
                .unwrap();
        }
        old.await_durable("source", 1, 1).await.unwrap();
        old.publish_checkpoint("source", 1, "checkpoint-1")
            .await
            .unwrap();
        // Create-or-verify compares decrypted bytes. A randomized nonce must
        // not make an exact checkpoint retry look like conflicting content.
        old.publish_checkpoint("source", 1, "checkpoint-1")
            .await
            .unwrap();

        let old_ltx = raw_objects(&store, "cells/source/ltx/e1/").await;
        assert!(!old_ltx.is_empty());
        assert!(old_ltx.iter().all(|(_, bytes)| {
            crate::durability_encryption::envelope_key_id_for_test(bytes).unwrap() == "old"
        }));
        let checkpoint = store
            .get(&ObjPath::from(
                "cells/source/checkpoints/checkpoint-1/database.sqlite",
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(checkpoint.starts_with(b"CRCELD01"));
        assert!(!checkpoint
            .windows(b"encrypted-old".len())
            .any(|window| window == b"encrypted-old"));

        let missing_directory = tempfile::tempdir().unwrap();
        let missing = LtxRepl::start_with_store_and_codec_for_test(
            missing_directory.path(),
            store.clone(),
            encrypted_codec("new", &[("new", 2)]),
        );
        let error = match missing
            .activate(ActivationOptions {
                cell: "source",
                epoch: 2,
                fresh: false,
                took_over: true,
                resume_local: false,
            })
            .await
        {
            Ok(_) => panic!("restore succeeded without the required old key"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unknown key old"));

        let rotated_directory = tempfile::tempdir().unwrap();
        let rotated = LtxRepl::start_with_store_and_codec_for_test(
            rotated_directory.path(),
            store.clone(),
            encrypted_codec("new", &[("old", 1), ("new", 2)]),
        );
        let restored = rotated
            .activate(ActivationOptions {
                cell: "source",
                epoch: 2,
                fresh: false,
                took_over: true,
                resume_local: false,
            })
            .await
            .unwrap();
        assert!(restored.restored);
        let connection = rusqlite::Connection::open(&restored.path).unwrap();
        let value: String = connection
            .query_row("SELECT value FROM state WHERE key = 'phase'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(value, "encrypted-old");
        connection
            .execute(
                "UPDATE state SET value = 'encrypted-new' WHERE key = 'phase'",
                [],
            )
            .unwrap();
        drop(connection);
        rotated.await_durable("source", 2, 1).await.unwrap();
        let new_ltx = raw_objects(&store, "cells/source/ltx/e2/").await;
        assert!(!new_ltx.is_empty());
        assert!(new_ltx.iter().all(|(_, bytes)| {
            crate::durability_encryption::envelope_key_id_for_test(bytes).unwrap() == "new"
        }));

        rotated
            .publish_fork_seed_from_checkpoint("source", "checkpoint-1", "fork", false)
            .await
            .unwrap();
        rotated
            .publish_fork_seed_from_checkpoint("source", "checkpoint-1", "fork", false)
            .await
            .unwrap();
        let fork_object = store
            .get(&ObjPath::from("cells/fork/fork-seed/database.sqlite"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(fork_object.starts_with(b"CRCELD01"));
        assert_eq!(
            crate::durability_encryption::envelope_key_id_for_test(&fork_object).unwrap(),
            "new"
        );

        let fork = rotated.activate(activation("fork", true)).await.unwrap();
        assert!(fork.restored);
        let fork_connection = rusqlite::Connection::open(&fork.path).unwrap();
        let fork_value: String = fork_connection
            .query_row("SELECT value FROM state WHERE key = 'phase'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(fork_value, "encrypted-old");

        // The supported offline export path restores through the same codec.
        let exported = rotated.restore_snapshot("source").await.unwrap().unwrap();
        let exported_connection = rusqlite::Connection::open(exported.path()).unwrap();
        let exported_value: String = exported_connection
            .query_row("SELECT value FROM state WHERE key = 'phase'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(exported_value, "encrypted-new");

        // The supported offline import path encrypts its new LTX lineage and
        // verifies it by immediately restoring through the configured codec.
        let import_directory = tempfile::tempdir().unwrap();
        let import_database = import_directory.path().join("import.sqlite");
        let import_connection = rusqlite::Connection::open(&import_database).unwrap();
        import_connection
            .execute_batch(
                "CREATE TABLE imported(value TEXT NOT NULL);\n\
                 INSERT INTO imported VALUES ('encrypted-import');",
            )
            .unwrap();
        drop(import_connection);
        rotated
            .seed_import_epoch("imported", 1, &import_database)
            .await
            .unwrap();
        let imported_ltx = raw_objects(&store, "cells/imported/ltx/e1/").await;
        assert!(!imported_ltx.is_empty());
        assert!(imported_ltx.iter().all(|(_, bytes)| {
            crate::durability_encryption::envelope_key_id_for_test(bytes).unwrap() == "new"
        }));

        let (tampered_key, mut tampered_bytes) = new_ltx.into_iter().next().unwrap();
        *tampered_bytes.last_mut().unwrap() ^= 1;
        store
            .put(
                &ObjPath::from(tampered_key),
                PutPayload::from(tampered_bytes),
            )
            .await
            .unwrap();
        let corrupt_directory = tempfile::tempdir().unwrap();
        let corrupt = LtxRepl::start_with_store_and_codec_for_test(
            corrupt_directory.path(),
            store.clone(),
            encrypted_codec("new", &[("old", 1), ("new", 2)]),
        );
        let error = match corrupt
            .activate(ActivationOptions {
                cell: "source",
                epoch: 3,
                fresh: false,
                took_over: true,
                resume_local: false,
            })
            .await
        {
            Ok(_) => panic!("restore accepted tampered ciphertext"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("authentication failed"));
    }

    #[tokio::test]
    async fn active_snapshots_use_independent_temporary_directories() {
        let directory = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let replication = LtxRepl::start_with_store_for_test(directory.path(), store);
        replication
            .activate(activation("source", true))
            .await
            .unwrap();

        let first = replication.snapshot_active("source", 1).unwrap().unwrap();
        let second = replication.snapshot_active("source", 1).unwrap().unwrap();
        assert_ne!(first.path(), second.path());
        assert!(first.path().is_file());
        assert!(second.path().is_file());
    }

    #[tokio::test]
    async fn incomplete_seed_never_activates_as_empty() {
        let directory = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &ObjPath::from("cells/incomplete/fork-seed/reserved.json"),
                PutPayload::from_static(b"{}"),
            )
            .await
            .unwrap();
        let replication = LtxRepl::start_with_store_for_test(directory.path(), store);
        let error = match replication.activate(activation("incomplete", true)).await {
            Ok(_) => panic!("incomplete seed activated"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("reserved but incomplete"));
        assert!(!directory
            .path()
            .join("incomplete/ltx/e1/db.sqlite")
            .exists());
    }

    #[tokio::test]
    async fn corrupt_seed_hash_never_activates() {
        let directory = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sqlite = b"not a sqlite database".to_vec();
        let manifest = ForkSeedManifest {
            format: FORK_SEED_FORMAT.to_string(),
            checkpoint_id: "checkpoint-corrupt-hash".to_string(),
            source_cell: "source".to_string(),
            source_epoch: 1,
            sqlite_sha256: "0".repeat(64),
            sqlite_bytes: sqlite.len() as u64,
        };
        plant_fork_seed(&store, "corrupt-hash", &manifest, sqlite).await;
        let replication = LtxRepl::start_with_store_for_test(directory.path(), store);
        let error = match replication.activate(activation("corrupt-hash", true)).await {
            Ok(_) => panic!("corrupt hash seed activated"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("fork seed hash mismatch"));
        assert!(!directory
            .path()
            .join("corrupt-hash/ltx/e1/db.sqlite")
            .exists());
    }

    #[tokio::test]
    async fn invalid_sqlite_seed_never_activates() {
        let directory = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sqlite = b"not a sqlite database".to_vec();
        let manifest = ForkSeedManifest {
            format: FORK_SEED_FORMAT.to_string(),
            checkpoint_id: "checkpoint-invalid-sqlite".to_string(),
            source_cell: "source".to_string(),
            source_epoch: 1,
            sqlite_sha256: format!("{:x}", Sha256::digest(&sqlite)),
            sqlite_bytes: sqlite.len() as u64,
        };
        plant_fork_seed(&store, "invalid-sqlite", &manifest, sqlite).await;
        let replication = LtxRepl::start_with_store_for_test(directory.path(), store);
        let error = match replication
            .activate(activation("invalid-sqlite", true))
            .await
        {
            Ok(_) => panic!("invalid SQLite seed activated"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("fork seed SQLite quick_check failed"));
        assert!(!directory
            .path()
            .join("invalid-sqlite/ltx/e1/db.sqlite")
            .exists());
    }
}
