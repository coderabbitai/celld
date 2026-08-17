#!/usr/bin/env bash

# The default CI lane uses an isolated MinIO container. Set
# CELLD_ARCHIVE_BACKEND=gcs, CELLD_ARCHIVE_GCS_PROJECT, and
# CELLD_ARCHIVE_BINARY to exercise the same recovery contract against a unique
# GCS bucket with a host binary and Application Default Credentials.

set -euo pipefail

readonly MINIO_IMAGE='minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e'
readonly MC_IMAGE='minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727'
readonly CELLD_IMAGE="${CELLD_ARCHIVE_IMAGE:-celld-ci}"
readonly BACKEND="${CELLD_ARCHIVE_BACKEND:-minio}"
readonly RUN_ID="cell-archive-${RANDOM}-$$"
readonly NETWORK="${RUN_ID}-network"
readonly MINIO="${RUN_ID}-minio"
TEST_ROOT="$(mktemp -d /tmp/celld-archive.XXXXXX)"
readonly TEST_ROOT
readonly ENDPOINT='http://minio:9000'
ACCESS_KEY="celld$(printf '%s' "$RUN_ID-access" | shasum -a 256 | cut -c1-12)"
readonly ACCESS_KEY
SECRET_KEY="$(printf '%s' "$RUN_ID-secret" | shasum -a 256 | cut -c1-32)"
readonly SECRET_KEY
export TEST_ROOT

if [[ "$BACKEND" != 'minio' && "$BACKEND" != 'gcs' ]]; then
	echo 'CELLD_ARCHIVE_BACKEND must be minio or gcs' >&2
	exit 1
fi

gcs_bucket_created=false
if [[ "$BACKEND" == 'gcs' ]]; then
	readonly GCS_PROJECT="${CELLD_ARCHIVE_GCS_PROJECT:-}"
	if [[ ! "$GCS_PROJECT" =~ ^[a-z][a-z0-9-]{4,28}[a-z0-9]$ ]]; then
		echo 'CELLD_ARCHIVE_GCS_PROJECT must be an explicit GCP project ID' >&2
		exit 1
	fi
	if [[ -z "${CELLD_ARCHIVE_BINARY:-}" || ! -x "$CELLD_ARCHIVE_BINARY" ]]; then
		echo 'CELLD_ARCHIVE_BINARY must name an executable host Celld binary' >&2
		exit 1
	fi
	project_hash="$(printf '%s' "$GCS_PROJECT" | shasum -a 256 | cut -c1-10)"
	run_hash="$(printf '%s' "$RUN_ID" | shasum -a 256 | cut -c1-16)"
	readonly GCS_BUCKET="cr-celld-archive-$project_hash-$run_hash"
	readonly BUCKET="gs://$GCS_BUCKET/fleet"
	gcloud auth application-default print-access-token >/dev/null
else
	readonly BUCKET='celld-archive/fleet'
fi

cleanup() {
	docker rm -f "$MINIO" >/dev/null 2>&1 || true
	docker network rm "$NETWORK" >/dev/null 2>&1 || true
	if [[ "$gcs_bucket_created" == true ]]; then
		gcloud storage rm --recursive "gs://$GCS_BUCKET/**" >/dev/null 2>&1 || true
		gcloud storage buckets delete "gs://$GCS_BUCKET" --quiet >/dev/null 2>&1 || true
	fi
	rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

if [[ "$BACKEND" == 'minio' ]]; then
	docker network create "$NETWORK" >/dev/null
	docker run -d --name "$MINIO" --network "$NETWORK" --network-alias minio \
		-e "MINIO_ROOT_USER=$ACCESS_KEY" -e "MINIO_ROOT_PASSWORD=$SECRET_KEY" \
		"$MINIO_IMAGE" server /data >/dev/null
else
	gcloud storage buckets create "gs://$GCS_BUCKET" \
		--project "$GCS_PROJECT" --location us-central1 \
		--uniform-bucket-level-access >/dev/null
	gcs_bucket_created=true
fi

mc() {
	docker run --rm -i --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" -c \
		"mc alias set local $ENDPOINT $ACCESS_KEY $SECRET_KEY >/dev/null && $*"
}

object_cat() {
	local key="$1"
	if [[ "$BACKEND" == 'minio' ]]; then
		mc "mc cat local/celld-archive/fleet/$key"
	else
		gcloud storage cat "gs://$GCS_BUCKET/fleet/$key"
	fi
}

object_put() {
	local key="$1"
	if [[ "$BACKEND" == 'minio' ]]; then
		mc "mc pipe local/celld-archive/fleet/$key >/dev/null"
	else
		local input_file
		input_file="$(mktemp "$TEST_ROOT/object.XXXXXX")"
		cat > "$input_file"
		gcloud storage cp "$input_file" "gs://$GCS_BUCKET/fleet/$key" >/dev/null
		rm -f "$input_file"
	fi
}

object_prefix_exists() {
	local prefix="$1"
	if [[ "$BACKEND" == 'minio' ]]; then
		[[ -n "$(mc "mc find local/celld-archive/fleet/$prefix" 2>/dev/null)" ]]
	else
		gcloud storage ls --recursive "gs://$GCS_BUCKET/fleet/$prefix/**" >/dev/null 2>&1
	fi
}

if [[ "$BACKEND" == 'minio' ]]; then
	for attempt in $(seq 1 40); do
		if mc 'mc ready local >/dev/null' 2>/dev/null; then
			break
		fi
		if [[ "$attempt" == 40 ]]; then
			echo 'MinIO did not become ready' >&2
			exit 1
		fi
		sleep 0.25
	done
	mc 'mc mb local/celld-archive >/dev/null'
fi

python3 <<'PY'
import os
import sqlite3

path = os.path.join(os.environ["TEST_ROOT"], "source.sqlite")
connection = sqlite3.connect(path)
connection.execute("PRAGMA journal_mode=WAL")
connection.execute("CREATE TABLE facts(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
connection.executemany(
    "INSERT INTO facts(body) VALUES (?)",
    [("durable knowledge",), ("second fact",)],
)
connection.commit()
connection.close()
PY

celld() {
	if [[ "$BACKEND" == 'minio' ]]; then
		docker run --rm --network "$NETWORK" --user "$(id -u):$(id -g)" \
			-v "$TEST_ROOT:/archive" \
			-e "AWS_ACCESS_KEY_ID=$ACCESS_KEY" -e "AWS_SECRET_ACCESS_KEY=$SECRET_KEY" \
			-e AWS_REGION=us-east-1 "$CELLD_IMAGE" "$@"
	else
		"$CELLD_ARCHIVE_BINARY" "$@"
	fi
}

if [[ "$BACKEND" == 'minio' ]]; then
	archive_root='/archive'
	storage_args=(--bucket "$BUCKET" --endpoint "$ENDPOINT")
else
	archive_root="$TEST_ROOT"
	storage_args=(--bucket "$BUCKET")
fi

celld cell import Knowledge:test --input "$archive_root/source.sqlite" \
	"${storage_args[@]}" --offline
celld cell export Knowledge:test --output "$archive_root/export.sqlite" \
	"${storage_args[@]}"

python3 <<'PY'
import hashlib
import json
import os
import sqlite3
import stat

root = os.environ["TEST_ROOT"]
database = os.path.join(root, "export.sqlite")
manifest_path = database + ".manifest.json"
connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
rows = connection.execute("SELECT body FROM facts ORDER BY id").fetchall()
connection.close()
assert rows == [("durable knowledge",), ("second fact",)]
with open(database, "rb") as file:
    digest = hashlib.sha256(file.read()).hexdigest()
with open(manifest_path, encoding="utf-8") as file:
    manifest = json.load(file)
assert manifest == {
    "version": 1,
    "cell": "Knowledge:test",
    "source_epoch": 1,
    "source_txid": 1,
    "database_sha256": digest,
}
assert stat.S_IMODE(os.stat(database).st_mode) == 0o600
assert stat.S_IMODE(os.stat(manifest_path).st_mode) == 0o600
PY

# Recreate a crash after the owner CAS but before the ready-marker CAS. The
# second command must derive the durable TXID from epoch 1 and finish safely.
ready_marker="$(object_cat 'cells/Knowledge:test/import.json')"
source_hash="$(jq -r .source_sha256 <<<"$ready_marker")"
object_put 'cells/Knowledge:test/import.json' <<JSON
{"version":1,"phase":"staging","source_sha256":"$source_hash","attempt_id":"11111111111111111111111111111111","attempt_expires_ms":1,"durable_txid":null}
JSON
celld cell import Knowledge:test --input "$archive_root/source.sqlite" \
	"${storage_args[@]}" --offline --resume
resumed_marker="$(object_cat 'cells/Knowledge:test/import.json')"
jq -e '.phase == "ready" and .durable_txid == 1' <<<"$resumed_marker" >/dev/null

# A crash before the owner CAS can leave partial LTX. An expired claimant may
# resume, clears that private epoch, and republishes a fully verified lineage.
object_put 'cells/Knowledge:partial/import.json' <<JSON
{"version":1,"phase":"staging","source_sha256":"$source_hash","attempt_id":"22222222222222222222222222222222","attempt_expires_ms":1,"durable_txid":null}
JSON
object_put 'cells/Knowledge:partial/ltx/e1/partial' <<'PARTIAL'
not-an-ltx-file
PARTIAL
celld cell import Knowledge:partial --input "$archive_root/source.sqlite" \
	"${storage_args[@]}" --offline --resume
partial_marker="$(object_cat 'cells/Knowledge:partial/import.json')"
jq -e '.phase == "ready" and .durable_txid == 1' <<<"$partial_marker" >/dev/null

# A still-live staging claimant cannot be stolen by a concurrent retry.
future_attempt="$(( $(date +%s) * 1000 + 60000 ))"
object_put 'cells/Knowledge:active/import.json' <<JSON
{"version":1,"phase":"staging","source_sha256":"$source_hash","attempt_id":"33333333333333333333333333333333","attempt_expires_ms":$future_attempt,"durable_txid":null}
JSON
if celld cell import Knowledge:active --input "$archive_root/source.sqlite" \
	"${storage_args[@]}" --offline --resume >"$TEST_ROOT/active.log" 2>&1; then
	echo 'a concurrent retry unexpectedly stole an active import attempt' >&2
	exit 1
fi
grep -F 'is still active' "$TEST_ROOT/active.log" >/dev/null

# A different archive cannot replace the imported lineage.
python3 <<'PY'
import os
import sqlite3

path = os.path.join(os.environ["TEST_ROOT"], "different.sqlite")
connection = sqlite3.connect(path)
connection.execute("CREATE TABLE facts(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
connection.execute("INSERT INTO facts(body) VALUES ('different')")
connection.commit()
connection.close()
PY
if celld cell import Knowledge:test --input "$archive_root/different.sqlite" \
	"${storage_args[@]}" --offline >"$TEST_ROOT/different.log" 2>&1; then
	echo 'a different archive unexpectedly replaced the imported lineage' >&2
	exit 1
fi
grep -F 'different archive' "$TEST_ROOT/different.log" >/dev/null

# A live fleet also fails closed before creating a target marker or lineage.
future_ms="$(( $(date +%s) * 1000 + 60000 ))"
object_put 'nodes/live-test.json' <<JSON
{"node":"live-test","expires_ms":$future_ms,"addr":"127.0.0.1:1","probe_public_key":"x","peer_protocol":1,"ownership_index_generation":"x","load":{"sampled_ms":$future_ms,"resident_cells":0,"host_websockets":0,"rss_bytes":1,"in_use_bytes":null,"cpu_percent_x100":0,"open_fds":0,"fd_limit":1,"pressured":false,"shed_cells":0,"restoring":0}}
JSON
if celld cell import Knowledge:blocked --input "$archive_root/source.sqlite" \
	"${storage_args[@]}" --offline >"$TEST_ROOT/live.log" 2>&1; then
	echo 'import unexpectedly ran while a node lease was live' >&2
	exit 1
fi
grep -F 'live celld node lease(s): live-test' "$TEST_ROOT/live.log" >/dev/null
if object_prefix_exists 'cells/Knowledge:blocked'; then
	echo 'failed live-fleet import created target marker, owner, or LTX state' >&2
	exit 1
fi

echo "cell archive $BACKEND test passed"
