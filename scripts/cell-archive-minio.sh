#!/usr/bin/env bash

set -euo pipefail

readonly MINIO_IMAGE='minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e'
readonly MC_IMAGE='minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727'
readonly CELLD_IMAGE="${CELLD_ARCHIVE_IMAGE:-celld-ci}"
readonly RUN_ID="cell-archive-${RANDOM}-$$"
readonly NETWORK="${RUN_ID}-network"
readonly MINIO="${RUN_ID}-minio"
readonly TEST_ROOT="$(mktemp -d /tmp/celld-archive.XXXXXX)"
readonly BUCKET='celld-archive/fleet'
readonly ENDPOINT='http://minio:9000'
readonly ACCESS_KEY='celldtest'
readonly SECRET_KEY='celldtestsecret'
export TEST_ROOT

cleanup() {
	docker rm -f "$MINIO" >/dev/null 2>&1 || true
	docker network rm "$NETWORK" >/dev/null 2>&1 || true
	rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

docker network create "$NETWORK" >/dev/null
docker run -d --name "$MINIO" --network "$NETWORK" --network-alias minio \
	-e "MINIO_ROOT_USER=$ACCESS_KEY" -e "MINIO_ROOT_PASSWORD=$SECRET_KEY" \
	"$MINIO_IMAGE" server /data >/dev/null

mc() {
	docker run --rm -i --network "$NETWORK" --entrypoint /bin/sh "$MC_IMAGE" -c \
		"mc alias set local $ENDPOINT $ACCESS_KEY $SECRET_KEY >/dev/null && $*"
}

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
	docker run --rm --network "$NETWORK" --user "$(id -u):$(id -g)" \
		-v "$TEST_ROOT:/archive" \
		-e "AWS_ACCESS_KEY_ID=$ACCESS_KEY" -e "AWS_SECRET_ACCESS_KEY=$SECRET_KEY" \
		-e AWS_REGION=us-east-1 "$CELLD_IMAGE" "$@"
}

celld cell import Knowledge:test --input /archive/source.sqlite \
	--bucket "$BUCKET" --endpoint "$ENDPOINT" --offline
celld cell export Knowledge:test --output /archive/export.sqlite \
	--bucket "$BUCKET" --endpoint "$ENDPOINT"

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
ready_marker="$(mc 'mc cat local/celld-archive/fleet/cells/Knowledge:test/import.json')"
source_hash="$(jq -r .source_sha256 <<<"$ready_marker")"
mc 'mc pipe local/celld-archive/fleet/cells/Knowledge:test/import.json >/dev/null' <<JSON
{"version":1,"phase":"staging","source_sha256":"$source_hash","attempt_id":"11111111111111111111111111111111","attempt_expires_ms":1,"durable_txid":null}
JSON
celld cell import Knowledge:test --input /archive/source.sqlite \
	--bucket "$BUCKET" --endpoint "$ENDPOINT" --offline --resume
resumed_marker="$(mc 'mc cat local/celld-archive/fleet/cells/Knowledge:test/import.json')"
jq -e '.phase == "ready" and .durable_txid == 1' <<<"$resumed_marker" >/dev/null

# A crash before the owner CAS can leave partial LTX. An expired claimant may
# resume, clears that private epoch, and republishes a fully verified lineage.
mc 'mc pipe local/celld-archive/fleet/cells/Knowledge:partial/import.json >/dev/null' <<JSON
{"version":1,"phase":"staging","source_sha256":"$source_hash","attempt_id":"22222222222222222222222222222222","attempt_expires_ms":1,"durable_txid":null}
JSON
mc 'mc pipe local/celld-archive/fleet/cells/Knowledge:partial/ltx/e1/partial >/dev/null' <<'PARTIAL'
not-an-ltx-file
PARTIAL
celld cell import Knowledge:partial --input /archive/source.sqlite \
	--bucket "$BUCKET" --endpoint "$ENDPOINT" --offline --resume
partial_marker="$(mc 'mc cat local/celld-archive/fleet/cells/Knowledge:partial/import.json')"
jq -e '.phase == "ready" and .durable_txid == 1' <<<"$partial_marker" >/dev/null

# A still-live staging claimant cannot be stolen by a concurrent retry.
future_attempt="$(( $(date +%s) * 1000 + 60000 ))"
mc 'mc pipe local/celld-archive/fleet/cells/Knowledge:active/import.json >/dev/null' <<JSON
{"version":1,"phase":"staging","source_sha256":"$source_hash","attempt_id":"33333333333333333333333333333333","attempt_expires_ms":$future_attempt,"durable_txid":null}
JSON
if celld cell import Knowledge:active --input /archive/source.sqlite \
	--bucket "$BUCKET" --endpoint "$ENDPOINT" --offline --resume >"$TEST_ROOT/active.log" 2>&1; then
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
if celld cell import Knowledge:test --input /archive/different.sqlite \
	--bucket "$BUCKET" --endpoint "$ENDPOINT" --offline >"$TEST_ROOT/different.log" 2>&1; then
	echo 'a different archive unexpectedly replaced the imported lineage' >&2
	exit 1
fi
grep -F 'different archive' "$TEST_ROOT/different.log" >/dev/null

# A live fleet also fails closed before creating a target marker or lineage.
future_ms="$(( $(date +%s) * 1000 + 60000 ))"
mc 'mc pipe local/celld-archive/fleet/nodes/live-test.json >/dev/null' <<JSON
{"node":"live-test","expires_ms":$future_ms,"addr":"127.0.0.1:1","probe_public_key":"x","peer_protocol":1,"ownership_index_generation":"x","load":{"sampled_ms":$future_ms,"resident_cells":0,"host_websockets":0,"rss_bytes":1,"in_use_bytes":null,"cpu_percent_x100":0,"open_fds":0,"fd_limit":1,"pressured":false,"shed_cells":0,"restoring":0}}
JSON
if celld cell import Knowledge:blocked --input /archive/source.sqlite \
	--bucket "$BUCKET" --endpoint "$ENDPOINT" --offline >"$TEST_ROOT/live.log" 2>&1; then
	echo 'import unexpectedly ran while a node lease was live' >&2
	exit 1
fi
grep -F 'live celld node lease(s): live-test' "$TEST_ROOT/live.log" >/dev/null

echo 'cell archive MinIO test passed'
