#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

: "${OPEN_SSPM_SPEC_REPO:?OPEN_SSPM_SPEC_REPO is required (local path or git URL)}"
: "${OPEN_SSPM_SPEC_REF:?OPEN_SSPM_SPEC_REF is required (tag/branch/commit)}"

tmp_dir="$(mktemp -d)"
cleanup() {
	rm -rf "${tmp_dir}"
}
trap cleanup EXIT

spec_dir="${tmp_dir}/open-sspm-spec"

echo "==> Cloning ${OPEN_SSPM_SPEC_REPO}..."
git clone "${OPEN_SSPM_SPEC_REPO}" "${spec_dir}" >/dev/null

echo "==> Checking out ${OPEN_SSPM_SPEC_REF}..."
git -C "${spec_dir}" fetch --all --tags --prune >/dev/null
git -C "${spec_dir}" checkout --quiet "${OPEN_SSPM_SPEC_REF}"

echo "==> Validating spec..."
(cd "${spec_dir}" && go run ./tools/osspec/cmd/osspec validate --repo .)

echo "==> Building compiled artifacts..."
(cd "${spec_dir}" && go run ./tools/osspec/cmd/osspec build --repo . --out dist)

assets_dir="${ROOT_DIR}/internal/opensspm/specassets"
mkdir -p "${assets_dir}"

echo "==> Copying descriptor snapshot..."
cp "${spec_dir}/dist/descriptor.v1.json" "${assets_dir}/descriptor.v1.json"

if [[ -f "${spec_dir}/dist/index/requirements.json" ]]; then
	echo "==> Copying requirements snapshot..."
	cp "${spec_dir}/dist/index/requirements.json" "${assets_dir}/requirements.json"
fi

upstream_commit="$(git -C "${spec_dir}" rev-parse HEAD)"

echo "==> Pinning Go types dependency..."
(cd "${ROOT_DIR}" && go get "github.com/open-sspm/open-sspm-spec@${OPEN_SSPM_SPEC_REF}")

echo "==> Updating spec.lock.json..."
python3 - "${assets_dir}" "${OPEN_SSPM_SPEC_REPO}" "${OPEN_SSPM_SPEC_REF}" "${upstream_commit}" <<'PY'
import datetime
import hashlib
import json
import pathlib
import sys

assets_dir = pathlib.Path(sys.argv[1])
upstream_repo = sys.argv[2]
upstream_ref = sys.argv[3]
upstream_commit = sys.argv[4]

descriptor_path = assets_dir / "descriptor.v1.json"
descriptor_hash = hashlib.sha256(descriptor_path.read_bytes()).hexdigest()

lock = {
    "upstream_repo": upstream_repo,
    "upstream_ref": upstream_ref,
    "upstream_commit": upstream_commit,
    "descriptor_hash": descriptor_hash,
    "descriptor_hash_algorithm": "sha256",
    "updated_at": datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
}

(assets_dir / "spec.lock.json").write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")
print(f"descriptor_hash={descriptor_hash}")
PY

echo "==> Done."

