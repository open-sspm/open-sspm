#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

: "${OPEN_SSPM_SPEC_REPO:=https://github.com/open-sspm/open-sspm-spec}"
: "${OPEN_SSPM_SPEC_REF:?OPEN_SSPM_SPEC_REF is required (tag/branch/commit)}"

normalize_repo_url() {
	local repo="$1"

	if [[ "${repo}" =~ ^git@([^:]+):(.+)$ ]]; then
		repo="https://${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
	fi
	repo="${repo#ssh://git@}"
	repo="${repo#git://}"
	repo="${repo%.git}"
	printf '%s\n' "${repo}"
}

canonical_repo="$(normalize_repo_url "${OPEN_SSPM_SPEC_REPO}")"
if [[ ! "${canonical_repo}" =~ ^https://github\.com/[^/]+/[^/]+$ ]]; then
	echo "error: OPEN_SSPM_SPEC_REPO must be a GitHub repository URL, got ${OPEN_SSPM_SPEC_REPO}" >&2
	exit 1
fi
if [[ "${canonical_repo}" != "https://github.com/open-sspm/open-sspm-spec" ]]; then
	echo "error: OPEN_SSPM_SPEC_REPO must be https://github.com/open-sspm/open-sspm-spec" >&2
	exit 1
fi

tmp_dir="$(mktemp -d)"
cleanup() {
	rm -rf "${tmp_dir}"
}
trap cleanup EXIT

spec_dir="${tmp_dir}/open-sspm-spec"

echo "==> Cloning ${canonical_repo}..."
git clone "${canonical_repo}" "${spec_dir}" >/dev/null

echo "==> Checking out ${OPEN_SSPM_SPEC_REF}..."
git -C "${spec_dir}" fetch --all --tags --prune >/dev/null
git -C "${spec_dir}" checkout --quiet "${OPEN_SSPM_SPEC_REF}"

echo "==> Validating spec..."
(cd "${spec_dir}" && go run ./tools/osspec/cmd/osspec validate --repo .)

echo "==> Building compiled artifacts..."
(cd "${spec_dir}" && go run ./tools/osspec/cmd/osspec build --repo . --out dist)

assets_dir="${ROOT_DIR}/internal/opensspm/specassets"
mkdir -p "${assets_dir}"

rm -f "${assets_dir}/descriptor.v1.json" "${assets_dir}/requirements.json" "${assets_dir}/descriptor.v1.yaml" "${assets_dir}/requirements.yaml" "${assets_dir}/descriptor.v2.yaml"

upstream_commit="$(git -C "${spec_dir}" rev-parse HEAD)"
upstream_repo="${canonical_repo}"

descriptor_dist_path="${spec_dir}/dist/descriptor.v2.yaml"
if [[ ! -f "${descriptor_dist_path}" ]]; then
	echo "error: expected compiled descriptor at ${descriptor_dist_path}" >&2
	exit 1
fi

echo "==> Copying descriptor snapshot..."
cp "${descriptor_dist_path}" "${assets_dir}/descriptor.v2.yaml"

echo "==> Pinning Go types dependency..."
(cd "${ROOT_DIR}" && go get "github.com/open-sspm/open-sspm-spec@${OPEN_SSPM_SPEC_REF}")

echo "==> Updating spec.lock.json..."
python3 - "${assets_dir}" "${upstream_repo}" "${OPEN_SSPM_SPEC_REF}" "${upstream_commit}" "${assets_dir}/descriptor.v2.yaml" <<'PY'
import datetime
import hashlib
import json
import pathlib
import sys

assets_dir = pathlib.Path(sys.argv[1])
upstream_repo = sys.argv[2]
upstream_ref = sys.argv[3]
upstream_commit = sys.argv[4]
descriptor_asset_path = pathlib.Path(sys.argv[5])

descriptor_hash = hashlib.sha256(descriptor_asset_path.read_bytes()).hexdigest()

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
