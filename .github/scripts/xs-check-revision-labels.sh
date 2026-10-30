#!/usr/bin/env bash
# Check that every platform image under an index carries the expected commit
# in its org.opencontainers.image.revision label.
#
# usage: xs-check-revision-labels.sh <image> <digest> <commit>
# exit:  0 when both linux/amd64 and linux/arm64 match, 1 otherwise.
set -euo pipefail

image="${1:?image required}"
digest="${2:?digest required}"
commit="${3:?commit required}"

fail() {
  echo "::error::${image}@${digest} exists without trusted provenance ($1): investigate, then bump -xs.n" >&2
  exit 1
}

manifest="$(crane manifest "${image}@${digest}")"
for arch in amd64 arm64; do
  d="$(jq -r --arg a "${arch}" '.manifests[] | select(.platform.os == "linux" and .platform.architecture == $a) | .digest' <<<"${manifest}")"
  [[ "${d}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "no linux/${arch} image"
  rev="$(crane config "${image}@${d}" | jq -r '.config.Labels["org.opencontainers.image.revision"] // ""')"
  [[ "${rev}" == "${commit}" ]] || fail "linux/${arch} revision label is '${rev}', expected ${commit}"
done
