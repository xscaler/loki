#!/usr/bin/env bash
# Decide whether an image that already exists under a release tag can be reused.
#
# usage: xs-trust-existing.sh <image> <tag> <commit>
# env:   OIDC_ISSUER, CERT_IDENTITY (exact), REPOSITORY, GH_TOKEN
# out:   prints the digest on stdout when trusted; exits 1 otherwise.
#
# Anyone with a workflow in this repository can write tags of the package, so
# the registry content is never trusted on its own. The image is accepted only
# with a cosign signature and a provenance attestation from the exact release
# identity, and with every platform image labelled with the requested commit.
set -euo pipefail

image="${1:?image required}"
tag="${2:?tag required}"
commit="${3:?commit required}"
: "${OIDC_ISSUER:?}" "${CERT_IDENTITY:?}" "${REPOSITORY:?}"

fail() {
  echo "::error::${image}:${tag} exists without trusted provenance ($1): investigate, then bump -xs.n" >&2
  exit 1
}

digest="$(crane digest "${image}:${tag}")"
ref="${image}@${digest}"

cosign verify "${ref}" --certificate-oidc-issuer "${OIDC_ISSUER}" \
  --certificate-identity "${CERT_IDENTITY}" >/dev/null 2>&1 || fail "no valid signature from ${CERT_IDENTITY}"

gh attestation verify "oci://${ref}" --repo "${REPOSITORY}" \
  --cert-identity "${CERT_IDENTITY}" >/dev/null 2>&1 || fail "no valid provenance attestation"

manifest="$(crane manifest "${ref}")"
for arch in amd64 arm64; do
  d="$(jq -r --arg a "${arch}" '.manifests[] | select(.platform.os == "linux" and .platform.architecture == $a) | .digest' <<<"${manifest}")"
  [[ "${d}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "no linux/${arch} image"
  rev="$(crane config "${image}@${d}" | jq -r '.config.Labels["org.opencontainers.image.revision"] // ""')"
  [[ "${rev}" == "${commit}" ]] || fail "linux/${arch} revision label is '${rev}', expected ${commit}"
done

echo "${digest}"
