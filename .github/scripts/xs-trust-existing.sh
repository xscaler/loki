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
# The label check lives in xs-check-revision-labels.sh so CI can exercise it
# against a local registry; the signature and attestation checks need GHCR and
# the GitHub attestation API, but the "no signatures found" rejection is
# exercised locally too.
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

"$(dirname "$0")/xs-check-revision-labels.sh" "${image}" "${digest}" "${commit}" \
  || fail "revision label check failed"

echo "${digest}"
