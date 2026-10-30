#!/usr/bin/env bash
# Idempotent cosign keyless signing of an image digest.
#
# usage: xs-ensure-signed.sh <image@sha256:digest>
# env:   OIDC_ISSUER, CERT_IDENTITY (exact certificate identity)
#
# Verifies first. Signs (recursively, so per-platform manifests are covered)
# only when the registry answers "no signatures found". Any other verify
# failure, including a signature from a different identity, fails.
set -euo pipefail

ref="${1:?image reference with digest required}"
: "${OIDC_ISSUER:?}" "${CERT_IDENTITY:?}"

verify=(cosign verify "${ref}" --certificate-oidc-issuer "${OIDC_ISSUER}"
  --certificate-identity "${CERT_IDENTITY}")

err="$(mktemp)"
trap 'rm -f "${err}"' EXIT

if "${verify[@]}" >/dev/null 2>"${err}"; then
  echo "${ref}: already signed by ${CERT_IDENTITY}"
  exit 0
fi
if ! grep -q 'no signatures found' "${err}"; then
  echo "::error::signature verification failed for ${ref}:"
  cat "${err}"
  exit 1
fi

echo "${ref}: no signatures found, signing"
cosign sign --yes --recursive "${ref}"
"${verify[@]}" >/dev/null
echo "${ref}: signed and verified"
