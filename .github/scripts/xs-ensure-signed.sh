#!/usr/bin/env bash
# Idempotent cosign keyless signing (and optional SBOM attestation) of an image digest.
#
# usage: xs-ensure-signed.sh <image@sha256:digest> [<sbom.spdx.json>]
# env:   OIDC_ISSUER, CERT_IDENTITY_REGEXP
#
# Verifies first. Signs only when the registry answers that no signature exists;
# any other verify failure (auth, network, registry error) fails the job.
set -euo pipefail

ref="${1:?image reference with digest required}"
sbom="${2:-}"

: "${OIDC_ISSUER:?}" "${CERT_IDENTITY_REGEXP:?}"

absent_re='no signatures found|no matching signatures|no attestations found|no matching attestations|MANIFEST_UNKNOWN|NAME_UNKNOWN|NOT_FOUND|404'

# ensure <what> <verify-cmd...> -- <create-cmd...>
ensure() {
  local what="$1"
  shift
  local verify=() create=()
  while [[ "$#" -gt 0 && "$1" != "--" ]]; do verify+=("$1"); shift; done
  shift
  create=("$@")

  local err
  err="$(mktemp)"
  if "${verify[@]}" >/dev/null 2>"${err}"; then
    echo "${what}: already present on ${ref}"
    return 0
  fi
  if ! grep -Eq "${absent_re}" "${err}"; then
    echo "::error::${what}: verification failed for a reason other than a missing signature:"
    cat "${err}"
    return 1
  fi
  echo "${what}: not present on ${ref}, creating"
  "${create[@]}"
  "${verify[@]}" >/dev/null
  echo "${what}: created and verified"
}

ensure "signature" \
  cosign verify "${ref}" --certificate-oidc-issuer "${OIDC_ISSUER}" \
  --certificate-identity-regexp "${CERT_IDENTITY_REGEXP}" \
  -- cosign sign --yes "${ref}"

if [[ -n "${sbom}" ]]; then
  ensure "SBOM attestation" \
    cosign verify-attestation "${ref}" --type spdxjson --certificate-oidc-issuer "${OIDC_ISSUER}" \
    --certificate-identity-regexp "${CERT_IDENTITY_REGEXP}" \
    -- cosign attest --yes --type spdxjson --predicate "${sbom}" "${ref}"
fi
