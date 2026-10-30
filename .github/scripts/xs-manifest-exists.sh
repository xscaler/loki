#!/usr/bin/env bash
# Check whether an image tag exists (needs crane, logged in; gh and GH_TOKEN for ghcr.io).
#
# usage: xs-manifest-exists.sh <ref>
# out:   the manifest digest on stdout when the tag exists.
# exit:  0 exists, 1 absent, 2 any other error.
#
# For ghcr.io references the GitHub Packages API is asked first. A 404 there
# means the package does not exist yet, so the tag is absent. Only then is the
# tag looked up with crane, where only MANIFEST_UNKNOWN and NAME_UNKNOWN count
# as absent. Any other answer is an error.
set -uo pipefail

ref="${1:?image reference required}"
err="$(mktemp)"
trap 'rm -f "${err}"' EXIT

if [[ "${ref}" == ghcr.io/* ]]; then
  path="${ref#ghcr.io/}"
  path="${path%:*}"
  owner="${path%%/*}"
  name="${path#*/}"
  encoded="${name//\//%2F}"
  if ! gh api "orgs/${owner}/packages/container/${encoded}" >/dev/null 2>"${err}"; then
    if grep -q 'HTTP 404' "${err}"; then
      exit 1
    fi
    echo "::error::could not look up package ${owner}/${name}:" >&2
    cat "${err}" >&2
    exit 2
  fi
fi

# One registry call: its output is the digest, its stderr classifies a failure.
if digest="$(crane digest "${ref}" 2>"${err}")"; then
  echo "${digest}"
  exit 0
fi
if grep -Eq 'MANIFEST_UNKNOWN|NAME_UNKNOWN' "${err}"; then
  exit 1
fi
echo "::error::could not check ${ref}:" >&2
cat "${err}" >&2
exit 2
