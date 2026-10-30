#!/usr/bin/env bash
# Check whether an image reference exists in a registry (needs crane, logged in).
#
# usage: xs-manifest-exists.sh <ref>
# exit:  0 exists, 1 absent, 2 any other error.
# Only MANIFEST_UNKNOWN and NAME_UNKNOWN count as absent.
set -uo pipefail

ref="${1:?image reference required}"
err="$(mktemp)"
trap 'rm -f "${err}"' EXIT

if crane manifest "${ref}" >/dev/null 2>"${err}"; then
  exit 0
fi
if grep -Eq 'MANIFEST_UNKNOWN|NAME_UNKNOWN' "${err}"; then
  exit 1
fi
echo "::error::could not check ${ref}:" >&2
cat "${err}" >&2
exit 2
