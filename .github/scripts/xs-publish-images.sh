#!/usr/bin/env bash
# Push the per-arch image tarballs by digest and create the multi-arch index.
# Shared by the real publish job and the pull request dry run against a local registry.
#
# usage: xs-publish-images.sh <image> <tag> <images-dir>
# out:   digest, amd64 and arm64 lines appended to $GITHUB_OUTPUT (or stdout).
set -euo pipefail

image="${1:?image required}"
tag="${2:?tag required}"
dir="${3:?images dir required}"
metadata="$(mktemp)"
out="${GITHUB_OUTPUT:-/dev/stdout}"

refs=()
for arch in amd64 arm64; do
  tar="${dir}/loki-${arch}/loki-${arch}.tar"
  d="$(crane digest --tarball "${tar}")"
  crane push "${tar}" "${image}@${d}"
  echo "${arch}: ${d}"
  refs+=("${image}@${d}")
done

# Last check before the tag is created; a concurrent publisher must not be overwritten.
rc=0
"$(dirname "$0")/xs-manifest-exists.sh" "${image}:${tag}" || rc=$?
if [[ "${rc}" != "1" ]]; then
  echo "::error::${image}:${tag} exists or could not be checked (rc=${rc}); refusing to create it"
  exit 1
fi

docker buildx imagetools create --metadata-file "${metadata}" --tag "${image}:${tag}" "${refs[@]}"
digest="$(jq -r '."containerimage.descriptor".digest' "${metadata}")"
if [[ ! "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "::error::could not read the index digest from the create output"
  exit 1
fi

manifest="$(crane manifest "${image}@${digest}")"
{
  echo "digest=${digest}"
  for arch in amd64 arm64; do
    d="$(jq -r --arg a "${arch}" '.manifests[] | select(.platform.os == "linux" and .platform.architecture == $a) | .digest' <<<"${manifest}")"
    if [[ ! "${d}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
      echo "::error::no linux/${arch} manifest in ${image}@${digest}" >&2
      exit 1
    fi
    echo "${arch}=${d}"
  done
} >> "${out}"
echo "${image}:${tag} -> ${digest}"
