#!/usr/bin/env bash
# Release labels are independent of the checked-out branch, tags and commit.
resolve_release_version() {
  local value=${1:-} repo=$2 token=${GH_TOKEN:-${GITHUB_TOKEN:-}}
  local -a auth=()
  if [[ -n "$token" ]]; then auth=(-H "Authorization: Bearer $token"); fi
  if [[ -z "$value" || "$value" == auto ]]; then
    local response
    response=$(curl -fsSL --retry 3 --connect-timeout 15 --max-time 60 "${auth[@]}" \
      -H 'Accept: application/vnd.github+json' "https://api.github.com/repos/$repo/releases/latest") || return 1
    value=$(jq -er '.tag_name | select(type == "string" and length > 0)' <<< "$response") || return 1
  fi
  value="v${value#v}"
  [[ "$value" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]] || {
    echo "Invalid release version: $value" >&2; return 1;
  }
  printf '%s' "$value"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  VERSION=$(resolve_release_version "${VERSION:-}" "${VERSION_REPO:-OpenListTeam/OpenList}")
  FRONTEND_VERSION=$(resolve_release_version "${FRONTEND_VERSION:-}" "${FRONTEND_REPO:-12189108/OpenList-Frontend}")
  echo "Backend version: $VERSION; frontend release: $FRONTEND_VERSION"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf 'version=%s\nfrontend_version=%s\n' "$VERSION" "$FRONTEND_VERSION" >> "$GITHUB_OUTPUT"
  fi
  if [[ -n "${GITHUB_ENV:-}" ]]; then
    printf 'VERSION=%s\nFRONTEND_VERSION=%s\n' "$VERSION" "$FRONTEND_VERSION" >> "$GITHUB_ENV"
  fi
fi
