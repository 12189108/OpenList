#!/usr/bin/env bash
# Build the checked-out backend; version inputs label artifacts, never reset code.
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
VERSION=${VERSION:-}
VERSION_REPO=${VERSION_REPO:-OpenListTeam/OpenList}
FRONTEND_REPO=${FRONTEND_REPO:-12189108/OpenList-Frontend}
FRONTEND_VERSION=${FRONTEND_VERSION:-}
FRONTEND_VERSION_REPO=${FRONTEND_VERSION_REPO:-OpenListTeam/OpenList-Frontend}
FRONTEND_MODE=${FRONTEND_MODE:-source}
FRONTEND_REF=${FRONTEND_REF:-main}
FRONTEND_SOURCE_DIR=${FRONTEND_SOURCE_DIR:-}
TARGETS=${TARGETS:-linux/amd64 linux/arm64 windows/amd64 windows/arm64}
OUTPUT_DIR=${OUTPUT_DIR:-$ROOT/build}
export GH_TOKEN=${GH_TOKEN:-${GITHUB_TOKEN:-}}

usage() {
  cat <<'EOF'
Usage: bash scripts/build-custom.sh [options]
  --version VERSION             Backend label; empty/auto = latest stable upstream
  --version-repo OWNER/REPO      Backend version lookup repository
  --frontend-mode source|release Source build (default) or published frontend archive
  --frontend-repo OWNER/REPO     Default: 12189108/OpenList-Frontend
  --frontend-ref REF             Source branch, tag or SHA; default: main
  --frontend-version VERSION    Label for source mode; release tag for release mode
  --frontend-version-repo REPO   Source-mode version lookup; default: official frontend
  --frontend-source-dir DIR      Existing source checkout, or path to clone into
  --targets "OS/ARCH ..."        Default: linux/amd64 linux/arm64 windows/amd64 windows/arm64
  --output-dir DIR              Default: build
Requires Bash, gh, jq, git, Go, tar and target C/C++ compilers.
Source mode also requires Node and pnpm matching the frontend packageManager.
EOF
}
while (($#)); do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --version|--version-repo|--frontend-mode|--frontend-repo|--frontend-ref|--frontend-version|--frontend-version-repo|--frontend-source-dir|--targets|--output-dir)
      (($# >= 2)) || { echo "Missing value for $1" >&2; exit 1; }
      case "$1" in
        --version) VERSION=$2 ;;
        --version-repo) VERSION_REPO=$2 ;;
        --frontend-mode) FRONTEND_MODE=$2 ;;
        --frontend-repo) FRONTEND_REPO=$2 ;;
        --frontend-ref) FRONTEND_REF=$2 ;;
        --frontend-version) FRONTEND_VERSION=$2 ;;
        --frontend-version-repo) FRONTEND_VERSION_REPO=$2 ;;
        --frontend-source-dir) FRONTEND_SOURCE_DIR=$2 ;;
        --targets) TARGETS=$2 ;;
        --output-dir) OUTPUT_DIR=$2 ;;
      esac
      shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

source "$ROOT/scripts/resolve-versions.sh"
VERSION=$(resolve_release_version "$VERSION" "$VERSION_REPO")
mkdir -p "$OUTPUT_DIR" "$ROOT/.build"
STAGING=$(mktemp -d "$ROOT/.build/frontend-assets.XXXXXX")
trap 'rm -rf -- "$STAGING"' EXIT

case "$FRONTEND_MODE" in
  source)
    FRONTEND_VERSION=$(resolve_release_version "$FRONTEND_VERSION" "$FRONTEND_VERSION_REPO")
    FRONTEND_SOURCE_DIR=${FRONTEND_SOURCE_DIR:-$STAGING/source}
    if [[ ! -d "$FRONTEND_SOURCE_DIR" ]]; then
      mkdir -p "$FRONTEND_SOURCE_DIR"
      git -C "$FRONTEND_SOURCE_DIR" init -q
      git -C "$FRONTEND_SOURCE_DIR" remote add origin "https://github.com/$FRONTEND_REPO.git"
      git -C "$FRONTEND_SOURCE_DIR" fetch --depth=1 origin "$FRONTEND_REF"
      git -C "$FRONTEND_SOURCE_DIR" checkout --detach FETCH_HEAD
    fi
    FRONTEND_COMMIT=$(git -C "$FRONTEND_SOURCE_DIR" rev-parse HEAD 2>/dev/null || echo unknown)
    # Existing checkouts are used as-is, including local feature changes.
    (
      cd "$FRONTEND_SOURCE_DIR"
      cp package.json "$STAGING/package.json"
      trap 'cp "$STAGING/package.json" package.json' EXIT
      node --input-type=commonjs -e 'const fs=require("fs"); const p=JSON.parse(fs.readFileSync("package.json","utf8")); p.version=process.argv[1]; fs.writeFileSync("package.json",JSON.stringify(p,null,2)+"\n")' "${FRONTEND_VERSION#v}"
      pnpm install --frozen-lockfile
      gh release download --repo OpenListTeam/OpenList-Frontend --pattern i18n.tar.gz --dir "$STAGING"
      mkdir -p src/lang
      tar -xzf "$STAGING/i18n.tar.gz" -C src/lang
      node scripts/i18n.mjs
      pnpm build
      printf '%s' "$FRONTEND_VERSION" > dist/VERSION
      tar -cf "$STAGING/frontend.tar" -C dist .
    )
    ;;
  release)
    FRONTEND_VERSION=$(resolve_release_version "$FRONTEND_VERSION" "$FRONTEND_REPO")
    FRONTEND_COMMIT="release:$FRONTEND_VERSION"
    gh release download "$FRONTEND_VERSION" --repo "$FRONTEND_REPO" \
      --pattern "openlist-frontend-dist-$FRONTEND_VERSION.tar.gz" --dir "$STAGING"
    mkdir "$STAGING/dist"
    tar -xzf "$STAGING/openlist-frontend-dist-$FRONTEND_VERSION.tar.gz" -C "$STAGING/dist"
    tar -cf "$STAGING/frontend.tar" -C "$STAGING/dist" .
    ;;
  *) echo "Invalid frontend mode: $FRONTEND_MODE" >&2; exit 1 ;;
esac
# Replace the embedded bundle so files removed by the new frontend cannot linger.
rm -rf -- "$ROOT/public/dist"
mkdir -p "$ROOT/public/dist"
tar -xf "$STAGING/frontend.tar" -C "$ROOT/public/dist"
[[ -f public/dist/index.html && -f public/dist/VERSION ]] || { echo "Frontend bundle is incomplete" >&2; exit 1; }
[[ "$(cat public/dist/VERSION)" == "$FRONTEND_VERSION" ]] || { echo "Frontend VERSION does not match the selected version" >&2; exit 1; }

BUILT_AT=$(date +'%F %T %z')
GIT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo unknown)
LDFLAGS="-s -w -X 'github.com/OpenListTeam/OpenList/v4/internal/conf.BuiltAt=$BUILT_AT' -X 'github.com/OpenListTeam/OpenList/v4/internal/conf.GitAuthor=The OpenList Projects Contributors <noreply@oplist.org>' -X 'github.com/OpenListTeam/OpenList/v4/internal/conf.GitCommit=$GIT_COMMIT' -X 'github.com/OpenListTeam/OpenList/v4/internal/conf.Version=$VERSION' -X 'github.com/OpenListTeam/OpenList/v4/internal/conf.WebVersion=$FRONTEND_VERSION'"
echo "Compiling backend $VERSION with frontend $FRONTEND_REPO $FRONTEND_VERSION ($FRONTEND_MODE)"
read -r -a selected_targets <<< "$TARGETS"
((${#selected_targets[@]})) || { echo "No build targets selected" >&2; exit 1; }
for target in "${selected_targets[@]}"; do
  case "$target" in
    linux/amd64) cc=gcc; cxx=g++ ;;
    linux/arm64) cc=aarch64-linux-gnu-gcc; cxx=aarch64-linux-gnu-g++ ;;
    windows/amd64) cc=x86_64-w64-mingw32-gcc; cxx=x86_64-w64-mingw32-g++ ;;
    windows/arm64) cc="$ROOT/wrapper/zcc-arm64"; cxx="$ROOT/wrapper/zcxx-arm64" ;;
    *) echo "Unsupported target: $target" >&2; exit 1 ;;
  esac
  output="$OUTPUT_DIR/openlist-${target%/*}-${target#*/}"
  if [[ "$target" == windows/* ]]; then output+=.exe; fi
  CGO_ENABLED=1 GOOS="${target%/*}" GOARCH="${target#*/}" CC="$cc" CXX="$cxx" \
    go build -trimpath -o "$output" -ldflags="$LDFLAGS" -tags=jsoniter .
done
jq -n --arg version "$VERSION" --arg commit "$GIT_COMMIT" --arg built_at "$BUILT_AT" \
  --arg frontend_repo "$FRONTEND_REPO" --arg frontend_version "$FRONTEND_VERSION" \
  --arg frontend_mode "$FRONTEND_MODE" --arg frontend_ref "$FRONTEND_REF" \
  --arg frontend_commit "$FRONTEND_COMMIT" --arg targets "$TARGETS" \
  '$ARGS.named' > "$OUTPUT_DIR/build-info.json"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf 'version=%s\nfrontend_version=%s\n' "$VERSION" "$FRONTEND_VERSION" >> "$GITHUB_OUTPUT"
fi
echo "Build output: $OUTPUT_DIR"
