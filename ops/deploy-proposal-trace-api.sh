#!/usr/bin/env bash
set -euo pipefail

if [[ ${1:-} != claw || $# -lt 1 || $# -gt 2 ||
  (${2:-} != "" && ${2:-} != --bootstrap) ]]; then
  echo "usage: ops/deploy-proposal-trace-api.sh claw [--bootstrap]" >&2
  exit 2
fi

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
HOST=${WAVEY_API_DEPLOY_HOST:-claw}
BOOTSTRAP=${2:-}
if [[ $HOST != claw ]]; then
  echo "Proposal Trace API deployment is restricted to Claw" >&2
  exit 2
fi
if [[ -n $(git -C "$ROOT" status --porcelain) ]]; then
  echo "deployment requires a clean worktree" >&2
  exit 1
fi
if [[ $(git -C "$ROOT" branch --show-current) != master ]]; then
  echo "deployment requires the master branch" >&2
  exit 1
fi
git -C "$ROOT" fetch --quiet origin master
COMMIT=$(git -C "$ROOT" rev-parse HEAD)
if [[ $COMMIT != $(git -C "$ROOT" rev-parse origin/master) ]]; then
  echo "deployment requires the exact origin/master commit" >&2
  exit 1
fi

python3 -m pytest -q
ARCHIVE=$(mktemp /tmp/wavey-api-release.XXXXXXXX.tar.gz)
REMOTE_ARCHIVE=
cleanup() {
  rm -f -- "$ARCHIVE"
  if [[ -n $REMOTE_ARCHIVE ]]; then
    ssh "$HOST" "rm -f -- '$REMOTE_ARCHIVE'" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
git -C "$ROOT" archive --format=tar.gz --output="$ARCHIVE" HEAD
DIGEST=$(shasum -a 256 "$ARCHIVE" | awk '{print $1}')
REMOTE_ARCHIVE=$(ssh "$HOST" 'mktemp /var/tmp/wavey-api-release.XXXXXX.upload')
scp -q "$ARCHIVE" "$HOST:$REMOTE_ARCHIVE"
ssh "$HOST" "set -eu
  chmod 0600 '$REMOTE_ARCHIVE'
  sudo -n /bin/bash -s -- '$REMOTE_ARCHIVE' '$DIGEST' '$COMMIT' '$BOOTSTRAP'" \
  <"$ROOT/ops/deploy-proposal-trace-api-remote.sh"
REMOTE_ARCHIVE=
echo "Proposal Trace API deployed to Claw"
