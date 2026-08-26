#!/usr/bin/env bash
set -euo pipefail

readonly ROOT=/opt/wavey-api
readonly RELEASES=$ROOT/releases
readonly CURRENT=$ROOT/current
readonly CONFIG_ROOT=/etc/wavey-api
readonly ENV_FILE=$CONFIG_ROOT/proposal-trace.env
readonly UNIT=proposal-trace-api.service
readonly STATE_ROOT=/var/lib/proposal-trace
readonly DATABASE=$STATE_ROOT/state.db
readonly READER_GROUP=proposal-trace-reader
readonly SERVICE_USER=wavey-api

if (( $# != 4 )); then
  echo "remote deployment requires archive, digest, commit, and bootstrap fields" >&2
  exit 2
fi
readonly ARCHIVE=$1
readonly EXPECTED_DIGEST=$2
readonly COMMIT=$3
readonly BOOTSTRAP=$4
if [[ ! $ARCHIVE =~ ^/var/tmp/wavey-api-release\.[A-Za-z0-9]+\.upload$ ||
  ! $EXPECTED_DIGEST =~ ^[0-9a-f]{64}$ || ! $COMMIT =~ ^[0-9a-f]{40}$ ||
  ($BOOTSTRAP != "" && $BOOTSTRAP != --bootstrap) ]]; then
  echo "remote deployment metadata is invalid" >&2
  exit 2
fi

release=
previous=
cut_over=false
cleanup() {
  status=$?
  rm -f -- "$ARCHIVE"
  if (( status != 0 )); then
    if [[ $cut_over == true ]]; then
      if [[ -n $previous ]]; then
        replacement="$CURRENT.rollback.$$"
        ln -s -- "$previous" "$replacement"
        mv -Tf -- "$replacement" "$CURRENT"
        systemctl restart "$UNIT" >/dev/null 2>&1 || true
      else
        systemctl disable --now "$UNIT" >/dev/null 2>&1 || true
        rm -f -- "$CURRENT"
      fi
    fi
    if [[ -n $release && -d $release ]]; then
      rm -rf -- "$release"
    fi
  fi
}
trap cleanup EXIT

actual=$(sha256sum "$ARCHIVE" | awk '{print $1}')
if [[ $actual != "$EXPECTED_DIGEST" ]]; then
  echo "release archive digest mismatch" >&2
  exit 1
fi
if [[ $BOOTSTRAP == --bootstrap ]]; then
  if ! getent group "$READER_GROUP" >/dev/null; then
    groupadd --system "$READER_GROUP"
  fi
  if ! getent passwd "$SERVICE_USER" >/dev/null; then
    useradd --system --user-group --home-dir /var/lib/wavey-api \
      --create-home --shell /usr/sbin/nologin "$SERVICE_USER"
  fi
  usermod --append --groups "$READER_GROUP" "$SERVICE_USER"
  install -d -m 0755 -o root -g root "$ROOT" "$RELEASES" "$CONFIG_ROOT"
  printf '%s\n' \
    'PROPOSAL_TRACE_DB_PATH=/var/lib/proposal-trace/state.db' \
    'PROPOSAL_TRACE_BUSY_TIMEOUT_MS=5000' \
    >"$ENV_FILE"
  chown root:root "$ENV_FILE"
  chmod 0644 "$ENV_FILE"
elif ! getent passwd "$SERVICE_USER" >/dev/null ||
  ! getent group "$READER_GROUP" >/dev/null || [[ ! -f $ENV_FILE ]]; then
  echo "Proposal Trace API has not been bootstrapped" >&2
  exit 1
fi

if [[ ! -f $DATABASE || -L $DATABASE || ! -d $STATE_ROOT || -L $STATE_ROOT ]]; then
  echo "Proposal Trace database is unavailable" >&2
  exit 1
fi
chown proposal-trace:"$READER_GROUP" "$STATE_ROOT" "$DATABASE"
chmod 0710 "$STATE_ROOT"
chmod 0640 "$DATABASE"
if id -nG "$SERVICE_USER" | tr ' ' '\n' | grep -Fx proposal-trace >/dev/null; then
  echo "API service account must not join the Proposal Trace service group" >&2
  exit 1
fi

version="$(date -u +%Y%m%d%H%M%S)-${COMMIT:0:12}"
release="$RELEASES/$version"
mkdir "$release"
tar -xzf "$ARCHIVE" -C "$release" --no-same-owner --no-same-permissions
python3 -m venv "$release/.venv"
"$release/.venv/bin/pip" install --disable-pip-version-check --no-input \
  --requirement "$release/requirements-proposal-trace.txt"
chown -R root:root "$release"
chmod -R a-w "$release"

if ! (
  cd "$release"
  runuser -u "$SERVICE_USER" -- env \
    PROPOSAL_TRACE_DB_PATH="$DATABASE" PROPOSAL_TRACE_BUSY_TIMEOUT_MS=5000 \
    "$release/.venv/bin/python" -c \
    'from services.proposal_trace import ProposalTraceService; import sys; result=ProposalTraceService(sys.argv[1]).list_audits("curve:ownership", "1"); assert result["contract_version"] == 1' \
    "$DATABASE"
); then
  echo "Proposal Trace API read-only smoke check failed" >&2
  exit 1
fi

if [[ -L $CURRENT ]]; then
  previous=$(readlink -f "$CURRENT")
elif [[ -e $CURRENT ]]; then
  echo "$CURRENT is not a symlink" >&2
  exit 1
fi
install -m 0644 -o root -g root "$release/deploy/systemd/$UNIT" "/etc/systemd/system/$UNIT"
systemctl daemon-reload
replacement="$CURRENT.new.$$"
ln -s -- "$release" "$replacement"
mv -Tf -- "$replacement" "$CURRENT"
cut_over=true
systemctl enable "$UNIT"
systemctl restart "$UNIT"
systemctl is-active --quiet "$UNIT"
if ! curl --fail --silent --show-error --max-time 10 \
  'http://127.0.0.1:3101/api/proposal-trace/curve/ownership/audits?limit=1' \
  | "$release/.venv/bin/python" -c \
    'import json,sys; value=json.load(sys.stdin); assert value["contract_version"] == 1 and len(value["audits"]) <= 1'; then
  echo "Proposal Trace API HTTP smoke check failed" >&2
  exit 1
fi
cut_over=false
echo "Proposal Trace API release $version is active on 127.0.0.1:3101"
