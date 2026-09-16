# Scheduled data

This Brownie project owns the Resupply and YBS feeds and the existing YBS account/event indexing. It runs inside this directory with its own Python environment. The Flask API never imports Brownie. Git publication, Prisma collection and position monitoring are retired.

Use Python 3.12 with the shared service's patched SQLite library. Install `requirements.lock` with hash verification into `.venv`; keep the API environment separate. Brownie 1.21/Web3 6.11 are pinned here; the API retains Web3 7.3. Brownie requires an `.env` file and the installed `electro` network configuration. Keep credentials out of Git.

Required service settings:

- `YEARN_DB_PATH=/var/lib/yearn/yearn.sqlite3` and the existing `YEARN_IMPORT_SHA256`.
- `LD_LIBRARY_PATH` pointing to the installed patched SQLite library.
- `OPEN_DATA_WORK_DIR=/var/cache/wavey-collectors/data` for disposable names/selectors.
- `YBS_CACHE_DIR=/var/cache/wavey-collectors/joblib` for disposable RPC lookup caches.

The existing `open-data-scripts.service` runs `.venv/bin/python .venv/bin/brownie run run --network electro --raise` every three minutes. `ybs-info.service` runs the same command with `ybs_dash/processes/user_data` every eleven minutes. Both use this directory as their working directory, this environment, and `/usr/bin/flock --exclusive --no-fork /var/lib/yearn/open-data-brownie.lock`. No additional scheduler is needed.

## State and reads

`feed_snapshots` contains the last complete JSON document for each feed. `collector_state` contains each feed's accepted configuration, resume state, attempt status and source history not yet represented in the published document. The existing YBS tables and checkpoints remain authoritative for account/event indexing. No position data is imported.

Network work happens outside SQLite transactions. Each feed publishes its document and cursor together; failures keep both previous values. Independent jobs continue, then the runner exits nonzero if any failed. Historical appends stop at a fixed finalized block. A changed saved hash or configuration fails explicitly; investigate and reconcile the specific state instead of clearing it. Mutable market reads span the reported collection interval.

`/api/resupply/data` and `/api/ybs/data` read SQLite only. They return the last complete document plus `_meta` collection/publication and attempt/success/error fields, even after a failed collection. Missing feeds return 503; successful reads cache for 60 seconds. Source freshness is `collection_finished_at`, which preserves original `last_update` during import. Frontends show stale data after fifteen minutes and retain loaded values on refresh failure.

## One-time import and cutover

Freeze these six files under one private source directory while holding the existing Brownie lock:

```
feeds/resupply_market_data.json
feeds/ybs_data.json
state/withdrawal_feed_cache.json
state/authorizations_cache.json
state/loan_repayment_cache.json
state/ybs_data.json
```

The last file is the old collector working directory's weekly history, not the published YBS feed. Preserve the entire legacy position cache and Git history separately in the one-time offline archive.

Set `OPEN_DATA_RPC_URL` to the installed Ethereum RPC and run from this directory:

```sh
.venv/bin/python import_feeds.py --source /absolute/frozen-inputs --database /absolute/yearn.sqlite3
```

All adopted boundaries must already be finalized. The importer preserves values and inactive history, verifies cursor hashes before writing, adds two tables, and records `open_data_schema_version=1` and a source checksum atomically. It does not change global schema version, original import identity or unrelated tables. An identical rerun is a no-op even after subsequent collection; a different source is refused.

Rehearse against an isolated online database copy first. At cutover, pause both timers, drain active jobs, hold their lock and freeze final inputs. Import only after finality checks pass. Install the API, merged collector and matching backup configuration; change both services' working directories and interpreter paths to this release. Smoke-test API reads before deploying frontends. Release the migration lock, run both service entry points through their normal lock, verify success and resume the two timers.

On failure, hold the affected collector and fix forward while the API serves imported or last-good data. Never restore an old whole-database copy over unrelated writers, restart the old file writer or add a compatibility publisher. Recovery uses the installed `electro-backup` runbook with workers held until reviewed.

Run collector tests from this directory with `.venv/bin/python -m unittest discover -s tests`. Run API tests from the repository root in its separate environment.
