# perk-redis

A Redis database plugin for [perk-workbench](https://github.com/l3aro/perk-workbench),
speaking the perk/v1 protocol: JSON-RPC 2.0 over newline-delimited JSON on
stdin/stdout. The workbench spawns it, handshakes it, and registers it as
the **Redis (Rust plugin)** driver, indistinguishable from a compiled-in
driver.

The plugin opens plain **TCP** Redis targets only — `redis://` with no TLS.
TLS (`rediss://`) and non-TCP targets are rejected at open. There is no
TLS option in the connection form.

## Prerequisites

- **Docker** with the Compose v2 plugin (for `docker compose up -d
  --wait`; v2.17+).
- **Rust toolchain** (edition 2024; `rustc`/`cargo` 1.85+). `--locked`
  everywhere: the lockfile pins the exact dependency set.
- **The built workbench host**: `../source/perk-workbench` (the sibling
  checkout of perk-workbench), or any `perk-workbench` binary via the
  `PERK_WORKBENCH` environment variable. Build it in the source project
  with `go build ./cmd/perk-workbench`.

## Reproducible demo

The fixture, seed, plugin build, temporary config, and TUI launch are
wrapped in one command:

```bash
scripts/run-workbench-demo.sh
```

It builds `target/debug/perk-redis` with the lockfile, starts the Redis
fixture and waits for it to be healthy, seeds logical database 2, writes a
throwaway `$XDG_CONFIG_HOME/perk-workbench/config.json` listing the plugin
executable, and launches the TUI directly at
`redis://:workbench-demo@127.0.0.1:6380/2`. On exit it stops the fixture
and removes the temporary config. All script messages go to stderr; the
plugin's stdout carries only protocol frames.

### Step by step

1. Start the fixture and wait until its healthcheck passes:

   ```bash
   docker compose up -d --wait
   ```

   The fixture is the official `redis:7-alpine` image, published only
   on `127.0.0.1:6380`, with `requirepass workbench-demo`, append-only
   persistence disabled, and an authenticated `redis-cli ping`
   healthcheck.

2. Seed logical database 2 (idempotent — safe to re-run; it flushes and
   rewrites the same snapshot):

   ```bash
   scripts/seed-demo.sh
   ```

   This creates: `greeting` (string), `product` (string), `user:1`
   (hash), `queue:jobs` (list), `tags:demo` (set), and
   `leaderboard:demo` (sorted set).

3. Build and test the plugin (unit and transport tests; the integration
   test needs a live server, see below):

   ```bash
   cargo build --locked
   cargo test --locked
   ```

4. Tell the workbench to load the plugin. `plugins` in
   `$XDG_CONFIG_HOME/perk-workbench/config.json` is an **explicit
   allowlist** — the workbench spawns exactly the executables listed
   there and never auto-discovers anything. A temporary config for this
   checkout looks like:

   ```json
   {"plugins":["/absolute/path/to/perk-redis/target/debug/perk-redis"]}
   ```

   (Exact shape: one JSON object; `plugins` is an array of executable
   paths. Paths containing a separator are used as-is — absolute paths
   directly, relative ones resolved against the config file's directory.
   A bare name resolves through `PATH`. A symlink to the built plugin is
   a deployment convenience for pointing the allowlist at a stable
   path — it never causes discovery; only allowlisted entries are ever
   spawned.)

   Launch the workbench directly at the fixture, using a temporary
   `XDG_CONFIG_HOME` so the real user config is untouched:

   ```bash
   export XDG_CONFIG_HOME=$(mktemp -d)
   mkdir -p "$XDG_CONFIG_HOME/perk-workbench"
   printf '{"plugins":["%s"]}\n' "$PWD/target/debug/perk-redis" > \
     "$XDG_CONFIG_HOME/perk-workbench/config.json"
   ../source/perk-workbench 'redis://:workbench-demo@127.0.0.1:6380/2'
   ```

   In the TUI, the connection opens straight into the ready state with
   the status bar reporting the Redis product and version (e.g.
   `Redis 7.4.7`). The schema sidebar renders no items for this plugin
   (see [Schema browsing](#schema-browsing)). The query editor tab is
   labeled **Command** (the plugin advertises `query_language` with
   name `Redis`, a concrete command placeholder, and example
   statements) but the initial focus is the schema pane — press `2`
   (focus workspace) to type into the editor.

## Trying the plugin in the TUI

- **PING** — type `PING`, press `F5` (or `Ctrl+Enter`). The result table
  shows `PONG`.
- **GET greeting** — press `F5`; the seeded greeting value is returned.
- **HGETALL user:1** — the seeded hash rows (`name`, `email`, `role`).
- **SCAN 0** — one row per key in database 2.
- **`SELECT * FROM "keys" LIMIT 25 OFFSET 0`** — the host-generated
  browse statement is accepted through the SQL editor exactly like the
  browse RPC: `execute`, `execute_read_only`, and `validate` route
  `SELECT * FROM "keys" [LIMIT n] [OFFSET m]` (quoted or bare table,
  either clause order, optional trailing `;`) to the same paged keys
  browse. Other `SELECT * FROM ...` shapes (unknown tables, extra
  clauses, malformed quoting) stay operation errors, and native Redis
  commands — including the `SELECT <db>` connection command — are
  unchanged.

Quit with `Ctrl+C` (or `Ctrl+Q` → Quit when the SQL editor holds text).

## Row writing

The plugin advertises `write_capabilities.row_writer` and serves
`perk/v1/row_write` over the virtual `keys` table (covered by
`tests/redis_integration.rs`). One row is one key in the selected logical
database; `rows_affected` reports the real count.

- **Insert** — an explicit `key` plus a `value` creates a Redis string
  with `SET ... NX`: an existing key is rejected and nothing is
  overwritten. The optional `type` column accepts only `string` (blank
  defaults to string); collection types are rejected. A missing `value`
  inserts the empty string.
- **Update** — the `value` column replaces the complete string with
  `SET`, but only for an existing string whose full value fits the
  workbench's 300-rune display cell (valid UTF-8, at most 300 Unicode
  scalar values). Larger strings, non-UTF-8 blobs, and hash/list/set/zset
  values are rejected before anything is written: a bounded preview can
  never overwrite a value it did not fully show. Editing the `key`
  column renames the existing key (`RENAME` semantics: empty or colliding
  destinations are rejected; renaming to the same name is a successful
  no-op). `type` is immutable. A combined rename + value change
  validates first, then applies atomically in one Lua script — a
  collision or concurrent change aborts the whole update with no partial
  mutation.
- **Delete** — `DEL` the identified key; `rows_affected` is the actual
  0/1 deletion count.

Every successful row write reports the **exact native Redis command that
was executed** as the wire `statement`, always paired with
`statement_metadata` (`{language: "redis", replayable, sensitive}`).
Inserts log `SET <key> <value> NX`, deletes log `DEL <key>`, and updates
log `EVAL <script> 1 <key> <dst> <want> <expected> <new>` — the shared
atomic update script with its exact keys and arguments, never a simpler
`RENAME`/`SET` whose effects (e.g. overwriting a colliding destination)
would differ from the guarded operation. Every token is shell-quoted, so
keys and values containing spaces, quotes, backslashes, or newlines
survive the round trip.

Sensitivity is conservative: statements that embed a value (inserts,
value edits, and any native command carrying a payload, credential, or
script — `SET`/`HSET`/`LPUSH`/…, `AUTH`, `HELLO AUTH`, `ACL SETUSER`,
`CONFIG` password/auth settings, `MIGRATE AUTH`, `EVAL`, unknown/module
commands) are flagged **sensitive** and **non-replayable**: the host
redacts the text and never stores it verbatim. Key-only statements
(`DEL`, key renames, reads, the virtual browse SELECT) stay
**non-sensitive** and **replayable**: they can be pasted into Workbench
Execute and replayed verbatim. There are no descriptive
`Table:`/`Key:`/`Changes:` previews.

Every successful `execute`/`execute_read_only` result reports the exact
command the plugin accepted (re-rendered from the parsed tokens) as
`statement` with the same metadata; every `browse_table` result reports
the exact pseudo-command (`SELECT * FROM "keys" [LIMIT n] [OFFSET m]`)
that `execute` replays.

Service errors carry structured provenance on the wire:
`error.data = {"kind", "plugin": "redis", "method"}` with the stable
kinds `validation`, `authentication`, `connection`, `operation`,
`unsupported`, `cancelled` (mirroring the perk/v1 contract). Redis
credential failures map to `authentication`, connection/open failures
to `connection`, parse/params/row-write input to `validation`,
unsupported tables/methods/schema mutations to `unsupported`,
cancellation to `cancelled`, and everything else to `operation`; the
advisory `method` is the actual wire method, rendered exactly once.

In the current host the row forms live on the Browse tab, which cannot
open `keys` because of the schema-sidebar limitation below, so the TUI
path for writes is still the SQL editor (`SET`/`DEL`/`RENAME`). The
row-write RPC itself is exercised end-to-end by the integration tests;
collection **value** editing is deliberately unsupported.

## Schema browsing

The plugin's `perk/v1/list_schema` serves exactly one virtual table —
`keys` under database `db2` — with key, type, and value-preview columns,
and `browse_table` pages over it (covered by
`tests/redis_integration.rs::virtual_keys_schema_and_browse_paging`).
The **current host does not render that response in the schema
sidebar**, and no perk/v1 field or host option can change that without
modifying the host:

- `internal/workbench/schema/model.go` `RebuildTree` creates sidebar
  roots only from schema objects of type `database`; table objects are
  emitted only under an expanded database root.
- `ExpandedDatabases` is populated only from `database`-type objects
  (`initialDatabaseExpansion`), so a lone `{type: "table"}` object is
  skipped entirely.
- `database.Open` and the app's `updateOpen` pass `list_schema` results
  through unchanged — no root synthesis exists.

With the exact one-object contract the sidebar therefore renders
`No items.` (PTY-verified) and `keys` cannot be opened from the TUI.
Queries, the result table, and the query log are unaffected; the browse
RPC itself is exercised by the integration tests above.

### Connection form

Launch the workbench without a target (`perk-workbench` with the plugin
config installed) and the connection form's **Driver** select offers
**Redis (Rust plugin)** alongside the built-in drivers. Values for this
fixture:

| Field | Value |
|---|---|
| Host | `127.0.0.1` |
| Port | `6380` |
| Username | *(blank)* |
| Password | `workbench-demo` |
| Database | `2` |

## Integration tests

The real Redis-backed service is covered by
`tests/redis_integration.rs`, which needs a live server and skips
otherwise. With the fixture up and seeded:

```bash
REDIS_URL=redis://:workbench-demo@127.0.0.1:6380/2 \
  cargo test --locked --test redis_integration
```

## Tearing down

```bash
docker compose down --volumes
```

Stops and removes the fixture container and any of its volumes, leaving
nothing listening on `127.0.0.1:6380`.

## Continuous integration

`.github/workflows/compatibility.yml` runs on every push, pull request,
and manual `workflow_dispatch`. It checks formatting, clippy, and unit
tests; builds the plugin with the lockfile; starts and seeds the
loopback Compose fixture and runs the full integration suite against it;
then builds the latest `l3aro/perk-workbench` default branch and runs
its plugin conformance suite (`plugin test --json`, 16 transport cases)
against the built plugin. The fixture is torn down unconditionally when
the job ends.

The workflow is a **drift canary**: it validates against the current
default branch of the host rather than a pinned release, so it becomes
active only once this repository is hosted at its remote and tracks
whatever the host mainline is today. It intentionally claims no stable
release compatibility matrix.

## Layout

| Path | Purpose |
|---|---|
| `compose.yaml` | Loopback-only, password-protected Redis fixture |
| `scripts/seed-demo.sh` | Idempotent seed of logical database 2 |
| `scripts/run-workbench-demo.sh` | Build + fixture + seed + config + TUI in one command |
| `src/` | perk/v1 transport and the Redis-backed service |
| `tests/redis_integration.rs` | Integration tests against a live Redis |
