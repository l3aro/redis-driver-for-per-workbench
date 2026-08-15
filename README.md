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
   (see [Schema browsing](#schema-browsing)). The SQL tab is visible in
   the workspace but the initial focus is the schema pane — press `2`
   (focus workspace) to type into the editor.

## Trying the plugin in the TUI

- **PING** — type `PING`, press `F5` (or `Ctrl+Enter`). The result table
  shows `PONG`.
- **GET greeting** — press `F5`; the seeded greeting value is returned.
- **HGETALL user:1** — the seeded hash rows (`name`, `email`, `role`).
- **SCAN 0** — one row per key in database 2.

Quit with `Ctrl+C` (or `Ctrl+Q` → Quit when the SQL editor holds text).

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

## Layout

| Path | Purpose |
|---|---|
| `compose.yaml` | Loopback-only, password-protected Redis fixture |
| `scripts/seed-demo.sh` | Idempotent seed of logical database 2 |
| `scripts/run-workbench-demo.sh` | Build + fixture + seed + config + TUI in one command |
| `src/` | perk/v1 transport and the Redis-backed service |
| `tests/redis_integration.rs` | Integration tests against a live Redis |
