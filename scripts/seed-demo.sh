#!/usr/bin/env sh
# Idempotently seeds logical database 2 of the compose fixture with the
# representative keys the README demo and the integration tests use:
# a greeting string, a plain string, a user:1 hash, a list, a set, and a
# sorted set.
#
# The seed is a snapshot, not an accumulation: FLUSHDB first, then write
# every key, so re-running it always produces exactly the same final
# state. Requires the fixture to be up (`docker compose up -d --wait`).
#
# stdout carries the DBSIZE confirmation; nothing here is protocol
# traffic (that restriction applies to the plugin child only).
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

docker compose exec -T redis redis-cli -a workbench-demo --no-auth-warning -n 2 <<'REDIS'
FLUSHDB
SET greeting "Hello from perk-redis!"
SET product "perk-workbench"
HSET user:1 name "Ada Lovelace" email "ada@example.test" role "admin"
RPUSH queue:jobs "job:1" "job:2" "job:3"
SADD tags:demo "redis" "plugin" "rust"
ZADD leaderboard:demo 10 "alice" 20 "bob" 30 "carol"
REDIS

keys=$(docker compose exec -T redis redis-cli -a workbench-demo --no-auth-warning -n 2 DBSIZE)
printf 'seeded database 2: %s keys (greeting, product, user:1, queue:jobs, tags:demo, leaderboard:demo)\n' "$keys"
