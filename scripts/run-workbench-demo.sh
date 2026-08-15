#!/usr/bin/env sh
# One-command reproducible demo: build the plugin, bring up the Redis
# fixture, seed database 2, install the plugin into a throwaway
# $XDG_CONFIG_HOME, and launch the perk-workbench TUI against
# redis://:workbench-demo@127.0.0.1:6380/2.
#
# Usage: scripts/run-workbench-demo.sh
#   PERK_WORKBENCH=/path/to/perk-workbench  override the host binary
#                                           (default: ../source/perk-workbench)
#
# The plugin's stdout is the perk/v1 protocol channel and belongs to the
# workbench host alone; this script never writes to it. Every message
# this script prints goes to stderr, so the terminal stays clean for the
# TUI. On exit (normal or via INT/TERM) the fixture is stopped and the
# temporary config directory is removed.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

say() { printf '%s\n' "$*" >&2; }

fixture_up=0
xdg_home=
cleanup() {
    status=$?
    if [ -n "$xdg_home" ]; then
        rm -rf -- "$xdg_home"
    fi
    if [ "$fixture_up" -eq 1 ]; then
        say "[run-workbench-demo] stopping fixture"
        docker compose down >&2
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

# The already-built host: the perk-workbench TUI from the sibling source
# project. Override with PERK_WORKBENCH when the binary lives elsewhere.
workbench=${PERK_WORKBENCH:-"$(CDPATH= cd -- "$repo_root/../source" && pwd)/perk-workbench"}
if [ ! -x "$workbench" ]; then
    say "error: workbench binary not found or not executable: $workbench"
    say "build it in the source project, or set PERK_WORKBENCH"
    exit 1
fi

say "[run-workbench-demo] building plugin with the lockfile (cargo build --locked)"
cargo build --locked

plugin_bin="$repo_root/target/debug/perk-redis"
if [ ! -x "$plugin_bin" ]; then
    say "error: $plugin_bin missing after cargo build"
    exit 1
fi

say "[run-workbench-demo] starting fixture (docker compose up -d --wait)"
docker compose up -d --wait
fixture_up=1

say "[run-workbench-demo] seeding database 2"
scripts/seed-demo.sh

# plugins is an explicit allowlist: the workbench spawns exactly these
# executables and nothing else. The absolute path is the plugin built
# above.
xdg_home=$(mktemp -d)
mkdir -p "$xdg_home/perk-workbench"
escaped=$(printf '%s' "$plugin_bin" | sed 's/\\/\\\\/g; s/"/\\"/g')
printf '{"plugins":["%s"]}\n' "$escaped" > "$xdg_home/perk-workbench/config.json"

say "[run-workbench-demo] launching $workbench"
say "target: redis://:workbench-demo@127.0.0.1:6380/2  (quit with Ctrl+C)"
XDG_CONFIG_HOME="$xdg_home" "$workbench" 'redis://:workbench-demo@127.0.0.1:6380/2'
