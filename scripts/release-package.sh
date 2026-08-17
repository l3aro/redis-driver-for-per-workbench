#!/usr/bin/env bash
# Packages one perk-redis release into OUT: the versioned reproducible
# archive (executable + README, plus LICENSE when present), SHA256SUMS,
# the raw plugin-test evidence JSON, and the release manifest tying
# tag/plugin version, target, exact host ref, protocol, contract
# digest, executable/archive checksums, and conformance pass/counts.
# Every invariant is validated with jq before anything is written; a
# single failure exits nonzero so CI never publishes incomplete
# evidence.
#
# Environment:
#   TAG               Release tag, e.g. v0.1.0 (required). Must equal
#                     "v" + the Cargo.toml version unless DRY_RUN=1.
#   EVIDENCE          Path of the plugin test evidence JSON produced by
#                     `perk-workbench plugin test --json` against the
#                     release binary (required).
#   OUT               Output directory (default: <repo>/dist).
#   ROOT              Repo root (default: parent of the script dir).
#   SOURCE_DATE_EPOCH Unix timestamp for the archive's mtimes —
#                     reproducibility requires it (CI sets it from the
#                     release commit time).
#   DRY_RUN=1         Skip the tag/version match check and mark the
#                     release manifest dry_run: true (workflow_dispatch
#                     and local validation runs).
set -euo pipefail

root=${ROOT:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)}
out=${OUT:-"$root/dist"}
evidence=${EVIDENCE:?EVIDENCE (plugin test evidence JSON) is required}
tag=${TAG:?TAG is required}
epoch=${SOURCE_DATE_EPOCH:?SOURCE_DATE_EPOCH (release commit time) is required for reproducible archives}

manifest="$root/compatibility-manifest.json"
binary="$root/target/release/perk-redis"

version=$(sed -n 's/^version = "\([^"]*\)"/\1/p' "$root/Cargo.toml" | head -n1)
[ -n "$version" ] || { echo "cannot read version from Cargo.toml" >&2; exit 1; }
target=$(jq -r '.release_targets[0]' "$manifest")
protocol_version=$(jq -r '.protocol.version' "$manifest")
host_ref=$(jq -r '.host.tested_ref' "$manifest")
evidence_schema=$(jq -r '.conformance_evidence.schema' "$manifest")
evidence_version=$(jq -r '.conformance_evidence.version' "$manifest")
cases=$(jq -r '.conformance_evidence.cases' "$manifest")

# The manifest is the contract for this packaging run.
[ "$protocol_version" = "1" ] || { echo "manifest protocol version is not 1" >&2; exit 1; }
echo "$host_ref" | grep -Eq '^[0-9a-f]{40}$' || { echo "manifest host.tested_ref is not a full commit hash" >&2; exit 1; }
[ "$evidence_schema" = "perk/v1/plugin-test-evidence.schema.json" ] || { echo "manifest evidence schema mismatch" >&2; exit 1; }
[ -n "$evidence_version" ] && [ "$evidence_version" -gt 0 ] || { echo "manifest evidence version invalid" >&2; exit 1; }

if [ -z "${DRY_RUN:-}" ] && [ "$tag" != "v$version" ]; then
  echo "tag $tag does not match Cargo.toml version $version" >&2
  exit 1
fi

[ -x "$binary" ] || { echo "release binary missing: $binary (run: cargo build --locked --release)" >&2; exit 1; }
[ -f "$evidence" ] || { echo "evidence file missing: $evidence" >&2; exit 1; }

bin_sha=$(sha256sum "$binary" | cut -d' ' -f1)

# Evidence invariants: a passing run against exactly the archived
# binary — the evidence's executable digest must equal the digest of
# the binary being packaged.
jq -e --argjson cases "$cases" --arg bin_sha "$bin_sha" \
  '.ok == true and .failed == 0 and (.passed + .failed) == $cases
   and .protocol_version == 1
   and (.contract_sha256 | test("^[0-9a-f]{64}$"))
   and .executable_sha256 == $bin_sha
   and (.host_version | test("^perk-workbench "))' "$evidence" >/dev/null \
  || { echo "evidence invariants failed (run must pass against the release binary)" >&2; exit 1; }

archive_name="perk-redis-$version-$target.tar.gz"
rm -rf "$out"
mkdir -p "$out" "$out/stage"

cp "$binary" "$out/stage/perk-redis"
cp "$root/README.md" "$out/stage/README.md"
members="perk-redis README.md"
if [ -f "$root/LICENSE" ]; then
  cp "$root/LICENSE" "$out/stage/LICENSE"
  members="$members LICENSE"
fi

# Reproducible archive: sorted names, fixed mtime (SOURCE_DATE_EPOCH),
# fixed owner/group, ustar format, gzip without name/timestamp header.
# The same commit and SOURCE_DATE_EPOCH always produce the same bytes.
tar --sort=name --mtime="@$epoch" --owner=0 --group=0 --numeric-owner \
  --format=ustar -C "$out/stage" -cf - $members | gzip -n > "$out/$archive_name"
archive_sha=$(sha256sum "$out/$archive_name" | cut -d' ' -f1)

cp "$evidence" "$out/plugin-test-evidence.json"
( cd "$out" && sha256sum "$archive_name" plugin-test-evidence.json > SHA256SUMS )

if [ -n "${DRY_RUN:-}" ]; then dry_run_json="true"; else dry_run_json="false"; fi

jq -n \
  --arg tag "$tag" \
  --arg version "$version" \
  --arg target "$target" \
  --arg host_ref "$host_ref" \
  --arg host_version "$(jq -r '.host_version' "$evidence")" \
  --arg contract "$(jq -r '.contract_sha256' "$evidence")" \
  --arg bin_sha "$bin_sha" \
  --arg archive_sha "$archive_sha" \
  --argjson passed "$(jq '.passed' "$evidence")" \
  --argjson failed "$(jq '.failed' "$evidence")" \
  --argjson cases "$cases" \
  --arg archive_name "$archive_name" \
  --arg evidence_schema "$evidence_schema" \
  --argjson evidence_version "$evidence_version" \
  --argjson dry_run "$dry_run_json" \
  '{schema_version: 1,
    tag: $tag,
    plugin: {name: "perk-redis", version: $version, version_source: "Cargo.toml [package].version"},
    target: $target,
    host: {ref: $host_ref, version: $host_version},
    protocol: {version: 1},
    contract_sha256: $contract,
    checksums: {executable: $bin_sha, archive: $archive_sha},
    conformance: {evidence: "plugin-test-evidence.json", evidence_schema: $evidence_schema, evidence_version: $evidence_version, cases: $cases, passed: $passed, failed: $failed, ok: ($failed == 0)},
    artifacts: [$archive_name, "SHA256SUMS", "plugin-test-evidence.json", "release-manifest.json"],
    dry_run: $dry_run}' > "$out/release-manifest.json"

# Final release-manifest invariants — the tied document must be
# self-consistent before CI uploads anything.
jq -e \
  '((.dry_run == true) or (.tag == "v" + .plugin.version))
   and .protocol.version == 1
   and .conformance.ok == true and .conformance.failed == 0
   and .conformance.passed + .conformance.failed == .conformance.cases
   and .conformance.evidence_schema == "perk/v1/plugin-test-evidence.schema.json"
   and (.contract_sha256 | test("^[0-9a-f]{64}$"))
   and (.checksums.executable | test("^[0-9a-f]{64}$"))
   and (.checksums.archive | test("^[0-9a-f]{64}$"))
   and (.host.ref | test("^[0-9a-f]{40}$"))' "$out/release-manifest.json" >/dev/null \
  || { echo "release manifest invariants failed" >&2; exit 1; }

# Cross-checks: the archive members are exactly the declared set, and
# SHA256SUMS carries the real hashes of the archive and the evidence.
want_members="perk-redis
README.md"
[ -f "$root/LICENSE" ] && want_members="$want_members
LICENSE"
[ "$(tar -tzf "$out/$archive_name")" = "$want_members" ] \
  || { echo "archive members differ from the declared set" >&2; exit 1; }
grep -q "^$archive_sha  $archive_name$" "$out/SHA256SUMS" \
  || { echo "SHA256SUMS does not carry the archive hash" >&2; exit 1; }
evidence_sha=$(sha256sum "$evidence" | cut -d' ' -f1)
grep -q "^$evidence_sha  plugin-test-evidence.json$" "$out/SHA256SUMS" \
  || { echo "SHA256SUMS does not carry the evidence hash" >&2; exit 1; }

echo "packaged $tag ($target, host $host_ref):"
for f in "$archive_name" SHA256SUMS plugin-test-evidence.json release-manifest.json; do
  printf '  %8d  %s\n' "$(wc -c < "$out/$f")" "$f"
done
