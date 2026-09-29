#!/usr/bin/env bats
#
# bats suite for scripts/dead-code-report.sh
#
# Covers:
#   - report generation with canned cargo metadata output (PATH shims)
#   - empty-input handling producing a well-formed empty report
#

setup() {
  REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../.." && pwd)"
  SCRIPT="$REPO_ROOT/scripts/dead-code-report.sh"

  TEST_TMP="$(mktemp -d)"
  SHIM_BIN="$TEST_TMP/bin"
  mkdir -p "$SHIM_BIN"

  # Isolate the report output inside the temp dir.
  export DEAD_CODE_REPORT_OUT="$TEST_TMP/dead-code-report.md"

  # Prepend the shim dir so the script picks up our canned cargo tools.
  export PATH="$SHIM_BIN:$PATH"
}

teardown() {
  rm -rf "$TEST_TMP"
}

# Write a PATH shim that emits the given stdout and exits with the given code.
make_shim() {
  local name="$1"
  local output="$2"
  local code="${3:-0}"
  cat >"$SHIM_BIN/$name" <<EOF
#!/usr/bin/env bash
cat <<'SHIM_OUT'
$output
SHIM_OUT
exit $code
EOF
  chmod +x "$SHIM_BIN/$name"
}

@test "script exists and is executable" {
  [ -f "$SCRIPT" ]
  [ -x "$SCRIPT" ]
}

@test "generates a report from canned cargo metadata output" {
  make_shim cargo '{"packages":[{"name":"demo","targets":[{"name":"demo","kind":["bin"]}]}]}'

  run "$SCRIPT"
  [ "$status" -eq 0 ]

  [ -f "$DEAD_CODE_REPORT_OUT" ]
  [ -s "$DEAD_CODE_REPORT_OUT" ]

  # The report should reference the crate discovered via the shim.
  run grep -q "demo" "$DEAD_CODE_REPORT_OUT"
  [ "$status" -eq 0 ]
}

@test "empty tool output yields a well-formed empty report" {
  make_shim cargo ''

  run "$SCRIPT"
  [ "$status" -eq 0 ]

  [ -f "$DEAD_CODE_REPORT_OUT" ]

  # A well-formed report is still produced even with no findings.
  run grep -qi "dead code" "$DEAD_CODE_REPORT_OUT"
  [ "$status" -eq 0 ]
}
