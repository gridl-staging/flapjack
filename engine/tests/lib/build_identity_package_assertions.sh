# shellcheck shell=bash
# Foreign-artifact assertions sourced by build_identity_package_contract.sh.
assert_foreign_fixture_identity() {
  [ -f "$FOREIGN_FIXTURE" ] || die "foreign target fixture is missing: $FOREIGN_FIXTURE"
  [ -x "$FOREIGN_FIXTURE" ] || die "foreign target fixture must satisfy the helper executable precondition"

  python3 - "$FOREIGN_FIXTURE" "$FOREIGN_FIXTURE_SHA256" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expected_sha256 = sys.argv[2]
contents = path.read_bytes()
if hashlib.sha256(contents).hexdigest() != expected_sha256:
    raise SystemExit("foreign target fixture bytes do not match the known executable")
if contents[:4] != b"\x7fELF" or contents[4:6] != b"\x02\x01":
    raise SystemExit("foreign target fixture must be a 64-bit little-endian ELF")
if int.from_bytes(contents[18:20], "little") != 183:
    raise SystemExit("foreign target fixture must declare the Linux aarch64 machine type")
PY
}

run_foreign_package_without_execution() {
  local package_helper="$1"
  local proof_name="$2"
  local output_dir="$3"
  local stdout="$TMP_ROOT/${proof_name}_stdout.log"
  local stderr="$TMP_ROOT/${proof_name}_stderr.log"
  local execution_sentinel="$TMP_ROOT/${proof_name}_execution_sentinel"
  local status=0

  set +e
  COPYFILE_DISABLE=1 \
    FLAPJACK_EXECUTION_SENTINEL="$execution_sentinel" \
    "$package_helper" "$FOREIGN_TARGET" "$FOREIGN_FIXTURE" "$output_dir" >"$stdout" 2>"$stderr"
  status=$?
  set -e

  if [ -e "$execution_sentinel" ]; then
    die "foreign target package helper host-executed the target binary (execution sentinel created)"
  fi

  if [ "$status" -ne 0 ]; then
    cat "$stdout" "$stderr" >&2
    die "foreign target package helper must produce a manifest without host-executing the target binary (status $status)"
  fi
}

assert_foreign_package_outputs() {
  local output_dir="$1"
  python3 - \
    "$FOREIGN_FIXTURE" \
    "$output_dir/flapjack-${FOREIGN_TARGET}.manifest.json" \
    "$output_dir/flapjack-${FOREIGN_TARGET}.tar.gz" <<'PY'
import hashlib
import json
import pathlib
import sys
import tarfile

fixture_path = pathlib.Path(sys.argv[1])
manifest_path = pathlib.Path(sys.argv[2])
archive_path = pathlib.Path(sys.argv[3])

expected_build = {
    "schemaVersion": 1,
    "version": "1.0.11-fixture",
    "revision": "0123456789abcdef0123456789abcdef01234567",
    "revisionKnown": True,
    "dirty": False,
    "dirtyKnown": True,
    "workspaceDigest": "a" * 64,
    "profile": "release",
    "target": "aarch64-unknown-linux-musl",
    "features": ["fixture-feature", "vector-search"],
    "capabilities": {
        "vectorSearch": True,
        "vectorSearchLocal": False,
    },
}

manifest = json.loads(manifest_path.read_text())
if set(manifest) != {"schemaVersion", "artifact", "build"}:
    raise SystemExit(f"foreign target manifest keys mismatch: {sorted(manifest)}")
if manifest["schemaVersion"] != 1:
    raise SystemExit(f"foreign target schemaVersion mismatch: {manifest['schemaVersion']}")
if manifest["build"] != expected_build:
    raise SystemExit(
        "foreign target build metadata mismatch:\n"
        f"expected={json.dumps(expected_build, sort_keys=True)}\n"
        f"actual={json.dumps(manifest['build'], sort_keys=True)}"
    )

archive_sha256 = hashlib.sha256(archive_path.read_bytes()).hexdigest()
expected_artifact = {
    "file": "flapjack-aarch64-unknown-linux-musl.tar.gz",
    "target": "aarch64-unknown-linux-musl",
    "arch": "aarch64",
    "profile": "release",
    "sha256": archive_sha256,
}
if manifest["artifact"] != expected_artifact:
    raise SystemExit(
        "foreign target artifact metadata mismatch:\n"
        f"expected={json.dumps(expected_artifact, sort_keys=True)}\n"
        f"actual={json.dumps(manifest['artifact'], sort_keys=True)}"
    )

sidecar_path = pathlib.Path(str(archive_path) + ".sha256")
if sidecar_path.read_text().strip().split() != [archive_sha256, archive_path.name]:
    raise SystemExit("foreign target checksum sidecar does not match the packaged archive")

with tarfile.open(archive_path, "r:gz") as archive:
    packaged_files = [member for member in archive.getmembers() if member.isfile()]
    if [member.name for member in packaged_files] != ["./flapjack"]:
        raise SystemExit(
            f"foreign target archive file set mismatch: {[member.name for member in packaged_files]}"
        )
    packaged_binary = archive.extractfile(packaged_files[0])
    if packaged_binary is None or packaged_binary.read() != fixture_path.read_bytes():
        raise SystemExit("foreign target archive does not contain the exact fixture binary")
PY
}

assert_foreign_target_manifest_contract() {
  local package_helper="$1"
  local proof_name="$2"
  local output_dir="$TMP_ROOT/${proof_name}_output"

  mkdir -p "$output_dir"
  assert_foreign_fixture_identity
  run_foreign_package_without_execution "$package_helper" "$proof_name" "$output_dir"
  assert_foreign_package_outputs "$output_dir"
}

assert_duplicate_end_marker_rejected() {
  local fixture="$TMP_ROOT/duplicate_end_marker_fixture"
  local output_dir="$TMP_ROOT/duplicate_end_marker_output"
  local stderr="$TMP_ROOT/duplicate_end_marker_stderr.log"
  local status=0

  mkdir -p "$output_dir"
  python3 - "$FOREIGN_FIXTURE" "$fixture" <<'PY'
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
destination = pathlib.Path(sys.argv[2])
destination.write_bytes(b"\nFLAPJACK_BUILD_INFO_JSON_END\n" + source.read_bytes())
destination.chmod(0o755)
PY

  set +e
  # shellcheck disable=SC2153 # Assigned by the sourcing contract driver.
  "$PACKAGE_HELPER" "$FOREIGN_TARGET" "$fixture" "$output_dir" >/dev/null 2>"$stderr"
  status=$?
  set -e

  [ "$status" -ne 0 ] || die "duplicate embedded build-info end marker was silently accepted"
  grep -Fq 'embedded build-info JSON end marker must appear exactly once, found 2' "$stderr" \
    || die "duplicate end marker failed without the expected diagnostic"
  [ ! -e "$output_dir/flapjack-${FOREIGN_TARGET}.manifest.json" ] \
    || die "duplicate end marker must not produce a manifest"
}

assert_linux_musl_cli_mismatch_rejected() {
  local fixture="$TMP_ROOT/x86_64_musl_fixture"
  local fake_tools="$TMP_ROOT/fake_tools"
  local execution_sentinel="$TMP_ROOT/linux_musl_execution_sentinel"
  local output_dir="$TMP_ROOT/linux_musl_output"
  local stdout="$TMP_ROOT/linux_musl_stdout.log"
  local stderr="$TMP_ROOT/linux_musl_stderr.log"
  local status=0

  mkdir -p "$fake_tools" "$output_dir"
  python3 - "$fixture" "$fake_tools/rustc" <<'PY'
import pathlib
import sys

fixture = pathlib.Path(sys.argv[1])
fake_rustc = pathlib.Path(sys.argv[2])
embedded = '{"capabilities":{"vectorSearch":true,"vectorSearchLocal":false},"dirty":false,"dirtyKnown":true,"features":["vector-search"],"profile":"release","revision":"0123456789abcdef0123456789abcdef01234567","revisionKnown":true,"schemaVersion":1,"target":"x86_64-unknown-linux-musl","version":"1.0.11-fixture","workspaceDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'
executed = embedded.replace('"version":"1.0.11-fixture"', '"version":"1.0.11-mismatch"')
fixture.write_text(
    "#!/usr/bin/env bash\n"
    ": <<'BUILD_INFO_RECORD'\n"
    "FLAPJACK_BUILD_INFO_JSON_BEGIN\n"
    f"{embedded}\n"
    "FLAPJACK_BUILD_INFO_JSON_END\n"
    "BUILD_INFO_RECORD\n"
    'printf "executed\\n" >"${FLAPJACK_EXECUTION_SENTINEL:?}"\n'
    f"printf '%s\\n' '{executed}'\n"
)
fake_rustc.write_text("#!/bin/sh\nprintf 'host: x86_64-unknown-linux-gnu\\n'\n")
fixture.chmod(0o755)
fake_rustc.chmod(0o755)
PY

  set +e
  PATH="$fake_tools:$PATH" FLAPJACK_EXECUTION_SENTINEL="$execution_sentinel" \
    "$PACKAGE_HELPER" x86_64-unknown-linux-musl "$fixture" "$output_dir" \
    >"$stdout" 2>"$stderr"
  status=$?
  set -e

  [ -f "$execution_sentinel" ] \
    || die "x86_64 Linux musl artifact was not executed on its compatible GNU host"
  [ "$status" -ne 0 ] || die "CLI/embedded build-info mismatch was silently packaged"
  grep -Fq 'executed build-info JSON does not match embedded build-info JSON' "$stderr" \
    || die "CLI/embedded mismatch failed without the expected diagnostic"
  [ ! -e "$output_dir/flapjack-x86_64-unknown-linux-musl.manifest.json" ] \
    || die "CLI/embedded mismatch must not produce a manifest"
}
