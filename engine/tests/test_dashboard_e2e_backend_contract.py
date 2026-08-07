#!/usr/bin/env python3
"""Fail the build when a workflow runs dashboard e2e specs against a backend that cannot satisfy them.

WHY THIS EXISTS
---------------
Nightly run 31072375322 (2026-08-06) and prod push CI run 31096601354 (same day)
each failed on a *different* dashboard spec, for the *same* structural reason: the
CI job that starts the Flapjack backend and the spec that declares a backend
precondition had no shared owner, so they drifted.

  - `nightly.yml` "Dashboard all tests" starts a standalone backend and runs
    `npm run test:pages`, which is `playwright test --project=e2e-ui` with no path
    filter — so it includes `tests/e2e-ui/full/cluster_peers.spec.ts`, which throws
    unless replication is configured.
  - `ci.yml` "Dashboard full e2e tests" starts a replication-enabled backend but omits
    `FLAPJACK_AI_ALLOW_LOCAL_URLS`, so `tests/e2e-ui/full/vector-settings.spec.ts`
    trips the outbound SSRF control three times.

Both specs were correct. Both passed the lane that authored them, because that lane
started a backend that satisfied them. Nothing failed until the spec reached a job
whose backend was configured by a different hand.

WHY THE ENV MUST BE ON THE STARTUP STEP
---------------------------------------
`scripts/playwright-webserver.mjs::startPlaywrightServers` passes `allowReuse: true`
for the backend, so a backend already listening on the target port is REUSED and
`spawnBackendServer` — the one place that sets the right environment — never runs.
Environment declared on the Playwright step therefore never reaches the process under
test. This gate deliberately requires the env on the step that launches the binary,
because that is the only step that can supply it.

WHAT IT CHECKS
--------------
1. Every workflow job that starts a Flapjack backend and later runs a dashboard
   Playwright script declares every environment variable required by the specs that
   script actually selects. The selection is resolved from `package.json`, not
   restated here, so a script that gains a path filter cannot escape the contract.
2. `spawnBackendServer` supplies the union of every requirement, because it starts one
   backend for whatever a developer runs locally and must satisfy the widest scope.

WHAT IT DELIBERATELY DOES NOT CHECK
-----------------------------------
It does not run the specs, and it does not verify the running backend reports the
capability. A value can be present and wrong. This is a wiring contract: it catches
the omission that has now happened twice, cheaply, without a browser. The runtime half
is `assertBackendReadiness` in `engine/dashboard/scripts/playwright-webserver.mjs`,
which refuses at startup rather than at spec time.

It also finds a job's backend only by `BACKEND_LAUNCH_RE`, i.e. a `flapjack --data-dir`
invocation. A job that starts one some other way — a container, a wrapper script, a
different flag order — is skipped rather than reported, because "no backend launch
found" is also the correct reading of a job that lets Playwright spawn its own. If a
future job starts its backend differently, extend that pattern; do not assume silence
here means the job was checked.

NON-VACUITY
-----------
Named live specimens at `dcdb8a77a`: this gate reports two findings and exits 1 —
`nightly.yml` job `dashboard` missing `replication` and `ai_local_outbound`, and
`ci.yml` job `dashboard-full-e2e` missing `ai_local_outbound`. Deleting
`FLAPJACK_AI_ALLOW_LOCAL_URLS` from either fixed job, or from `spawnBackendServer`,
returns it to red today.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"
DASHBOARD_DIR = REPO_ROOT / "engine" / "dashboard"
CONTRACT_PATH = DASHBOARD_DIR / "tests" / "e2e_backend_contract.json"
PACKAGE_JSON_PATH = DASHBOARD_DIR / "package.json"
WEBSERVER_PATH = DASHBOARD_DIR / "scripts" / "playwright-webserver.mjs"

# The backend is always launched by invoking the binary with an explicit data dir.
# Matching that, rather than the binary path, keeps this working across the three
# different paths the workflows use (`/tmp/flapjack/flapjack`,
# `engine/target/debug/flapjack`, `$PWD/engine/target/release/flapjack`).
BACKEND_LAUNCH_RE = re.compile(r"\bflapjack\s+--data-dir\b")

# `npm run <script>` / `npm run <script> -- ...`, the only way these workflows enter
# the dashboard test suite.
NPM_RUN_RE = re.compile(r"\bnpm\s+run\s+([A-Za-z0-9:_-]+)")

# `KEY=value` used as a command prefix. Anchored to a token boundary so a value
# containing `=` (an advertise URL, for instance) cannot be mistaken for a new
# assignment.
ENV_ASSIGNMENT_RE = re.compile(r"(?:^|\s)([A-Z][A-Z0-9_]*)=(\S*)")


class Finding(Exception):
    """A contract violation, carried with enough detail to act on without re-deriving it."""


def load_yaml(path: Path):
    """Parse a workflow. A gate that cannot parse its input must fail, never skip."""
    try:
        import yaml  # noqa: PLC0415 - imported here so the failure message can be specific
    except ImportError:  # pragma: no cover - environment defect, not a contract defect
        print(
            "FATAL: PyYAML is required to read the workflow files. A contract gate that "
            "cannot read its input has not verified anything, so this is a failure rather "
            "than a skip. Install with: python3 -m pip install pyyaml",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def collect_prefix_env(run_body: str) -> dict[str, str]:
    """Environment set as a command prefix on the backend launch.

    The workflows write the launch as a backslash-continued run of `KEY=value`
    assignments ending in the binary invocation. Joining continuations first means a
    multi-line prefix reads as one command, which is how the shell sees it.
    """
    joined = re.sub(r"\\\s*\n\s*", " ", run_body)
    env: dict[str, str] = {}
    for line in joined.splitlines():
        if not BACKEND_LAUNCH_RE.search(line):
            continue
        # Only assignments to the LEFT of the binary are a command prefix; anything
        # after it is an argument, so slicing at the match keeps flags out.
        prefix = line[: BACKEND_LAUNCH_RE.search(line).start()]
        for name, value in ENV_ASSIGNMENT_RE.findall(prefix):
            env[name] = value
    return env


def step_env(step: dict) -> dict[str, str]:
    """Environment a step declares through GitHub's own `env:` mapping."""
    declared = step.get("env") or {}
    return {str(k): str(v) for k, v in declared.items()}


def resolve_script_paths(invocation: str) -> list[str]:
    """Spec paths named as arguments to one Playwright invocation.

    An invocation with no path argument selects its whole project, which is exactly how
    `test:pages` came to run the `full/` specs in the nightly. That case returns the empty
    list and is treated by the caller as "the whole project".
    """
    return [token.strip("'\"") for token in invocation.split() if token.startswith("tests/")]


# Playwright project -> testDir, from engine/dashboard/playwright.config.ts. An invocation
# with no path filter selects everything under its project's testDir.
PROJECT_TEST_DIRS = {
    "--project=e2e-ui": "tests/e2e-ui/",
    "--project=e2e-api": "tests/e2e-api/",
}


def playwright_invocations(script_body: str) -> list[str]:
    """Each `playwright test ...` command in an npm script, separately.

    Splitting matters. `test:integration` chains two: the e2e-api project with NO path
    filter, then one named e2e-ui spec. Evaluating the script as a single blob let the
    second invocation's path filter mask the first invocation's unfiltered selection, so a
    requirement naming any `tests/e2e-api/` spec was silently unchecked — a false negative
    in the guard itself, confirmed by probe before this was rewritten. Splitting on shell
    command separators keeps each invocation's project and paths together, which is how the
    shell actually runs them.
    """
    invocations = []
    for chunk in re.split(r"&&|\|\||;", script_body):
        marker = chunk.find("playwright test")
        if marker != -1:
            invocations.append(chunk[marker:])
    return invocations


def invocation_selects(spec_path: str, invocation: str) -> bool:
    """Does one Playwright invocation run `spec_path`?"""
    test_dir = next(
        (d for flag, d in PROJECT_TEST_DIRS.items() if flag in invocation), None
    )
    if test_dir is None:
        # No recognised --project. Do not guess: an unrecognised invocation is reported by
        # returning False here and would surface as a requirement going unchecked, which is
        # why PROJECT_TEST_DIRS must be extended whenever a project is added.
        return False
    selected = resolve_script_paths(invocation)
    if not selected:
        return spec_path.startswith(test_dir)
    return any(spec_path == sel or spec_path.startswith(sel) for sel in selected)


def script_selects(spec_path: str, script_body: str) -> bool:
    """Does this npm script run `spec_path` through any of its Playwright invocations?"""
    return any(
        invocation_selects(spec_path, invocation)
        for invocation in playwright_invocations(script_body)
    )


def check_workflows(contract: dict, scripts: dict[str, str]) -> list[str]:
    findings: list[str] = []
    entry_points = set(contract["npm_script_spec_scopes"]["playwright_entry_points"])

    for workflow_path in sorted(WORKFLOW_DIR.glob("*.yml")):
        document = load_yaml(workflow_path)
        # GitHub resolves env as workflow < job < step, and a `KEY=value` command prefix is
        # applied by the shell on top of all three. Reading only the step and the prefix
        # made the gate FAIL a job that correctly supplied the variable at job level — a
        # false positive confirmed by probe. Merging in GitHub's own precedence order is
        # what makes the gate agree with the runtime.
        workflow_env = {str(k): str(v) for k, v in (document.get("env") or {}).items()}

        for job_id, job in (document.get("jobs") or {}).items():
            steps = job.get("steps") or []
            job_env = {str(k): str(v) for k, v in (job.get("env") or {}).items()}

            backend_env: dict[str, str] = {}
            backend_step_name = None
            selected_scripts: list[str] = []

            for step in steps:
                if not isinstance(step, dict):
                    continue
                run_body = step.get("run") or ""

                if BACKEND_LAUNCH_RE.search(run_body):
                    backend_step_name = step.get("name") or "<unnamed step>"
                    backend_env = {
                        **workflow_env,
                        **job_env,
                        **step_env(step),
                        **collect_prefix_env(run_body),
                    }
                    # A later launch in the same job replaces the earlier one, which
                    # matches the shell: the last backend started is the one serving.
                    continue

                for script_name in NPM_RUN_RE.findall(run_body):
                    if script_name in entry_points:
                        selected_scripts.append(script_name)

            if backend_step_name is None or not selected_scripts:
                continue

            for requirement in contract["requirements"]:
                needed_by = [
                    spec
                    for spec in requirement["required_by"]
                    for script in selected_scripts
                    if script_selects(spec, scripts.get(script, ""))
                ]
                if not needed_by:
                    continue
                missing = [k for k in requirement["env"] if k not in backend_env]
                if missing:
                    findings.append(
                        f"{workflow_path.name} job '{job_id}' step '{backend_step_name}' "
                        f"starts a backend without requirement '{requirement['id']}' "
                        f"(missing {', '.join(sorted(missing))}), but the job runs "
                        f"{sorted(set(selected_scripts))} which selects "
                        f"{sorted(set(needed_by))}. Symptom when missing: "
                        f"{requirement['symptom_if_missing']!r}. The env must be on the "
                        f"backend startup step — Playwright reuses an already-listening "
                        f"backend, so env on the test step never reaches it."
                    )
    return findings


def check_spawn_backend_server(_contract: dict) -> list[str]:
    """The locally-spawned backend must derive its environment from the same contract.

    This is a WIRING check only. Whether `spawnBackendServer` actually applies the
    contract is asserted behaviourally — by calling it — in
    `engine/dashboard/playwright-webserver.test.ts`, which is a stronger check than any
    grep and is where it belongs. What this adds is that the wiring cannot be deleted
    silently: if `spawnBackendServer` stops reading the contract and goes back to
    literals, the vitest assertion could still be made to pass by copying the values,
    and the two owners would be free to drift again. That is the failure this contract
    exists to prevent, so it is checked here rather than left to review.
    """
    source = WEBSERVER_PATH.read_text(encoding="utf-8")
    findings = []

    if "e2e_backend_contract.json" not in source:
        findings.append(
            f"{WEBSERVER_PATH.relative_to(REPO_ROOT)} does not read "
            f"{CONTRACT_PATH.relative_to(REPO_ROOT)}. The locally-spawned e2e backend must "
            "derive its capability environment from the same declared contract the CI "
            "workflows are held to, otherwise the two can drift — which is the defect the "
            "contract was created for."
        )

    match = re.search(r"export function spawnBackendServer\(.*?\n\}", source, re.DOTALL)
    if match is None:
        findings.append(
            f"{WEBSERVER_PATH.relative_to(REPO_ROOT)}: spawnBackendServer not found. It is "
            "the single owner of the locally-spawned e2e backend environment; this gate "
            "cannot verify a contract against a function that no longer exists."
        )
    # The application call, not the identifier. Checking for `contractEnv` alone stayed
    # green when the value was deleted, because the parameter name survived in the
    # signature — a false green caught by mutation-testing this gate rather than by
    # reading it. `applyBackendContractEnvironment` owns both positive env injection and
    # `env_absent` deletion, so a literal `...contractEnv` spread is no longer sufficient.
    elif "applyBackendContractEnvironment(process.env, contractEnv)" not in match.group(0):
        findings.append(
            f"{WEBSERVER_PATH.relative_to(REPO_ROOT)}: spawnBackendServer accepts the "
            "contract environment but never applies it to the spawned process env, so "
            "the contract has no effect. Behavioural owner of this assertion: "
            "engine/dashboard/playwright-webserver.test.ts."
        )

    return findings


def check_gate_is_wired() -> list[str]:
    """This gate must itself be invoked by CI.

    `engine/tests/test_release_workflow_structure.sh` asserted `release.yml`'s shape for
    months while no workflow invoked it, so every assertion in it was inert. A contract
    test that nothing runs is not a guard, and the cheapest way to keep this one honest
    is to make unwiring it self-detecting. Anchored to an actual `run:` line, because a
    bare path match is also satisfied by a commented-out invocation — which is exactly
    how a suite gets quietly disabled.
    """
    invocation = re.compile(
        r"^\s*run: python3 engine/tests/test_dashboard_e2e_backend_contract\.py\s*$",
        re.MULTILINE,
    )
    for workflow_path in WORKFLOW_DIR.glob("*.yml"):
        if invocation.search(workflow_path.read_text(encoding="utf-8")):
            return []
    return [
        "No workflow invokes this contract. A gate nothing runs has verified nothing; "
        "restore the `run: python3 engine/tests/test_dashboard_e2e_backend_contract.py` "
        "step (it lives in ci.yml's `release-contracts` job)."
    ]


def main() -> int:
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    scripts = json.loads(PACKAGE_JSON_PATH.read_text(encoding="utf-8"))["scripts"]

    # A contract with no requirements would make every assertion below vacuously true.
    if not contract.get("requirements"):
        print("FATAL: the contract declares no requirements; this gate would pass vacuously.", file=sys.stderr)
        return 2

    findings = (
        check_workflows(contract, scripts)
        + check_spawn_backend_server(contract)
        + check_gate_is_wired()
    )

    if findings:
        print("Dashboard e2e backend contract: FAILED\n", file=sys.stderr)
        for finding in findings:
            print(f"  - {finding}\n", file=sys.stderr)
        print(
            f"{len(findings)} violation(s). Owner of the requirement set: "
            f"{CONTRACT_PATH.relative_to(REPO_ROOT)}",
            file=sys.stderr,
        )
        return 1

    print(
        "Dashboard e2e backend contract: OK "
        f"({len(contract['requirements'])} requirement(s) checked against "
        f"{len(list(WORKFLOW_DIR.glob('*.yml')))} workflow file(s) and spawnBackendServer)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
