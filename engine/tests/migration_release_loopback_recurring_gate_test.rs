use regex::Regex;
use std::env;
use std::fs;
use std::path::PathBuf;

const WORKFLOW_PATH_OVERRIDE_ENV: &str = "FLAPJACK_NIGHTLY_WORKFLOW_PATH_UNDER_TEST";
const MIGRATION_JOB: &str = "migration-import-contract";
const LOOPBACK_CONTRACT_COMMAND: &str = "bash engine/tests/migration_release_loopback_contract.sh";
const RELEASE_MEILISEARCH_TEST_COMMAND: &str = "cargo test --release -p flapjack-http --lib -- preview_loopback_constructor_is_reachable_in_release_with_explicit_opt_in";

#[test]
fn scheduled_migration_import_job_runs_release_loopback_gate() {
    let workflow = fs::read_to_string(workflow_path()).expect("nightly workflow must be readable");
    let job = workflow_job(&workflow, MIGRATION_JOB);

    assert!(
        workflow_has_schedule_trigger(&workflow),
        "nightly workflow must retain its top-level schedule trigger for {MIGRATION_JOB}"
    );
    assert!(
        job_has_run_command(job, RELEASE_MEILISEARCH_TEST_COMMAND),
        "{MIGRATION_JOB} must run the release-only Meilisearch loopback constructor proof"
    );
    assert!(
        job_has_run_command(job, LOOPBACK_CONTRACT_COMMAND),
        "{MIGRATION_JOB} must run the shipped-profile migration release loopback contract"
    );
    assert!(
        !job_has_dispatch_only_condition(job),
        "{MIGRATION_JOB} is scheduled and must not be restricted to workflow_dispatch"
    );
}

fn workflow_path() -> PathBuf {
    env::var_os(WORKFLOW_PATH_OVERRIDE_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.github/workflows/nightly.yml")
        })
}

fn workflow_has_schedule_trigger(workflow: &str) -> bool {
    workflow
        .lines()
        .skip_while(|line| *line != "on:")
        .skip(1)
        .take_while(|line| {
            line.is_empty() || line.starts_with(char::is_whitespace) || line.starts_with('#')
        })
        .any(|line| line == "  schedule:")
}

fn workflow_job<'a>(workflow: &'a str, job_name: &str) -> &'a str {
    let job_header =
        Regex::new(&format!(r"^  {}:$", regex::escape(job_name))).expect("job regex must compile");
    let next_job_header =
        Regex::new(r"^  [A-Za-z0-9_-]+:$").expect("job header regex must compile");
    let start = workflow
        .lines()
        .scan(0, |offset, line| {
            let line_start = *offset;
            *offset += line.len() + 1;
            Some((line_start, line))
        })
        .find_map(|(offset, line)| job_header.is_match(line).then_some(offset))
        .unwrap_or_else(|| panic!("nightly workflow must contain the {job_name} job"));
    let remainder = &workflow[start..];
    let end = remainder
        .lines()
        .scan(0, |offset, line| {
            let line_start = *offset;
            *offset += line.len() + 1;
            Some((line_start, line))
        })
        .skip(1)
        .find_map(|(offset, line)| next_job_header.is_match(line).then_some(offset))
        .unwrap_or(remainder.len());

    &remainder[..end]
}

fn job_has_dispatch_only_condition(job: &str) -> bool {
    job.lines()
        .filter_map(|line| line.trim().strip_prefix("if:"))
        .any(|condition| condition.contains("workflow_dispatch"))
}

fn job_has_run_command(job: &str, command: &str) -> bool {
    job.lines()
        .map(str::trim)
        .map(|line| line.strip_prefix("- ").unwrap_or(line))
        .filter_map(|line| line.strip_prefix("run:"))
        .any(|configured_command| configured_command.trim() == command)
}

#[test]
fn schedule_trigger_detection_is_scoped_to_top_level_on_section() {
    assert!(workflow_has_schedule_trigger(
        "on:\n  schedule:\n    - cron: '0 2 * * *'\njobs:\n"
    ));
    assert!(!workflow_has_schedule_trigger(
        "on:\n  workflow_dispatch:\njobs:\n  schedule:\n    runs-on: ubuntu-latest\n"
    ));
}

#[test]
fn run_command_detection_rejects_commented_commands() {
    assert!(job_has_run_command(
        "  migration-import-contract:\n    steps:\n      - run: required command\n",
        "required command"
    ));
    assert!(!job_has_run_command(
        "  migration-import-contract:\n    steps:\n      # - run: required command\n",
        "required command"
    ));
}
