//! Fail the build when a receipt asks for a ledger correction that no ledger absorbed.
//!
//! WHY THIS EXISTS
//! ---------------
//! Lanes already produce ledger deltas. Nine receipts under `docs2/4_EVIDENCE/`
//! carry a `ROADMAP CORRECTION REQUIRED` block with verbatim replacement text
//! written for a downstream ledger writer to apply. Nothing anywhere read them —
//! no lint, no gate, no applier, in this repo or in the orchestrator. The delta
//! was produced and discarded.
//!
//! The structural cause is that every batch assigns its ledger edits to a
//! *terminal* lane, so the writer is a dependent of the lanes it describes. When
//! the batch stalls above it — and on 2026-08-03 both live batches had their
//! writer sitting behind undispatched lanes — the ledger simply never gets
//! written. Eleven lanes merged over twenty hours with no writer running, and the
//! public docs carried two false "paid beta" claims and a "the e2e suite is
//! green" claim the measurements contradicted. The ROADMAP header records this
//! same failure three separate times, which is the tell that narrative reminders
//! do not fix it.
//!
//! This test is the enforcement half. It cannot make the writer dispatch, but it
//! bounds how long the repository can be quietly wrong about itself.
//!
//! THE GRACE WINDOW, AND WHY THE FIRST VERSION OF THIS TEST WAS WRONG
//! ------------------------------------------------------------------
//! The first version failed the instant an uncited correction existed. That is a
//! real defect and it was caught the same day, by a concurrent session rather
//! than by this author: `cargo test --workspace` is this repository's
//! wave-transition gate, so a receipt landing mid-batch would have held **every**
//! wave transition red until the batch's ledger writer ran. A batch's own lane
//! file recorded the consequence and routed around the marker entirely —
//! "using the marker would hold `cargo test --workspace` red for the whole batch
//! ... which is a worse outcome than the one the gate exists to prevent". That
//! reasoning is correct. A gate that makes normal operation impossible does not
//! get obeyed; it gets bypassed, and then the delta is invisible again.
//!
//! So the check is now age-aware. An uncited correction is *reported* the moment
//! it appears and only *fails* once the receipt has been on the tree longer than
//! `GRACE_HOURS`. A live batch is never blocked, because its receipts are fresh.
//! What the window buys is a bound: the ledgers can be wrong, but not
//! indefinitely, and the incident that motivated this test — eleven lanes merged
//! over twenty hours with no writer running and two false "paid beta" claims on a
//! publicly-synced surface — becomes a build failure rather than something only
//! an operator reading the tree by hand would notice.
//!
//! `GRACE_HOURS` is deliberately not generous. It is not a scheduling allowance
//! for a terminal ledger lane; the right discharge point is the merge that lands
//! the receipt, where the correction text already exists verbatim and a human or
//! supervisor is already present.
//!
//! WHAT THIS CHECKS, AND WHAT IT DELIBERATELY DOES NOT
//! ---------------------------------------------------
//! It checks that a receipt declaring a correction is *cited* by at least one
//! ledger. It does NOT check that the correction's text landed, or that it landed
//! correctly — a citation proves someone acted on the receipt, not that they
//! acted well. That limit is deliberate rather than an oversight: the stronger
//! check would need either a fragile text match against prose that is expected to
//! be edited, or a hand-maintained "applied" marker that would rot into a
//! rubber stamp. The weak check covers the failure that actually happened
//! (nobody touched the ledger at all), states its own limit here so no reader
//! mistakes it for proof of content, and costs one directory read.
//!
//! NON-VACUITY
//! -----------
//! Named live specimen: at `3d0d2a7a4` — `main` on 2026-08-03 before the
//! operator-directed catch-up in `a61c7760e` — eight of the nine
//! correction-bearing receipts were cited by no ledger, so this test would have
//! reported eight findings and failed. It passes at `a61c7760e` because those
//! corrections were applied. Deleting any one citation from `ROADMAP.md` returns
//! it to red today.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// How long a receipt may sit on the tree with its correction uncited before this
/// test fails.
///
/// **This window bounds the damage; it does not catch every instance, and saying
/// otherwise would be dishonest.** The incident that motivated the test was a
/// ~22-hour lag across eleven merges, and 24h would NOT have failed before the
/// operator noticed it by hand — it would have gone red about two hours later.
/// A shorter window would catch that case and would also wedge any batch that
/// discharges its corrections at batch-end rather than at merge, which is how
/// this repository works today. That is the trade, stated rather than hidden:
/// unbounded wrongness becomes roughly-a-day-bounded wrongness, and the number
/// can safely shrink once corrections are discharged at the merge that lands the
/// receipt.
///
/// Do not raise this to make a red run go away. Raising it is only correct if the
/// discharge point genuinely moved later, and that should be argued in the commit
/// that raises it.
const GRACE_HOURS: u64 = 24;

/// The one marker a receipt uses to say "a ledger owes an edit because of me".
/// Kept as a single literal so there is exactly one spelling to grep for; a
/// receipt that invents its own phrasing is invisible to this gate by design,
/// which is why the marker is documented in the failure message below.
const CORRECTION_MARKER: &str = "ROADMAP CORRECTION REQUIRED";

/// Every ledger allowed to discharge a correction. A receipt cited by any one of
/// them counts as absorbed: which ledger owns a given fact is a routing question
/// owned by `docs2/0_META/HOW_WE_DOCUMENT.md`, not by this test.
const LEDGERS: &[&str] = &[
    "ROADMAP.md",
    "PROJECT_OVERVIEW.md",
    "CHANGELOG.md",
    "engine/docs2/FEATURES.md",
];

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `engine/` for tests in this directory.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("engine/ always has a parent")
        .to_path_buf()
}

/// Both evidence trees, because two exist. `4_EVIDENCE/` is where lane receipts
/// land today; the lowercase `evidence/` tree predates it and still holds one
/// receipt. Scanning only the first would leave a receipt in the second able to
/// declare a correction that this gate never sees, which is the same
/// "guard looks at the wrong place and stays green" failure the gate exists to
/// stop. Recursion covers `evidence/`'s per-run subdirectories.
const EVIDENCE_DIRS: &[&str] = &["engine/docs2/4_EVIDENCE", "engine/docs2/evidence"];

/// Hours since the commit that first added `path`, or `None` when git cannot say.
///
/// `None` means "not yet committed, or history unavailable" and is treated as
/// fresh. Failing open on *age* is safe because age only decides WHEN an uncited
/// correction fails, never whether it is reported: an uncited receipt of unknown
/// age still prints. An uncommitted receipt genuinely is brand new, and it earns a
/// commit date the moment it lands.
fn hours_since_added(root: &Path, path: &Path) -> Option<u64> {
    let out = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["log", "--diff-filter=A", "--format=%ct", "-1", "--"])
        .arg(path)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let added_at: u64 = String::from_utf8(out.stdout).ok()?.trim().parse().ok()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(now.saturating_sub(added_at) / 3600)
}

/// Receipts that declare a correction, as `(file name, full path)`. The name is
/// what a ledger cites; the path is what git can date.
fn receipts_declaring_a_correction(dir: &Path, found: &mut BTreeSet<(String, PathBuf)>) {
    let entries = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read the evidence directory {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("directory entry is readable").path();
        if path.is_dir() {
            receipts_declaring_a_correction(&path, found);
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("md") {
            continue;
        }
        // A receipt that cannot be read is a failure, not a skip: silently
        // ignoring unreadable evidence is how a gate stops being able to fail.
        let body = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read receipt {}: {e}", path.display()));
        if body.contains(CORRECTION_MARKER) {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .expect("receipt file names are UTF-8")
                .to_string();
            found.insert((name, path));
        }
    }
}

fn ledger_text(root: &Path) -> String {
    LEDGERS
        .iter()
        .map(|rel| {
            let path = root.join(rel);
            fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read ledger {}: {e}", path.display()))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn every_receipt_declaring_a_ledger_correction_is_cited_by_a_ledger() {
    let root = repo_root();
    let mut declaring = BTreeSet::new();
    for dir in EVIDENCE_DIRS {
        receipts_declaring_a_correction(&root.join(dir), &mut declaring);
    }

    // Guard the guard. If the marker is ever renamed or the evidence directory
    // moves, this test would pass by finding nothing to check — the exact
    // "green because it looked at an empty set" failure the repo's testing rules
    // forbid. There is no valid state of this repository in which zero receipts
    // have ever asked for a ledger correction.
    assert!(
        !declaring.is_empty(),
        "no receipt under any of {EVIDENCE_DIRS:?} contains the marker {CORRECTION_MARKER:?}. \
         Either the marker was renamed (update CORRECTION_MARKER and every receipt together) \
         or the evidence directory moved. A zero-denominator pass is not a pass."
    );

    let ledgers = ledger_text(&root);
    let uncited: Vec<(&String, Option<u64>)> = declaring
        .iter()
        .filter(|(name, _)| !ledgers.contains(name))
        .map(|(name, path)| (name, hours_since_added(&root, path)))
        .collect();

    // Report every uncited correction, including the ones still inside the grace
    // window. Visibility is free and is most of the value: an operator or a
    // supervisor reading a test run should see the queue forming, not only learn
    // about it once it has already aged into a build failure.
    if !uncited.is_empty() {
        println!("{} uncited ledger correction(s):", uncited.len());
        for (name, age) in &uncited {
            let age_text = age.map_or_else(|| "uncommitted".to_string(), |h| format!("{h}h old"));
            println!("  - {name}  ({age_text})");
        }
    }

    let overdue: Vec<&(&String, Option<u64>)> = uncited
        .iter()
        .filter(|(_, age)| age.is_some_and(|h| h > GRACE_HOURS))
        .collect();

    assert!(
        overdue.is_empty(),
        "{} receipt(s) have declared a {CORRECTION_MARKER} for more than {GRACE_HOURS}h that no \
         ledger cites:\n{}\n\n\
         Each named receipt contains verbatim replacement text written for a ledger writer. \
         Apply it to whichever of {LEDGERS:?} owns that fact, citing the receipt file name so \
         the correction is traceable. Do NOT satisfy this by adding a bare link: the citation \
         is the acknowledgement, and the edit is the work.\n\n\
         If a correction turns out not to be needed, say so in the owning ledger and cite the \
         receipt there — a deliberate no-change is a decision and belongs in the record.\n\n\
         Do NOT satisfy this by avoiding the marker in future receipts. That returns the ledger \
         delta to being invisible, which is the failure this gate exists to catch. If the window \
         is genuinely too short for how this repository discharges corrections, change \
         GRACE_HOURS here and say why.",
        overdue.len(),
        overdue
            .iter()
            .map(|(name, age)| {
                let age_text = age.map_or_else(|| "unknown age".to_string(), |h| format!("{h}h"));
                format!("  - {name}  ({age_text} uncited)")
            })
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

// ---------------------------------------------------------------------------
// Second property: an evidence path a ledger names must be openable.
// ---------------------------------------------------------------------------
//
// WHY THIS EXISTS, SEPARATELY FROM THE TEST ABOVE
// -----------------------------------------------
// The test above asks "did any ledger acknowledge this receipt?" — it matches
// receipt file *names* against ledger text. That join key is one-directional and
// it fails open in a way that bit this repository on 2026-08-03: `ROADMAP.md` row
// `INFRA-1` cited a receipt filename that has never existed in this repository,
// so the real receipt read as uncited, aged past `GRACE_HOURS`, and turned
// `cargo test --workspace` — the wave-transition gate — red for every live batch.
// The harness the row described was fine. The citation was fiction, and *nothing*
// could catch it by reading, because a plausible-looking path in prose is
// indistinguishable from a real one at a glance.
//
// So this test asserts the other direction: every evidence path a ledger names
// resolves to a file on disk.
//
// WHAT COUNTS AS "NAMING A PATH", AND WHY PROSE IS NOT EXEMPT
// -----------------------------------------------------------
// Any `4_EVIDENCE/...md` or `docs2/evidence/...md` token, wherever it appears —
// a citation cell, a markdown link, or narrative prose. Prose is deliberately
// *not* exempt, and that is the interesting design decision here rather than an
// oversight.
//
// A reader cannot tell prose from citation either. Both live specimens found when
// this test was written were prose: one described a file that exists only on an
// abandoned branch, the other quoted the bad path while explaining the bad path.
// Both read as ordinary citations, and a reader who tried to open either would
// find nothing and have no way to know whether the evidence was missing or the
// text was. "Every evidence path this file names can be opened" is a rule a
// writer can follow without thinking about intent: to talk about evidence you
// cannot link, name the branch and the bare filename, and skip the directory
// prefix that makes it look resolvable.
//
// NON-VACUITY
// -----------
// Two live specimens at `583997a09`, both of which this test reports:
//   - `ROADMAP.md` `MIG-17` retained prose naming
//     `2026_07_26_migration_import_meta_suite_runtime_margin_gap.md`, which was
//     added on `batman/jul26_12am_2_migration_suite_runtime_margin` and never
//     merged — the branch `MIG-17` itself closed as ABANDON.
//   - `ROADMAP.md` `INFRA-1`'s own repair note, which quoted the fabricated path
//     verbatim while explaining it.
// `cited_evidence_paths` additionally has a synthetic-input test below, so the
// scanner cannot silently stop finding anything and leave this green by finding
// an empty set.

/// Markers that begin an evidence-path token, paired with whether the marker is
/// already anchored at the repository root.
///
/// `4_EVIDENCE/` appears in two forms in practice: `ROADMAP.md` writes it
/// repo-root-relative (`engine/docs2/4_EVIDENCE/x.md`) and `engine/docs2/FEATURES.md`
/// writes it relative to its own directory (`4_EVIDENCE/x.md`). Both must resolve,
/// so the scanner records the marker and lets the caller anchor it.
const EVIDENCE_PATH_MARKERS: &[&str] = &["4_EVIDENCE/", "docs2/evidence/"];

/// Characters that may continue a path token once a marker has matched.
///
/// Deliberately excludes backtick, parenthesis, and bracket, which is what stops
/// the scan at the end of an inline code span or a markdown link target rather
/// than swallowing the surrounding markup.
///
/// It also excludes every glob and template metacharacter (`*`, `?`, `{`, `}`,
/// `<`, `>`, `$`), and that exclusion is load-bearing rather than incidental —
/// **do not widen this set.** Ledgers legitimately name shapes that are not files:
/// `engine/loadtest/results/**`, `engine/docs2/4_EVIDENCE/<name>.md`,
/// `${FLAPJACK_DATA_DIR}/_idempotency/`. Because the scanner stops at the
/// metacharacter and then *requires* the surviving token to end in `.md`, every
/// one of those truncates to something without a `.md` and is skipped. A future
/// reader who adds `*` here to "catch more" will turn each of those into a
/// spurious failure, and the usual next step is a blanket exclusion that also
/// swallows the real defects this test exists to catch.
fn is_path_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/')
}

/// Every evidence path `body` names, as `(literal token, path relative to the
/// repository root)`.
///
/// `ledger_dir` is the ledger's own directory relative to the repository root
/// (`""` for a repo-root ledger), used to anchor the directory-relative form.
fn cited_evidence_paths(ledger_dir: &str, body: &str) -> BTreeSet<(String, String)> {
    let mut found = BTreeSet::new();
    for marker in EVIDENCE_PATH_MARKERS {
        // Byte offsets are safe to slice on here because every marker and every
        // path character is ASCII; a multi-byte char simply ends the token.
        for (start, _) in body.match_indices(marker) {
            // Extend in BOTH directions over path characters, so the token is the
            // complete path as the ledger wrote it, and normalize once afterwards.
            //
            // Extending left matters and the first version of this scanner got it
            // wrong: it special-cased an `engine/docs2/` prefix, which anchors
            // `engine/docs2/4_EVIDENCE/x.md` correctly but leaves the legacy
            // `engine/docs2/evidence/...` form (whose marker already begins with
            // `docs2/`) preceded by a bare `engine/` that the special case did not
            // match. The result was a false positive on a receipt that exists.
            // Capturing the whole token and then normalizing has one rule instead
            // of a rule per prefix shape.
            let right: String = body[start..]
                .chars()
                .take_while(|c| is_path_char(*c))
                .collect();
            let Some(end) = right.rfind(".md") else {
                continue;
            };
            let left: String = body[..start]
                .chars()
                .rev()
                .take_while(|c| is_path_char(*c))
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            let literal = format!("{left}{}", &right[..end + ".md".len()]);

            // A leading `/` means an absolute path or the remains of a URL after
            // the scan stopped at `:`. Neither is a repository-relative citation,
            // and resolving one against the repo root would be a false positive.
            if literal.starts_with('/') {
                continue;
            }

            let repo_relative = if literal.starts_with("engine/") {
                // Already repository-relative.
                literal.clone()
            } else if literal.starts_with("docs2/") {
                // The `engine/` that preceded the marker, restored.
                format!("engine/{literal}")
            } else if ledger_dir.is_empty() {
                literal.clone()
            } else {
                // The bare `4_EVIDENCE/...` form, relative to the ledger's own
                // directory. This is how `engine/docs2/FEATURES.md` cites.
                format!("{ledger_dir}/{literal}")
            };
            found.insert((literal, repo_relative));
        }
    }
    found
}

#[test]
fn every_evidence_path_a_ledger_names_exists_on_disk() {
    let root = repo_root();
    let mut unresolvable: Vec<(String, String)> = Vec::new();
    let mut checked = 0usize;

    for rel in LEDGERS {
        let path = root.join(rel);
        let body = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read ledger {}: {e}", path.display()));
        // `Path::parent` of a repo-root ledger is `""`, which is exactly the
        // "no anchoring needed" case `cited_evidence_paths` expects.
        let ledger_dir = Path::new(rel)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        for (literal, repo_relative) in cited_evidence_paths(&ledger_dir, &body) {
            checked += 1;
            if !root.join(&repo_relative).is_file() {
                unresolvable.push((rel.to_string(), literal));
            }
        }
    }

    // Guard the guard, same reasoning as the test above: if the evidence tree is
    // renamed or the markers drift, this test would pass by checking nothing.
    // There is no valid state of this repository in which the ledgers cite zero
    // receipts.
    assert!(
        checked > 0,
        "no ledger in {LEDGERS:?} names any evidence path matching {EVIDENCE_PATH_MARKERS:?}. \
         Either the evidence tree moved or the markers drifted; a zero-denominator pass is not a pass."
    );

    assert!(
        unresolvable.is_empty(),
        "{} evidence path(s) named by a ledger do not exist on disk:\n{}\n\n\
         A ledger that names evidence a reader cannot open is worse than one that stays silent: \
         the reader cannot tell whether the evidence is missing or the path is wrong, and a \
         fabricated path also defeats the citation gate above, which joins on file name.\n\n\
         If the file exists somewhere unmerged, say so without writing a resolvable-looking path: \
         name the branch and the bare filename. If the path is simply wrong, fix it.",
        unresolvable.len(),
        unresolvable
            .iter()
            .map(|(ledger, literal)| format!("  - {ledger}  ->  {literal}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

#[test]
fn the_evidence_path_scanner_finds_both_anchorings_and_stops_at_markup() {
    // Synthetic non-vacuity for the scanner itself. If this ever goes quiet, the
    // test above would pass by finding nothing rather than by finding nothing
    // wrong, which is the failure mode this repository's testing rules forbid.
    let body = "\
row cites `engine/docs2/4_EVIDENCE/root_form.md` and a link \
[`4_EVIDENCE/dir_form.md`](4_EVIDENCE/dir_form.md), then ends a sentence with \
engine/docs2/4_EVIDENCE/trailing_period.md. Also docs2/evidence/legacy_tree.md here.";

    let from_root_ledger = cited_evidence_paths("", body);
    let root_anchored: BTreeSet<&str> = from_root_ledger
        .iter()
        .map(|(_, repo_relative)| repo_relative.as_str())
        .collect();

    // The `engine/docs2/`-prefixed forms anchor at the repository root.
    assert!(root_anchored.contains("engine/docs2/4_EVIDENCE/root_form.md"));
    // A trailing sentence period is not part of the filename.
    assert!(root_anchored.contains("engine/docs2/4_EVIDENCE/trailing_period.md"));
    assert!(!root_anchored.contains("engine/docs2/4_EVIDENCE/trailing_period.md."));
    // The legacy lowercase tree is scanned too, and the bare `engine/` that
    // precedes its marker is restored rather than dropped. Regression pin: the
    // first version of this scanner reported `docs2/evidence/legacy_tree.md`,
    // which resolves nowhere, and so failed the gate on a receipt that exists.
    assert!(root_anchored.contains("engine/docs2/evidence/legacy_tree.md"));
    assert!(!root_anchored.contains("docs2/evidence/legacy_tree.md"));
    // Backticks and brackets end a token rather than being swallowed into it.
    assert!(root_anchored
        .iter()
        .all(|p| !p.contains('`') && !p.contains('[') && !p.contains(']') && !p.contains('(')));

    // The same bare token anchors under the ledger's own directory when the
    // ledger does not sit at the repository root — this is the FEATURES.md form,
    // and getting it wrong would report every FEATURES.md citation as missing.
    let from_nested_ledger = cited_evidence_paths("engine/docs2", body);
    let nested_anchored: BTreeSet<&str> = from_nested_ledger
        .iter()
        .map(|(_, repo_relative)| repo_relative.as_str())
        .collect();
    assert!(nested_anchored.contains("engine/docs2/4_EVIDENCE/dir_form.md"));
}

#[test]
fn the_evidence_path_scanner_skips_globs_and_templates_rather_than_failing_on_them() {
    // A ledger names shapes as well as files. If any of these were reported, the
    // gate would be un-greenable and the predictable repair — a blanket exclusion —
    // would also swallow the real defects. See `is_path_char` for why they are
    // skipped: the scan stops at the metacharacter, and what survives has no `.md`.
    let shapes = "\
results live under `engine/docs2/4_EVIDENCE/**`, a receipt is named \
`engine/docs2/4_EVIDENCE/<lane>_receipt.md`, and a templated one is \
`engine/docs2/4_EVIDENCE/${LANE}_receipt.md` plus a glob \
`engine/docs2/4_EVIDENCE/2026_08_0?_*.md`.";

    let found = cited_evidence_paths("", shapes);
    assert!(
        found.is_empty(),
        "glob/template shapes must not be reported as citations, got: {found:?}"
    );

    // Control: the identical sentence shape with a real filename IS reported, so
    // this test proves the metacharacters are what caused the skip — not that the
    // scanner simply fails to look at sentences of this form.
    let concrete = "a receipt is named `engine/docs2/4_EVIDENCE/real_receipt.md`.";
    assert_eq!(cited_evidence_paths("", concrete).len(), 1);
}
