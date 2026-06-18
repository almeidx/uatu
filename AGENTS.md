# AGENTS.md

uatu is a single-binary Rust CLI that wraps cron jobs: output passes
through to cron unchanged, the exit code is preserved, runs are recorded in
SQLite, and Discord/SMTP alerts are sent. Linux is the supported platform.

## The one rule that outranks everything

**Wrapping a job must never make it behave differently than the bare cron
line.** No uatu failure — config, disk, database, network, reporter — may
prevent or alter the child's execution, its output bytes, or the exit code
cron sees. When uatu breaks, the job still runs and uatu degrades
(passthrough mode, metadata-only mode, queued deliveries). If a change
trades this property for anything else, the change is wrong.

Practical consequences:
- `uatu run` returns the child's exit code whenever a child started.
  Reserved codes (only when the child did NOT produce them): 124 timeout,
  125 uatu pre-start failure, 126 not executable, 127 not found,
  128+N child killed by signal N, 2 CLI usage.
- Panics anywhere in the post-child path (delivery, capture finalize,
  pruning) are bugs: they replace the child's exit code.
- Inspection commands (`history`, `show`, `status`) must never perform
  network I/O; only `run` and `flush` deliver notifications.
- Raw passthrough is never gated on capture/redaction work.

## Build / test / lint (CI runs exactly these)

    cargo test                                  # unit + integration + reporter
    cargo clippy --all-targets -- -D warnings   # zero warnings policy
    cargo fmt --check

All three must pass before any commit is considered done. Tests are
Linux-only and run real subprocesses; they need no network (reporter tests
use in-process fake Discord/SMTP servers).

## Test harness map

- `tests/common/mod.rs` — `TestEnv` gives each test an isolated temp state
  dir and a built `uatu` binary; `FakeDiscord::start(vec![Behavior::...])`
  scripts webhook responses (Ok/RateLimited/Status/Hang); `FakeSmtp` for
  email. Prefer these over mocks.
- Send-budget timing in tests is controlled with env vars
  `UATU_PER_REPORTER_BUDGET_MS` and `UATU_OVERALL_BUDGET_MS`
  (see `src/report.rs`) — use them instead of long sleeps.
- Unit tests live in `#[cfg(test)] mod tests` inside each `src/*.rs`.

## Conventions

- Module-level `//!` docs state the module's contract and cite spec
  sections ("SPEC §7"); keep citations accurate when changing behavior.
- Error policy by command family: the `run` path warns to stderr and
  degrades (never fails the job); inspection/maintenance commands print
  `uatu: error: ...` and exit nonzero. State errors use `db::StateError`.
- File modes are part of the contract: state dir and per-run output dirs
  0700, db/oplog/lock/output files 0600.
- Redaction applies to everything stored or sent (captured output, argv,
  oplog, notifications) — but never to the raw passthrough. Env var values
  are never stored, names only.
- `--json` output is a stable contract: additive-only within v1.x — never
  rename, remove, or re-type a field. Timestamps RFC 3339 UTC; durations
  integer milliseconds; enums snake_case.

## Where things live

- `src/commands/run.rs` — the wrapper itself (spawn, process group,
  passthrough pump, timeout TERM→KILL, capture coordination, delivery).
  The most invariant-critical file in the repo; change with tests.
- `src/db.rs` — SQLite schema (forward-only migrations via
  `user_version`), run rows, delivery state machine, retention.
- `src/report.rs` — Discord/SMTP senders, backoff/retry/expiry.
- `src/events.rs` — event derivation and message building.
- `src/capture.rs` / `src/redact.rs` — capped capture and redaction.
- `src/reconcile.rs` + `src/liveness.rs` — stale-run detection
  (pid + /proc start time + boot id; never bare `kill(pid, 0)`).
- `src/prompt.rs` — the `Ui` prompt trait with two backends: `TermUi`
  (`inquire`-backed terminal UI) when stdin/stdout are a TTY, and `LinePrompt`
  (line-at-a-time, generic over `BufRead`/`Write`) for pipes, CI, and tests, so
  the wizard stays scriptable. Esc → `Cancel`, Ctrl-C → `Abort`.
- `src/commands/configure.rs` — `config wizard` / `init --interactive`: the
  menu-driven wizard plus the `Config`→TOML renderer (round-trip-tested against
  the config parser). Writes 0600, backs up any existing file, and reuses
  `notify test` for the optional end-of-wizard test send.

## Spec

The authoritative v1 spec (with a decision log) lives outside the repo at
`~/Documents/Notes/Projects/Uatu/SPEC.md` on the maintainer's
machine. If that path doesn't exist in your environment, treat README.md as
the contract; do not invent requirements beyond it.
