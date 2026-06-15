# uatu

Observe cron jobs without replacing cron.

Uatu is a single-binary wrapper you put in front of your existing crontab
entries. Your job's output still reaches cron byte-for-byte, the exit code is
preserved, and the environment is untouched — but every run is recorded
locally, and you can get Discord or email alerts on failure, recovery, stale
runs, and long runs.

```cron
0 2 * * * /usr/local/bin/uatu run --name nightly-backup -- /usr/local/bin/backup
*/10 * * * * /usr/local/bin/uatu flush
```

The foundational invariant: **wrapping a job with uatu must never make it
behave differently than the bare cron line.** No observability failure —
config, disk, database, network — may prevent or alter the job's execution.
When something in uatu breaks, the job still runs and uatu degrades.

> [!IMPORTANT]
> **Uatu cannot detect runs that never start.** It only observes runs that
> begin. If your crontab line has a typo, the cron daemon is dead, or the host
> is offline, uatu will tell you nothing — there is no "nightly-backup hasn't
> run in 3 days" alert in v1. For absence detection, pair uatu with an
> external dead-man's-switch service such as [Healthchecks.io](https://healthchecks.io)
> (or any similar ping-based monitor). This is the single biggest known blind
> spot of v1 and the first item on the roadmap.

## Install

From crates.io:

```sh
cargo install uatu
```

Or download a **fully static musl binary** (`x86_64` / `aarch64`) from GitHub
Releases — one binary that runs on any Linux of the last decade. Verify with
the published SHA-256 checksums.

Linux is the supported production platform. macOS builds may work for
development, but cron/process semantics are only guaranteed (and CI-tested) on
Linux.

## Quick start

```sh
uatu init                  # writes a starter config (~/.config/uatu/uatu.toml)
$EDITOR ~/.config/uatu/uatu.toml
uatu config validate       # strict: catches typos, bad durations, bad regexes
uatu notify test           # sends a marked test message through each reporter
uatu cron example --name nightly-backup   # crontab lines to copy, with absolute paths
```

Then inspect:

```sh
uatu history               # recent runs
uatu history --json        # stable JSON (additive-only within v1.x)
uatu show <run-id-prefix>  # one run in detail (≥4 unique chars is enough)
uatu show <run-id> --stdout --stderr
uatu status                # active and stale runs
```

Uatu works with **no config at all**: it runs local-only (history + capture,
no reporters), storing per-user state under `~/.local/state/uatu`.

## Wrapping cron lines

Prepend `uatu run --name <id> --` to simple commands. For lines that depend on
shell behavior (`cd`, pipes, redirects, `&&`, variable expansion), use shell
mode with a single quoted string:

```cron
0 1 * * * /usr/local/bin/uatu run --name cleanup --shell -- 'cd /srv/app && ./cleanup.sh | tee cleanup.log'
```

Notes that save debugging time:

- **Always use absolute paths** — cron's PATH is usually just
  `/usr/bin:/bin`. `uatu cron example` always prints the absolute path of the
  uatu binary for exactly this reason.
- Shell mode runs `$SHELL -c '<string>'` (not a login shell — cron does not
  use one either, and a login shell would silently change PATH/env). Under
  cron, `SHELL` is usually `/bin/sh`; when testing interactively your `$SHELL`
  may be a different shell with different `-c` semantics. Test shell-mode
  lines with the shell cron will actually use.
- Uatu does not read, edit, or install crontabs. `uatu cron example` only
  prints lines for you to paste.

### Job identity — use `--name` for variable arguments

Without `--name`, uatu infers a job id from the executable basename plus a
hash of the user, working directory, mode, and **the full argument list**.
That means a cron line like:

```cron
0 3 * * * uatu run -- backup --date $(date +%F)
```

produces a **new job id every day** (the argv changes daily), fragmenting
history and breaking recovery/stale logic for that job. Any job with variable
arguments must pass `--name`. `uatu history` prints a hint when it sees more
than 10 distinct inferred ids sharing one basename within 30 days.

### Exit codes

`uatu run` returns the child's exit code whenever a child started. Beyond
that, shell conventions apply: `124` configured timeout fired (GNU
`timeout(1)` parity), `125` uatu-internal pre-start failure, `126` command
found but not executable, `127` command not found, `128+N` child killed by
signal N, `2` CLI usage errors. Caveat: a child that itself exits with
124–127 is indistinguishable from these cases at the cron level; the run
status stored by uatu disambiguates (`uatu show <run-id>`).

Notification, database, capture, log, prune, or flush failures **never**
change the exit code cron sees.

## Configuration

One TOML file: `--config PATH`, else `$XDG_CONFIG_HOME/uatu/uatu.toml`
(`~/.config/uatu/uatu.toml`), else `/etc/uatu/uatu.toml`. Run
`uatu init --stdout` to see a fully commented sample.

Setting precedence: **CLI flag > job config > global config > built-in
default**. `--env` entries merge over job-config `env` key-by-key.

`uatu config validate` is strict (unknown keys are errors). At **runtime**
unknown keys only warn and execution continues — a one-character typo must not
silently turn all reporting off. Validate also flags job keys that change how
the job *executes* (`cwd`, `env`, `timeout`) rather than just how it is
observed, and warns when `expected_duration` exceeds `timeout`.

### Events and reporters

Events: `success`, `failure`, `recovery`, `stale`, `long_run`. Default:
`["success", "failure"]`. Per-reporter `events` filters intersect with the
job-effective set — "Discord gets everything, email only failures" needs no
routing matrix:

```toml
[notify]
events = ["success", "failure"]
reporters = ["discord.default", "smtp.ops"]

[reporters.discord.default]
webhook_url = "https://discord.com/api/webhooks/..."

[reporters.smtp.ops]
host = "smtp.example.com"
tls = "starttls"            # "starttls" | "smtps" | "none" (explicit, localhost only)
from = "uatu@example.com"
recipients = ["ops@example.com"]
events = ["failure", "recovery", "stale"]   # email only wakes people for problems
```

> [!TIP]
> **For frequent jobs (`*/5` health checks), don't alert on success** — that
> trains everyone to mute the channel. Use the quiet profile:
> `events = ["failure", "recovery"]`. `recovery` fires when a run succeeds
> after a failure/timeout/stale/start-failure, independently of whether
> `success` is enabled.

Timeouts report as `failure` events with timeout detail. `--expected-duration`
sends one mid-run `long_run` alert (the CLI flag implies the alert; from
config, add `long_run` to the job's `events`).

### Delivery model

`uatu run` sends its own alerts synchronously at run end, hard-capped (10s per
reporter, 30s overall) so the wrapper never lingers — anything unfinished is
queued. `uatu flush` retries the queue (backoff: 1m, 5m, 25m, 2h, then 6h,
±20% jitter; Discord `Retry-After` honored; 7-day expiry). Inspection commands
(`history`, `show`, `status`) reconcile and *enqueue* but **never** touch the
network.

Delivery is **at-least-once**: a timeout after the remote accepted can produce
a duplicate on retry. Duplicates are possible by design; alerts are never
silently dropped. Retried notifications are clearly marked delayed, with both
the event time and the delivery time.

> [!NOTE]
> Stale-run alerts are detected at reconcile time and delivered by `run`/
> `flush` — their latency is bounded by your flush cadence. That is why the
> recommended crontab includes `*/10 * * * * /usr/local/bin/uatu flush`.

## Output capture

Raw passthrough to cron is immediate and unconditional. Capture happens on a
separate path (line assembly → redaction → disk) and can never block or slow
the passthrough.

Default mode `capped` is bounded by construction: the first 64 KiB (head) is
written as the run progresses, the last 1 MiB (tail) is kept in memory and
written at run end, with a marker recording omitted bytes between them.
Consequences worth knowing:

- If the wrapper dies mid-run, the in-memory tail is lost — stale runs may
  have **head-only capture**.
- `capture_mode = "full"` is an explicit opt-in footgun: a runaway job can
  fill the disk during the run (retention only prunes afterwards). The free-
  space preflight (`min_free_bytes`, default 100MB) disables capture for new
  runs on a nearly-full disk; mid-run write errors degrade capture for that
  stream only. The job itself is never touched.

Retention defaults: 30 days, 1 GB of captured output (oldest output deleted
first; run metadata is kept so history stays explainable). Runs after every
`run`/`flush`, or manually via `uatu prune [--dry-run]`.

## Redaction

Configured literals and regexes (plus automatic literals for your webhook
URLs and SMTP passwords) are applied **before anything is stored or sent**:
captured output, the stored argv/shell string, the operational log, Discord
and email messages. Environment variable *values* are never stored at all —
names only. Raw passthrough to cron is intentionally not redacted.

Limitations (documented, by design): rules are single-line, applied to raw
bytes line-by-line with a 1 MiB assembly cap — a secret straddling a
fragment boundary of an enormous unbroken line can escape; multi-line secrets
are out of scope for v1. **Invalid redaction config fails safe**: the job
still runs, but capture and reporters are disabled for that run
(metadata-only) and `uatu config validate` exits nonzero.

## Failure policy (what breaks when things break)

| Problem | Behavior |
| --- | --- |
| No config | local-only (history + capture, no reporters) |
| Invalid TOML / bad table | local-only / that table defaults; warning to stderr + log |
| Unknown config key | runtime: warn and continue; `validate`: hard error |
| Invalid redaction | run executes; metadata-only; reporters disabled |
| State dir / DB unavailable | passthrough: no history/capture/queue/prune; child unaffected |
| DB newer than binary | same passthrough, with an "upgrade uatu" message |
| Low disk (preflight) | metadata-only for that run, warning |
| Mid-run capture I/O error | that stream degrades; child never signaled/blocked |
| Reporter down | bounded sync attempt, then queued for `flush`; exit code unchanged |
| Wrapper killed (TERM/INT/HUP) | child group gets TERM→KILL, real result recorded, events enqueued |
| Wrapper crash / power loss | run marked `stale` at next reconcile, stale alert queued |

Stale detection is pid-reuse- and reboot-proof: liveness is pid **plus**
`/proc` start time **plus** kernel boot id, never a bare `kill(pid, 0)`.

## State

Per Unix user by default: `~/.local/state/uatu` (override with `--data-dir`
or `global.data_dir`). Contains the SQLite database (WAL mode), captured
output (`output/<job>/<run>/`), the JSONL operational log, and the flush
lock. Directories are 0700, files 0600.

The state directory must be on a **local filesystem** — SQLite and `flock`
over NFS are unsupported.

## JSON output

`--json` (on `history`, `show`, `status`) is stable but unversioned:
additive-only within v1.x — fields are never renamed, removed, or re-typed.
Timestamps are RFC 3339 UTC strings, durations integer milliseconds, byte
counts integers, enums snake_case, absent values `null`. Feature-detect by
key presence.

## Not in v1 (deliberately)

Missed-run detection (see the warning at the top), daemons/schedulers,
crontab management, TUI/watch mode, stopping active jobs, overlap alerts
(overlapping runs are allowed and recorded), deb/rpm packages, shell
completions, system-wide multi-user state, Discord/email file attachments,
built-in generic secret detectors.

## Development

```sh
cargo test          # unit + integration + reporter tests (Linux)
cargo clippy --all-targets
```

CI runs the test suite and builds the static musl release targets. Merging a
release PR publishes `x86_64-unknown-linux-musl` and
`aarch64-unknown-linux-musl` binaries with SHA-256 checksums.

License: Apache-2.0.
