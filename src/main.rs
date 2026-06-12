//! `uatu` CLI (SPEC §3). Usage errors exit 2 (clap convention) before any
//! child is started.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

use uatu::commands::flush::{cmd_flush, FlushArgs};
use uatu::commands::inspect::{
    cmd_history, cmd_show, cmd_status, HistoryArgs, ShowArgs, StatusArgs,
};
use uatu::commands::maintain::{
    cmd_cron_example, cmd_init, cmd_notify_test, cmd_prune, cmd_validate, CronExampleArgs,
    InitArgs, NotifyTestArgs, PruneArgs, ValidateArgs,
};
use uatu::commands::run::{cmd_run, RunArgs};

#[derive(Parser)]
#[command(
    name = "uatu",
    version,
    about = "Observe cron jobs without replacing cron",
    long_about = "Uatu wraps cron commands: output streams through unchanged, the exit code is\n\
                  preserved, and runs are recorded locally with optional Discord/SMTP alerts.\n\
                  Wrapping a job must never make it behave differently than the bare cron line."
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a command under observation (use inside crontab)
    Run(RunCli),
    /// Retry queued notifications, reconcile stale runs, prune
    Flush(CommonCli),
    /// Show recent runs
    History(HistoryCli),
    /// Show one run in detail (accepts a unique run-id prefix, ≥4 chars)
    Show(ShowCli),
    /// Show active and stale runs
    Status(StatusCli),
    /// Apply retention to captured output and run metadata
    Prune(PruneCli),
    /// Generate a starter config file
    Init(InitCli),
    /// Config tools
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    /// Crontab helpers
    Cron {
        #[command(subcommand)]
        cmd: CronCmd,
    },
    /// Notification tools
    Notify {
        #[command(subcommand)]
        cmd: NotifyCmd,
    },
}

#[derive(Subcommand)]
enum ConfigCmd {
    /// Parse and validate the config file (strict: unknown keys are errors)
    Validate(ValidateCli),
}

#[derive(Subcommand)]
enum CronCmd {
    /// Print example crontab lines (with absolute paths)
    Example(CronExampleCli),
}

#[derive(Subcommand)]
enum NotifyCmd {
    /// Send a test notification through the real delivery path
    Test(NotifyTestCli),
}

fn parse_name(s: &str) -> Result<String, String> {
    if uatu::identity::valid_slug(s) {
        Ok(s.to_string())
    } else {
        Err(format!(
            "invalid job name {s:?}: must match ^[A-Za-z0-9._-]+$"
        ))
    }
}

fn parse_env_kv(s: &str) -> Result<(String, String), String> {
    let Some((k, v)) = s.split_once('=') else {
        return Err(format!("invalid --env {s:?}: expected K=V"));
    };
    let valid = !k.is_empty()
        && k.bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && k.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if !valid {
        return Err(format!("invalid environment variable name {k:?}"));
    }
    Ok((k.to_string(), v.to_string()))
}

fn parse_cli_duration(s: &str) -> Result<Duration, String> {
    uatu::util::parse_duration(s)
}

#[derive(Args)]
struct RunCli {
    /// Stable job id (default: inferred from the command line)
    #[arg(long, value_parser = parse_name)]
    name: Option<String>,
    /// Treat the single argument after -- as a shell command string ($SHELL -c)
    #[arg(long)]
    shell: bool,
    /// Override config file path
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    /// Override state directory
    #[arg(long = "data-dir", value_name = "PATH")]
    data_dir: Option<PathBuf>,
    /// Working directory for the child
    #[arg(long, value_name = "PATH")]
    cwd: Option<PathBuf>,
    /// Add or override an environment variable (repeatable)
    #[arg(long = "env", value_name = "K=V", value_parser = parse_env_kv)]
    env: Vec<(String, String)>,
    /// Maximum run duration (e.g. 30m); TERM then KILL on expiry, exit 124
    #[arg(long, value_name = "DURATION", value_parser = parse_cli_duration)]
    timeout: Option<Duration>,
    /// TERM-to-KILL grace period (default 30s)
    #[arg(long = "kill-grace", value_name = "DURATION", value_parser = parse_cli_duration)]
    kill_grace: Option<Duration>,
    /// Send one long-run alert if the run exceeds this duration
    #[arg(long = "expected-duration", value_name = "DURATION", value_parser = parse_cli_duration)]
    expected_duration: Option<Duration>,
    /// The command to run (after --)
    #[arg(last = true, required = true, value_name = "CMD")]
    cmd: Vec<OsString>,
}

#[derive(Args)]
struct CommonCli {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    #[arg(long = "data-dir", value_name = "PATH")]
    data_dir: Option<PathBuf>,
}

#[derive(Args)]
struct HistoryCli {
    #[command(flatten)]
    common: CommonCli,
    /// Maximum runs to show
    #[arg(long, default_value_t = 20)]
    limit: usize,
    /// Filter by job id
    #[arg(long, value_name = "SLUG")]
    job: Option<String>,
    /// Filter by status
    #[arg(long, value_parser = ["active", "success", "failure", "timeout", "stale", "start_failed"])]
    status: Option<String>,
    /// JSON output (stable, additive-only)
    #[arg(long)]
    json: bool,
}

fn parse_run_prefix(s: &str) -> Result<String, String> {
    if s.len() < 4 {
        return Err("run id prefix must be at least 4 characters".to_string());
    }
    Ok(s.to_string())
}

#[derive(Args)]
struct ShowCli {
    /// Run id or unique prefix (≥4 chars)
    #[arg(value_name = "RUN_ID", value_parser = parse_run_prefix)]
    run_id: String,
    #[command(flatten)]
    common: CommonCli,
    /// Print captured stdout after the metadata
    #[arg(long)]
    stdout: bool,
    /// Print captured stderr after the metadata
    #[arg(long)]
    stderr: bool,
    /// JSON output (stable, additive-only)
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct StatusCli {
    #[command(flatten)]
    common: CommonCli,
    /// JSON output (stable, additive-only)
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct PruneCli {
    #[command(flatten)]
    common: CommonCli,
    /// Report what would be pruned without deleting anything
    #[arg(long = "dry-run")]
    dry_run: bool,
}

#[derive(Args)]
struct InitCli {
    /// Target path (default: $XDG_CONFIG_HOME/uatu/uatu.toml)
    #[arg(long, value_name = "PATH")]
    path: Option<PathBuf>,
    /// Print the sample config instead of writing a file
    #[arg(long)]
    stdout: bool,
    /// Overwrite an existing file
    #[arg(long)]
    force: bool,
}

#[derive(Args)]
struct ValidateCli {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
}

#[derive(Args)]
struct CronExampleCli {
    /// Job name to use in the example
    #[arg(long, value_parser = parse_name)]
    name: Option<String>,
    /// Emit a shell-mode example
    #[arg(long)]
    shell: bool,
}

#[derive(Args)]
struct NotifyTestCli {
    /// Test only this reporter (e.g. discord.default); default: all
    #[arg(long, value_name = "NAME")]
    reporter: Option<String>,
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    #[arg(long = "data-dir", value_name = "PATH")]
    data_dir: Option<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let code = match cli.cmd {
        Cmd::Run(a) => cmd_run(RunArgs {
            name: a.name,
            shell: a.shell,
            config: a.config,
            data_dir: a.data_dir,
            cwd: a.cwd,
            env: a.env,
            timeout: a.timeout,
            kill_grace: a.kill_grace,
            expected_duration: a.expected_duration,
            cmd: a.cmd,
        }),
        Cmd::Flush(a) => cmd_flush(FlushArgs {
            config: a.config,
            data_dir: a.data_dir,
        }),
        Cmd::History(a) => cmd_history(HistoryArgs {
            config: a.common.config,
            data_dir: a.common.data_dir,
            limit: a.limit,
            job: a.job,
            status: a.status,
            json: a.json,
        }),
        Cmd::Show(a) => cmd_show(ShowArgs {
            config: a.common.config,
            data_dir: a.common.data_dir,
            run_id: a.run_id,
            stdout: a.stdout,
            stderr: a.stderr,
            json: a.json,
        }),
        Cmd::Status(a) => cmd_status(StatusArgs {
            config: a.common.config,
            data_dir: a.common.data_dir,
            json: a.json,
        }),
        Cmd::Prune(a) => cmd_prune(PruneArgs {
            config: a.common.config,
            data_dir: a.common.data_dir,
            dry_run: a.dry_run,
        }),
        Cmd::Init(a) => cmd_init(InitArgs {
            path: a.path,
            stdout: a.stdout,
            force: a.force,
        }),
        Cmd::Config {
            cmd: ConfigCmd::Validate(a),
        } => cmd_validate(ValidateArgs { config: a.config }),
        Cmd::Cron {
            cmd: CronCmd::Example(a),
        } => cmd_cron_example(CronExampleArgs {
            name: a.name,
            shell: a.shell,
        }),
        Cmd::Notify {
            cmd: NotifyCmd::Test(a),
        } => cmd_notify_test(NotifyTestArgs {
            reporter: a.reporter,
            config: a.config,
            data_dir: a.data_dir,
        }),
    };
    ExitCode::from(code.clamp(0, 255) as u8)
}
