//! Shared integration-test helpers: a fake Discord webhook HTTP server, a
//! fake SMTP server, and a per-test environment around the uatu binary.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub fn uatu_bin() -> &'static str {
    env!("CARGO_BIN_EXE_uatu")
}

/// Per-test sandbox: state dir, optional config, isolated env.
pub struct TestEnv {
    pub dir: tempfile::TempDir,
}

impl TestEnv {
    pub fn new() -> TestEnv {
        TestEnv {
            dir: tempfile::tempdir().expect("tempdir"),
        }
    }

    pub fn state_dir(&self) -> PathBuf {
        self.dir.path().join("state")
    }

    pub fn config_path(&self) -> PathBuf {
        self.dir.path().join("uatu.toml")
    }

    pub fn write_config(&self, content: &str) {
        std::fs::write(self.config_path(), content).expect("write config");
    }

    /// Build a uatu Command with isolated env. Inserts --data-dir/--config
    /// (where the subcommand supports them) BEFORE any `--` separator.
    pub fn cmd(&self, args: &[&str]) -> Command {
        let mut c = Command::new(uatu_bin());
        // Isolate from the developer's real config and shell.
        c.env("XDG_CONFIG_HOME", self.dir.path().join("xdg-config"));
        c.env("XDG_STATE_HOME", self.dir.path().join("xdg-state"));
        c.env_remove("SHELL");
        c.env_remove("UATU_PER_REPORTER_BUDGET_MS");
        c.env_remove("UATU_OVERALL_BUDGET_MS");

        let (wants_data_dir, wants_config) = match args.first().copied() {
            Some("init") | Some("cron") => (false, false),
            Some("config") => (false, true),
            _ => (true, true),
        };
        let split = args.iter().position(|a| *a == "--").unwrap_or(args.len());
        c.args(&args[..split]);
        if wants_data_dir {
            c.arg("--data-dir").arg(self.state_dir());
        }
        if wants_config && self.config_path().exists() {
            c.arg("--config").arg(self.config_path());
        }
        c.args(&args[split..]);
        c
    }

    pub fn run_ok(&self, args: &[&str]) -> Output {
        let out = self.cmd(args).output().expect("spawn uatu");
        assert!(
            out.status.success(),
            "uatu {args:?} failed (code {:?})\nstdout: {}\nstderr: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        out
    }

    pub fn run_code(&self, args: &[&str]) -> (i32, String, String) {
        let out = self.cmd(args).output().expect("spawn uatu");
        (
            out.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    /// Build a uatu Command with the isolated env but WITHOUT the automatic
    /// `--data-dir`/`--config` insertion `cmd` does. Tests that need to control
    /// those flags exactly (e.g. the interactive wizard) use this.
    pub fn cmd_raw(&self, args: &[&str]) -> Command {
        let mut c = Command::new(uatu_bin());
        c.env("XDG_CONFIG_HOME", self.dir.path().join("xdg-config"));
        c.env("XDG_STATE_HOME", self.dir.path().join("xdg-state"));
        c.env_remove("SHELL");
        c.env_remove("UATU_PER_REPORTER_BUDGET_MS");
        c.env_remove("UATU_OVERALL_BUDGET_MS");
        c.args(args);
        c
    }

    /// The wizard's default target under this sandbox's isolated XDG config.
    pub fn xdg_config_target(&self) -> PathBuf {
        self.dir
            .path()
            .join("xdg-config")
            .join("uatu")
            .join("uatu.toml")
    }

    /// Run a command feeding `input` on stdin to EOF, capturing everything.
    /// stdin is written from a separate thread so the child's stdout/stderr
    /// are drained concurrently — a single blocking write would deadlock if the
    /// child's output exceeded the pipe buffer before consuming all input.
    pub fn run_input(&self, mut cmd: Command, input: &str) -> (i32, String, String) {
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn uatu");
        let mut stdin = child.stdin.take().expect("stdin pipe");
        let bytes = input.as_bytes().to_vec();
        let writer = std::thread::spawn(move || {
            let _ = stdin.write_all(&bytes);
            // Dropping stdin here closes the pipe, signalling EOF to the child.
        });
        let out = child.wait_with_output().expect("wait uatu");
        let _ = writer.join();
        (
            out.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    /// Open the test sandbox's database directly via the uatu library.
    pub fn db(&self) -> uatu::db::Db {
        uatu::db::Db::open(&self.state_dir().join("uatu.db")).expect("open test db")
    }

    pub fn history_json(&self) -> serde_json::Value {
        let out = self.run_ok(&["history", "--json"]);
        serde_json::from_slice(&out.stdout).expect("history json")
    }

    pub fn latest_run(&self) -> serde_json::Value {
        self.history_json()
            .as_array()
            .and_then(|a| a.first().cloned())
            .expect("at least one run")
    }

    pub fn show_json(&self, run_id: &str) -> serde_json::Value {
        let out = self.run_ok(&["show", run_id, "--json"]);
        serde_json::from_slice(&out.stdout).expect("show json")
    }
}

/// Insert a fake run row (for stale/reconcile tests).
#[allow(clippy::too_many_arguments)]
pub fn insert_run_row(
    db: &uatu::db::Db,
    run_id: &str,
    job_id: &str,
    status: &str,
    start_ms: i64,
    wrapper_pid: i64,
    wrapper_start_ticks: i64,
    boot_id: &str,
) {
    let row = uatu::db::RunRow {
        run_id: run_id.into(),
        job_id: job_id.into(),
        job_id_inferred: false,
        inferred_basename: None,
        mode: "direct".into(),
        argv_json: Some("[\"fake\"]".into()),
        shell_cmd: None,
        cwd: None,
        env_names_json: None,
        host: "test-host".into(),
        schedule_label: None,
        status: status.into(),
        start_ms,
        end_ms: None,
        end_is_detection: false,
        exit_code: None,
        signal_no: None,
        timeout_fired: false,
        interrupted_by: None,
        start_error: None,
        wrapper_pid,
        wrapper_start_ticks,
        boot_id: boot_id.into(),
        child_pid: Some(4242),
        expected_duration_ms: None,
        long_run_fired: false,
        detached_children: false,
        stdout: uatu::db::CaptureMeta::default(),
        stderr: uatu::db::CaptureMeta::default(),
        output_pruned_ms: None,
    };
    db.insert_run(&row).expect("insert fake run");
}

// ---------------------------------------------------------------------------
// Fake Discord webhook server
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub enum Behavior {
    /// 204 No Content (Discord's success response).
    Ok,
    /// 429 with Retry-After (seconds).
    RateLimited(u64),
    /// Arbitrary status code.
    Status(u16),
    /// Sleep before answering (to blow the send budget).
    Hang(Duration),
    /// 429 with a raw (possibly malformed) Retry-After header value.
    RateLimitedRaw(&'static str),
}

pub struct FakeDiscord {
    pub addr: SocketAddr,
    pub hits: Arc<Mutex<Vec<serde_json::Value>>>,
    script: Arc<Mutex<VecDeque<Behavior>>>,
}

impl FakeDiscord {
    /// Start a server; `script` lists per-request behaviors (default Ok once
    /// the script is exhausted).
    pub fn start(script: Vec<Behavior>) -> FakeDiscord {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake discord");
        let addr = listener.local_addr().unwrap();
        let hits: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let script = Arc::new(Mutex::new(VecDeque::from(script)));
        let h = Arc::clone(&hits);
        let s = Arc::clone(&script);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let behavior = s.lock().unwrap().pop_front().unwrap_or(Behavior::Ok);
                let h = Arc::clone(&h);
                std::thread::spawn(move || handle_http(stream, behavior, h));
            }
        });
        FakeDiscord { addr, hits, script }
    }

    pub fn url(&self) -> String {
        format!("http://{}/api/webhooks/1/test-token", self.addr)
    }

    pub fn hit_count(&self) -> usize {
        self.hits.lock().unwrap().len()
    }

    pub fn payloads(&self) -> Vec<serde_json::Value> {
        self.hits.lock().unwrap().clone()
    }

    /// First embed of hit `i`.
    pub fn embed(&self, i: usize) -> serde_json::Value {
        self.payloads()[i]["embeds"][0].clone()
    }
}

fn handle_http(
    mut stream: TcpStream,
    behavior: Behavior,
    hits: Arc<Mutex<Vec<serde_json::Value>>>,
) {
    stream.set_read_timeout(Some(Duration::from_secs(20))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut content_length = 0usize;
    let mut line = String::new();
    // request line + headers
    loop {
        line.clear();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let l = line.trim_end();
        if l.is_empty() {
            break;
        }
        if let Some((k, v)) = l.split_once(':') {
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).ok();
    }
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body) {
        hits.lock().unwrap().push(v);
    }
    let response = match behavior {
        Behavior::Ok => "HTTP/1.1 204 No Content\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".to_string(),
        Behavior::RateLimited(secs) => format!(
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: {secs}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
        ),
        Behavior::Status(code) => format!(
            "HTTP/1.1 {code} Oops\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
        ),
        Behavior::Hang(d) => {
            std::thread::sleep(d);
            "HTTP/1.1 204 No Content\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".to_string()
        }
        Behavior::RateLimitedRaw(v) => format!(
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: {v}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
        ),
    };
    let _ = stream.write_all(response.as_bytes());
}

// ---------------------------------------------------------------------------
// Fake SMTP server
// ---------------------------------------------------------------------------

pub struct FakeSmtp {
    pub addr: SocketAddr,
    pub messages: Arc<Mutex<Vec<String>>>,
}

impl FakeSmtp {
    pub fn start() -> FakeSmtp {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake smtp");
        let addr = listener.local_addr().unwrap();
        let messages: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let m = Arc::clone(&messages);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let m = Arc::clone(&m);
                std::thread::spawn(move || handle_smtp(stream, m));
            }
        });
        FakeSmtp { addr, messages }
    }

    pub fn message_count(&self) -> usize {
        self.messages.lock().unwrap().len()
    }

    pub fn last_message(&self) -> String {
        self.messages
            .lock()
            .unwrap()
            .last()
            .cloned()
            .unwrap_or_default()
    }

    /// Last message with quoted-printable soft breaks and =XX escapes decoded
    /// (lettre QP-encodes plain-text bodies, wrapping at 76 columns).
    pub fn last_message_decoded(&self) -> String {
        decode_qp(&self.last_message())
    }
}

pub fn decode_qp(s: &str) -> String {
    let s = s.replace("=\r\n", "").replace("=\n", "");
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'='
            && i + 2 < bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
        {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
            out.push(u8::from_str_radix(hex, 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn handle_smtp(mut stream: TcpStream, messages: Arc<Mutex<Vec<String>>>) {
    stream.set_read_timeout(Some(Duration::from_secs(20))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone smtp stream"));
    let mut send = |s: &str| {
        let _ = stream.write_all(s.as_bytes());
    };
    send("220 fake.test ESMTP ready\r\n");
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let upper = line.trim_end().to_ascii_uppercase();
        if upper.starts_with("EHLO") || upper.starts_with("HELO") {
            send("250-fake.test greets you\r\n250 8BITMIME\r\n");
        } else if upper.starts_with("MAIL FROM") || upper.starts_with("RCPT TO") {
            send("250 OK\r\n");
        } else if upper.starts_with("DATA") {
            send("354 go ahead\r\n");
            let mut data = String::new();
            loop {
                line.clear();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    return;
                }
                if line == ".\r\n" || line == ".\n" {
                    break;
                }
                data.push_str(&line);
            }
            messages.lock().unwrap().push(data);
            send("250 queued\r\n");
        } else if upper.starts_with("QUIT") {
            send("221 bye\r\n");
            return;
        } else {
            send("250 OK\r\n");
        }
    }
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

/// Spawn a uatu run that lingers (for status/interrupt tests).
pub fn spawn_sleeper(env: &TestEnv, secs: &str, extra: &[&str]) -> std::process::Child {
    let mut c = env.cmd(&["run"]);
    c.args(extra);
    c.arg("--").arg("sleep").arg(secs);
    c.stdout(Stdio::null()).stderr(Stdio::null());
    c.spawn().expect("spawn sleeper")
}

pub fn wait_until<F: FnMut() -> bool>(timeout: Duration, mut cond: F) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    false
}
