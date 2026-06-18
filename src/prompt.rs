//! Interactive prompts for the configuration wizard (`config wizard` /
//! `init --interactive`).
//!
//! Two backends behind one [`Ui`] trait:
//!
//! - [`TermUi`] — a raw-mode terminal UI with a highlighted cursor moved by the
//!   arrow keys or `j`/`k`, Enter to choose, Space to toggle multi-selects.
//!   **Esc cancels the current action and goes back; Ctrl-C aborts the wizard.**
//!   Used when stdin and stdout are both a TTY.
//! - [`LinePrompt`] — a dependency-light, line-at-a-time reader (type the option
//!   number / `y`/`n`, press Enter). Used for pipes, redirected input, CI, and
//!   every test. Generic over `BufRead`/`Write` so it runs against in-memory
//!   buffers; it never produces a cancel/abort signal.
//!
//! Input methods return [`PromptResult`]: `Err(PromptError::Cancel)` for Esc
//! (the wizard returns to its menu) and `Err(PromptError::Abort)` for Ctrl-C
//! (the wizard exits without saving). [`cmd_configure`](crate::commands::configure)
//! picks the backend with [`stdio_is_tty`]. Blank input selects the default.

use std::io::{self, BufRead, Write};
use std::os::unix::io::RawFd;

/// Outcome of an input prompt: a value, a "go back" (Esc), or a "quit the
/// wizard" (Ctrl-C). I/O failures are carried as `Io`.
#[derive(Debug)]
pub enum PromptError {
    Io(io::Error),
    /// Esc — cancel the current action and return to the menu.
    Cancel,
    /// Ctrl-C — abandon the wizard without saving.
    Abort,
}

impl From<io::Error> for PromptError {
    fn from(e: io::Error) -> Self {
        PromptError::Io(e)
    }
}

pub type PromptResult<T> = Result<T, PromptError>;

/// The prompts the wizard needs. Generic methods keep it usable by static
/// dispatch (`run_wizard<U: Ui>`); it is intentionally not object-safe.
pub trait Ui {
    fn say(&mut self, line: &str) -> io::Result<()>;
    fn at_eof(&self) -> bool;
    fn text(&mut self, question: &str, default: Option<&str>) -> PromptResult<String>;
    fn confirm(&mut self, question: &str, default: bool) -> PromptResult<bool>;
    fn select(&mut self, question: &str, options: &[&str], default: usize) -> PromptResult<usize>;
    fn multi_select(
        &mut self,
        question: &str,
        options: &[&str],
        default_selected: &[bool],
    ) -> PromptResult<Vec<usize>>;

    fn blank(&mut self) -> io::Result<()> {
        self.say("")
    }

    /// Re-ask until `validate` accepts. On EOF the (defaulted) answer is
    /// returned even if invalid, so a truncated stream can never wedge a loop.
    /// Esc/Ctrl-C propagate out unchanged.
    fn text_validated<F>(
        &mut self,
        question: &str,
        default: Option<&str>,
        mut validate: F,
    ) -> PromptResult<String>
    where
        F: FnMut(&str) -> Result<(), String>,
    {
        loop {
            let answer = self.text(question, default)?;
            match validate(&answer) {
                Ok(()) => return Ok(answer),
                Err(e) => {
                    self.say(&format!("  ! {e}"))?;
                    if self.at_eof() {
                        return Ok(answer);
                    }
                }
            }
        }
    }
}

/// True only when stdin and stdout are both terminals — the condition for the
/// raw-mode [`TermUi`]. Redirected input/output falls back to [`LinePrompt`].
pub fn stdio_is_tty() -> bool {
    unsafe { libc::isatty(libc::STDIN_FILENO) == 1 && libc::isatty(libc::STDOUT_FILENO) == 1 }
}

// ---------------------------------------------------------------------------
// Line-based backend (pipes / tests) — never cancels or aborts.
// ---------------------------------------------------------------------------

pub struct LinePrompt<R: BufRead, W: Write> {
    reader: R,
    writer: W,
    eof: bool,
}

impl<R: BufRead, W: Write> LinePrompt<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        LinePrompt {
            reader,
            writer,
            eof: false,
        }
    }

    fn read_line(&mut self) -> io::Result<Option<String>> {
        if self.eof {
            return Ok(None);
        }
        let mut buf = String::new();
        if self.reader.read_line(&mut buf)? == 0 {
            self.eof = true;
            return Ok(None);
        }
        Ok(Some(buf.trim_end_matches(['\r', '\n']).to_string()))
    }

    fn put(&mut self, s: &str) -> io::Result<()> {
        self.writer.write_all(s.as_bytes())?;
        self.writer.flush()
    }
}

impl<R: BufRead, W: Write> Ui for LinePrompt<R, W> {
    fn say(&mut self, line: &str) -> io::Result<()> {
        self.put(&format!("{line}\n"))
    }

    fn at_eof(&self) -> bool {
        self.eof
    }

    fn text(&mut self, question: &str, default: Option<&str>) -> PromptResult<String> {
        let suffix = match default {
            Some(d) if !d.is_empty() => format!(" [{d}]"),
            _ => String::new(),
        };
        self.put(&format!("{question}{suffix}: "))?;
        let line = self.read_line()?.unwrap_or_default();
        let line = line.trim();
        if line.is_empty() {
            Ok(default.unwrap_or("").to_string())
        } else {
            Ok(line.to_string())
        }
    }

    fn confirm(&mut self, question: &str, default: bool) -> PromptResult<bool> {
        let hint = if default { "[Y/n]" } else { "[y/N]" };
        loop {
            self.put(&format!("{question} {hint}: "))?;
            let Some(line) = self.read_line()? else {
                return Ok(default);
            };
            match line.trim().to_ascii_lowercase().as_str() {
                "" => return Ok(default),
                "y" | "yes" => return Ok(true),
                "n" | "no" => return Ok(false),
                _ => self.say("  ! please answer y or n")?,
            }
        }
    }

    fn select(&mut self, question: &str, options: &[&str], default: usize) -> PromptResult<usize> {
        debug_assert!(!options.is_empty(), "select requires at least one option");
        let default = default.min(options.len().saturating_sub(1));
        loop {
            self.say(question)?;
            for (i, opt) in options.iter().enumerate() {
                let marker = if i == default { '>' } else { ' ' };
                self.say(&format!("  {marker} {}) {opt}", i + 1))?;
            }
            self.put(&format!("Choose 1-{} [{}]: ", options.len(), default + 1))?;
            let Some(line) = self.read_line()? else {
                return Ok(default);
            };
            let line = line.trim();
            if line.is_empty() {
                return Ok(default);
            }
            if let Ok(n) = line.parse::<usize>() {
                if (1..=options.len()).contains(&n) {
                    return Ok(n - 1);
                }
            }
            if let Some(i) = options.iter().position(|o| o.eq_ignore_ascii_case(line)) {
                return Ok(i);
            }
            self.say(&format!("  ! enter a number 1-{}", options.len()))?;
        }
    }

    fn multi_select(
        &mut self,
        question: &str,
        options: &[&str],
        default_selected: &[bool],
    ) -> PromptResult<Vec<usize>> {
        loop {
            self.say(question)?;
            for (i, opt) in options.iter().enumerate() {
                let on = default_selected.get(i).copied().unwrap_or(false);
                let mark = if on { "[x]" } else { "[ ]" };
                self.say(&format!("  {mark} {}) {opt}", i + 1))?;
            }
            self.put("Select (e.g. 1,3 / all / none) [keep defaults]: ")?;
            let Some(line) = self.read_line()? else {
                return Ok(default_indices(default_selected, options.len()));
            };
            let line = line.trim();
            if line.is_empty() {
                return Ok(default_indices(default_selected, options.len()));
            }
            match line.to_ascii_lowercase().as_str() {
                "none" => return Ok(Vec::new()),
                "all" => return Ok((0..options.len()).collect()),
                _ => {}
            }
            let mut chosen = Vec::new();
            let mut ok = true;
            for tok in line
                .split(|c: char| c == ',' || c.is_whitespace())
                .filter(|t| !t.is_empty())
            {
                match tok.parse::<usize>() {
                    Ok(n) if (1..=options.len()).contains(&n) => {
                        if !chosen.contains(&(n - 1)) {
                            chosen.push(n - 1);
                        }
                    }
                    _ => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                chosen.sort_unstable();
                return Ok(chosen);
            }
            self.say(&format!(
                "  ! enter numbers 1-{} separated by commas, or 'all'/'none'",
                options.len()
            ))?;
        }
    }
}

fn default_indices(selected: &[bool], len: usize) -> Vec<usize> {
    (0..len)
        .filter(|&i| selected.get(i).copied().unwrap_or(false))
        .collect()
}

// ---------------------------------------------------------------------------
// Terminal (raw-mode) backend
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Key {
    Up,
    Down,
    Enter,
    Space,
    Backspace,
    Esc,
    Char(u8),
    Interrupt,
    Eof,
    Other,
}

/// Decode the first key from a buffer of raw terminal bytes, returning the key
/// and how many bytes it consumed (so batched input — fast typing, paste, or a
/// test that feeds several keys at once — is decoded one key at a time). Arrow
/// keys arrive as escape sequences (`ESC [ A` etc.); a lone ESC is `Esc`.
fn decode_key(b: &[u8]) -> (Key, usize) {
    match b {
        [] => (Key::Eof, 0),
        [0x1b, b'[', rest @ ..] | [0x1b, b'O', rest @ ..] => match rest.first() {
            Some(b'A') => (Key::Up, 3),
            Some(b'B') => (Key::Down, 3),
            Some(_) => (Key::Other, 3),
            None => (Key::Other, 2),
        },
        [0x1b, ..] => (Key::Esc, 1),
        [b'\r', ..] | [b'\n', ..] => (Key::Enter, 1),
        [0x7f, ..] | [0x08, ..] => (Key::Backspace, 1),
        [b' ', ..] => (Key::Space, 1),
        [0x03, ..] => (Key::Interrupt, 1),
        [0x04, ..] => (Key::Eof, 1),
        [c, ..] => (Key::Char(*c), 1),
    }
}

fn step(cur: usize, len: usize, delta: isize) -> usize {
    if len == 0 {
        return 0;
    }
    let l = len as isize;
    (((cur as isize + delta) % l + l) % l) as usize
}

/// Remove the last UTF-8 scalar value from `buf` (pops trailing continuation
/// bytes then the lead byte), so Backspace deletes a whole character.
fn pop_utf8(buf: &mut Vec<u8>) {
    while let Some(b) = buf.pop() {
        // Stop after removing an ASCII byte or a UTF-8 lead byte (continuation
        // bytes are 0x80..0xC0).
        if !(0x80..0xc0).contains(&b) {
            break;
        }
    }
}

/// RAII raw-mode: disables canonical mode, echo, signal generation and output
/// post-processing on construction, restoring the saved settings on drop (so a
/// normal return always leaves the terminal usable). Ctrl-C is read as a byte
/// rather than a signal, so cancellation also restores cleanly.
struct RawMode {
    fd: RawFd,
    orig: libc::termios,
}

impl RawMode {
    fn enable(fd: RawFd) -> io::Result<RawMode> {
        unsafe {
            let mut t: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(fd, &mut t) != 0 {
                return Err(io::Error::last_os_error());
            }
            let orig = t;
            t.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG | libc::IEXTEN);
            t.c_iflag &= !(libc::IXON | libc::ICRNL);
            t.c_oflag &= !libc::OPOST;
            t.c_cc[libc::VMIN] = 1;
            t.c_cc[libc::VTIME] = 0;
            if libc::tcsetattr(fd, libc::TCSANOW, &t) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(RawMode { fd, orig })
        }
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        unsafe {
            libc::tcsetattr(self.fd, libc::TCSANOW, &self.orig);
        }
    }
}

fn write_all_fd(fd: RawFd, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        let n = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if n == 0 {
            break;
        }
        buf = &buf[n as usize..];
    }
    Ok(())
}

pub struct TermUi {
    in_fd: RawFd,
    out_fd: RawFd,
    /// Raw bytes read but not yet consumed: a single `read` can return several
    /// keys at once, so they are decoded one at a time from here.
    inbuf: Vec<u8>,
    eof: bool,
}

impl Default for TermUi {
    fn default() -> Self {
        Self::new()
    }
}

impl TermUi {
    pub fn new() -> TermUi {
        TermUi {
            in_fd: libc::STDIN_FILENO,
            out_fd: libc::STDOUT_FILENO,
            inbuf: Vec::new(),
            eof: false,
        }
    }

    #[cfg(test)]
    fn with_fds(in_fd: RawFd, out_fd: RawFd) -> TermUi {
        TermUi {
            in_fd,
            out_fd,
            inbuf: Vec::new(),
            eof: false,
        }
    }

    fn w(&self, s: &str) -> io::Result<()> {
        write_all_fd(self.out_fd, s.as_bytes())
    }

    /// Read one key (raw mode), refilling the buffer only when it is empty.
    fn read_key(&mut self) -> io::Result<Key> {
        if self.inbuf.is_empty() {
            let mut tmp = [0u8; 16];
            let n =
                unsafe { libc::read(self.in_fd, tmp.as_mut_ptr() as *mut libc::c_void, tmp.len()) };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            if n == 0 {
                return Ok(Key::Eof);
            }
            self.inbuf.extend_from_slice(&tmp[..n as usize]);
        }
        let (key, consumed) = decode_key(&self.inbuf);
        self.inbuf.drain(..consumed.min(self.inbuf.len()));
        Ok(key)
    }

    /// Collapse a finished prompt: move up over the `lines` it rendered, erase
    /// them, and leave only `summary` behind (empty `summary` erases the prompt
    /// entirely). This keeps the scrollback to one compact line per answer
    /// instead of a stack of full menus.
    fn finish_block(&self, lines: usize, summary: &str) -> io::Result<()> {
        let up = if lines > 0 {
            format!("\x1b[{lines}A")
        } else {
            String::new()
        };
        self.w(&format!("{up}\r\x1b[J{summary}"))
    }
}

/// A finished selection rendered as ` answer` in bold, for the collapsed line.
fn chosen_summary(question: &str, answer: &str) -> String {
    format!("{question} \x1b[1m{answer}\x1b[0m\r\n")
}

impl Ui for TermUi {
    fn say(&mut self, line: &str) -> io::Result<()> {
        self.w(&format!("{line}\n"))
    }

    fn at_eof(&self) -> bool {
        self.eof
    }

    fn text(&mut self, question: &str, default: Option<&str>) -> PromptResult<String> {
        let _raw = RawMode::enable(self.in_fd)?;
        let suffix = match default {
            Some(d) if !d.is_empty() => format!(" [{d}]"),
            _ => String::new(),
        };
        let prompt = format!("{question}{suffix}: ");
        self.w(&prompt)?;
        let mut buf: Vec<u8> = Vec::new();
        loop {
            match self.read_key()? {
                Key::Enter | Key::Eof => {
                    self.w("\r\n")?;
                    let s = String::from_utf8_lossy(&buf);
                    let s = s.trim();
                    return Ok(if s.is_empty() {
                        default.unwrap_or("").to_string()
                    } else {
                        s.to_string()
                    });
                }
                Key::Esc => {
                    self.w("\r\x1b[2K")?;
                    return Err(PromptError::Cancel);
                }
                Key::Interrupt => {
                    self.w("\r\x1b[2K")?;
                    return Err(PromptError::Abort);
                }
                Key::Backspace => pop_utf8(&mut buf),
                Key::Space => buf.push(b' '),
                Key::Char(c) if c >= 0x20 => buf.push(c),
                _ => continue,
            }
            self.w(&format!(
                "\r\x1b[2K{prompt}{}",
                String::from_utf8_lossy(&buf)
            ))?;
        }
    }

    fn confirm(&mut self, question: &str, default: bool) -> PromptResult<bool> {
        let _raw = RawMode::enable(self.in_fd)?;
        let hint = if default { "[Y/n]" } else { "[y/N]" };
        self.w(&format!("{question} {hint} (esc back) "))?;
        let result = loop {
            match self.read_key()? {
                Key::Char(b'y') | Key::Char(b'Y') => break Ok(true),
                Key::Char(b'n') | Key::Char(b'N') => break Ok(false),
                Key::Enter => break Ok(default),
                Key::Esc => break Err(PromptError::Cancel),
                Key::Interrupt => break Err(PromptError::Abort),
                Key::Eof => {
                    self.eof = true;
                    break Ok(default);
                }
                _ => {}
            }
        };
        let summary = match &result {
            Ok(b) => chosen_summary(question, if *b { "yes" } else { "no" }),
            Err(_) => String::new(),
        };
        self.finish_block(0, &summary)?;
        result
    }

    fn select(&mut self, question: &str, options: &[&str], default: usize) -> PromptResult<usize> {
        if options.is_empty() {
            return Ok(0);
        }
        let _raw = RawMode::enable(self.in_fd)?;
        let n = options.len();
        let mut sel = default.min(n - 1);
        self.w("\x1b[?25l")?;
        self.w(&format!("{question}\r\n"))?;
        self.w("  (\u{2191}/\u{2193} or j/k move \u{00b7} enter choose \u{00b7} esc back)\r\n")?;
        let mut first = true;
        let result = loop {
            if !first {
                self.w(&format!("\x1b[{n}A"))?;
            }
            first = false;
            for (i, opt) in options.iter().enumerate() {
                // Radio buttons: filled ● = current pick, hollow ○ = the rest,
                // so every line reads as a selectable option.
                if i == sel {
                    self.w(&format!("\r\x1b[2K\x1b[7m \u{25cf} {opt} \x1b[0m\r\n"))?;
                } else {
                    self.w(&format!("\r\x1b[2K \u{25cb} {opt}\r\n"))?;
                }
            }
            match self.read_key()? {
                Key::Up | Key::Char(b'k') => sel = step(sel, n, -1),
                Key::Down | Key::Char(b'j') => sel = step(sel, n, 1),
                Key::Char(c @ b'1'..=b'9') => {
                    let d = (c - b'0') as usize;
                    if d <= n {
                        sel = d - 1;
                    }
                }
                Key::Enter => break Ok(sel),
                Key::Esc => break Err(PromptError::Cancel),
                Key::Interrupt => break Err(PromptError::Abort),
                Key::Eof => {
                    self.eof = true;
                    break Ok(sel);
                }
                _ => {}
            }
        };
        let summary = match &result {
            Ok(sel) => chosen_summary(question, options[*sel]),
            Err(_) => String::new(),
        };
        self.finish_block(n + 2, &summary)?;
        self.w("\x1b[?25h")?;
        result
    }

    fn multi_select(
        &mut self,
        question: &str,
        options: &[&str],
        default_selected: &[bool],
    ) -> PromptResult<Vec<usize>> {
        let n = options.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        let _raw = RawMode::enable(self.in_fd)?;
        let mut chosen: Vec<bool> = (0..n)
            .map(|i| default_selected.get(i).copied().unwrap_or(false))
            .collect();
        let mut cur = 0usize;
        self.w("\x1b[?25l")?;
        self.w(&format!("{question}\r\n"))?;
        self.w("  (\u{2191}/\u{2193} or j/k move \u{00b7} space toggle \u{00b7} a/n all/none \u{00b7} enter ok \u{00b7} esc back)\r\n")?;
        let mut first = true;
        let result = loop {
            if !first {
                self.w(&format!("\x1b[{n}A"))?;
            }
            first = false;
            for (i, opt) in options.iter().enumerate() {
                let mark = if chosen[i] { "[x]" } else { "[ ]" };
                if i == cur {
                    self.w(&format!("\r\x1b[2K\x1b[7m> {mark} {opt} \x1b[0m\r\n"))?;
                } else {
                    self.w(&format!("\r\x1b[2K  {mark} {opt}\r\n"))?;
                }
            }
            match self.read_key()? {
                Key::Up | Key::Char(b'k') => cur = step(cur, n, -1),
                Key::Down | Key::Char(b'j') => cur = step(cur, n, 1),
                Key::Space => chosen[cur] = !chosen[cur],
                Key::Char(b'a') => chosen.fill(true),
                Key::Char(b'n') => chosen.fill(false),
                Key::Enter => break Ok((0..n).filter(|&i| chosen[i]).collect::<Vec<_>>()),
                Key::Esc => break Err(PromptError::Cancel),
                Key::Interrupt => break Err(PromptError::Abort),
                Key::Eof => {
                    self.eof = true;
                    break Ok((0..n).filter(|&i| chosen[i]).collect::<Vec<_>>());
                }
                _ => {}
            }
        };
        let summary = match &result {
            Ok(idx) => {
                let labels: Vec<&str> = idx.iter().map(|&i| options[i]).collect();
                let shown = if labels.is_empty() {
                    "none".to_string()
                } else {
                    labels.join(", ")
                };
                chosen_summary(question, &shown)
            }
            Err(_) => String::new(),
        };
        self.finish_block(n + 2, &summary)?;
        self.w("\x1b[?25h")?;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run<T>(
        input: &str,
        f: impl FnOnce(&mut LinePrompt<&[u8], Vec<u8>>) -> PromptResult<T>,
    ) -> (T, String) {
        let mut p = LinePrompt::new(input.as_bytes(), Vec::new());
        let out = f(&mut p).expect("prompt io");
        let written = String::from_utf8(p.writer).unwrap();
        (out, written)
    }

    #[test]
    fn text_uses_default_on_blank_and_value_otherwise() {
        let (a, shown) = run("\n", |p| p.text("Name", Some("default")));
        assert_eq!(a, "default");
        assert!(shown.contains("[default]"));
        let (b, _) = run("custom\n", |p| p.text("Name", Some("default")));
        assert_eq!(b, "custom");
        let (c, _) = run("  spaced  \n", |p| p.text("Name", None));
        assert_eq!(c, "spaced");
    }

    #[test]
    fn confirm_parses_variants_and_defaults() {
        assert!(run("y\n", |p| p.confirm("ok?", false)).0);
        assert!(run("YES\n", |p| p.confirm("ok?", false)).0);
        assert!(!run("n\n", |p| p.confirm("ok?", true)).0);
        assert!(run("\n", |p| p.confirm("ok?", true)).0);
        assert!(!run("\n", |p| p.confirm("ok?", false)).0);
        let (v, out) = run("maybe\nyes\n", |p| p.confirm("ok?", false));
        assert!(v);
        assert!(out.contains("answer y or n"));
    }

    #[test]
    fn select_by_number_label_default_and_invalid() {
        let opts = ["alpha", "beta", "gamma"];
        assert_eq!(run("2\n", |p| p.select("pick", &opts, 0)).0, 1);
        assert_eq!(run("gamma\n", |p| p.select("pick", &opts, 0)).0, 2);
        assert_eq!(run("\n", |p| p.select("pick", &opts, 2)).0, 2);
        let (v, out) = run("9\nbeta\n", |p| p.select("pick", &opts, 0));
        assert_eq!(v, 1);
        assert!(out.contains("enter a number"));
        let (_, out) = run("1\n", |p| p.select("pick", &opts, 1));
        assert!(out.contains("> 2) beta"), "{out}");
    }

    #[test]
    fn multi_select_numbers_all_none_default_dedup() {
        let opts = ["a", "b", "c", "d"];
        let def = [true, false, false, true];
        assert_eq!(
            run("1,3\n", |p| p.multi_select("e", &opts, &def)).0,
            vec![0, 2]
        );
        assert_eq!(
            run("3 1 1\n", |p| p.multi_select("e", &opts, &def)).0,
            vec![0, 2]
        );
        assert_eq!(
            run("all\n", |p| p.multi_select("e", &opts, &def)).0,
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            run("none\n", |p| p.multi_select("e", &opts, &def)).0,
            Vec::<usize>::new()
        );
        assert_eq!(
            run("\n", |p| p.multi_select("e", &opts, &def)).0,
            vec![0, 3]
        );
        let (v, out) = run("9\n2\n", |p| p.multi_select("e", &opts, &def));
        assert_eq!(v, vec![1]);
        assert!(out.contains("separated by commas"));
    }

    #[test]
    fn text_validated_reprompts_until_valid() {
        let (v, out) = run("bad\ngood\n", |p| {
            p.text_validated("x", None, |a| {
                if a == "good" {
                    Ok(())
                } else {
                    Err("nope".into())
                }
            })
        });
        assert_eq!(v, "good");
        assert!(out.contains("! nope"));
    }

    #[test]
    fn eof_returns_default_and_sets_flag() {
        let mut p = LinePrompt::new(&b""[..], Vec::new());
        assert_eq!(p.text("x", Some("d")).unwrap(), "d");
        assert!(p.at_eof());
        assert!(p.confirm("y?", true).unwrap());
        assert_eq!(p.select("s", &["a", "b"], 1).unwrap(), 1);
        assert_eq!(
            p.multi_select("m", &["a", "b"], &[true, false]).unwrap(),
            vec![0]
        );
    }

    #[test]
    fn decode_key_maps_arrows_vim_controls_and_esc() {
        assert_eq!(decode_key(b"\x1b[A"), (Key::Up, 3));
        assert_eq!(decode_key(b"\x1b[B"), (Key::Down, 3));
        assert_eq!(decode_key(b"\x1bOA"), (Key::Up, 3));
        assert_eq!(decode_key(b"\r"), (Key::Enter, 1));
        assert_eq!(decode_key(b" "), (Key::Space, 1));
        assert_eq!(decode_key(b"j"), (Key::Char(b'j'), 1));
        assert_eq!(decode_key(&[0x7f]), (Key::Backspace, 1));
        assert_eq!(decode_key(&[0x03]), (Key::Interrupt, 1));
        assert_eq!(decode_key(&[0x1b]), (Key::Esc, 1)); // lone ESC
        assert_eq!(decode_key(b""), (Key::Eof, 0));
    }

    #[test]
    fn decode_key_consumes_one_key_from_batched_bytes() {
        let buf = b"\x1b[B\x1b[B\n";
        let (k1, n1) = decode_key(buf);
        assert_eq!((k1, n1), (Key::Down, 3));
        let (k2, n2) = decode_key(&buf[n1..]);
        assert_eq!((k2, n2), (Key::Down, 3));
        let (k3, _) = decode_key(&buf[n1 + n2..]);
        assert_eq!(k3, Key::Enter);
    }

    #[test]
    fn step_wraps_both_directions() {
        assert_eq!(step(0, 3, -1), 2);
        assert_eq!(step(2, 3, 1), 0);
        assert_eq!(step(1, 3, 1), 2);
        assert_eq!(step(0, 0, 1), 0);
    }

    #[test]
    fn pop_utf8_removes_whole_chars() {
        let mut b = "aé世".as_bytes().to_vec();
        pop_utf8(&mut b);
        assert_eq!(b, "aé".as_bytes());
        pop_utf8(&mut b);
        assert_eq!(b, b"a");
        pop_utf8(&mut b);
        assert!(b.is_empty());
        pop_utf8(&mut b); // empty is a no-op
        assert!(b.is_empty());
    }

    // ----- raw-mode TermUi driven through a real pty -----

    #[cfg(unix)]
    fn open_pty() -> (RawFd, RawFd) {
        unsafe {
            let master = libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY);
            assert!(master >= 0, "posix_openpt");
            assert_eq!(libc::grantpt(master), 0, "grantpt");
            assert_eq!(libc::unlockpt(master), 0, "unlockpt");
            let name = libc::ptsname(master);
            assert!(!name.is_null(), "ptsname");
            let slave = libc::open(name, libc::O_RDWR | libc::O_NOCTTY);
            assert!(slave >= 0, "open slave");
            (master, slave)
        }
    }

    /// Put the slave in raw mode up front so the cooked line discipline never
    /// gets a window to line-buffer input, echo it, or (the subtle one) consume
    /// a Ctrl-C byte as VINTR before `TermUi` reads it.
    #[cfg(unix)]
    fn set_raw(fd: RawFd) {
        unsafe {
            let mut t: libc::termios = std::mem::zeroed();
            assert_eq!(libc::tcgetattr(fd, &mut t), 0);
            t.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG | libc::IEXTEN);
            t.c_iflag &= !(libc::IXON | libc::ICRNL);
            t.c_oflag &= !libc::OPOST;
            t.c_cc[libc::VMIN] = 1;
            t.c_cc[libc::VTIME] = 0;
            assert_eq!(libc::tcsetattr(fd, libc::TCSANOW, &t), 0);
        }
    }

    /// Run `f` against a `TermUi` wired to a pty, feeding `keys` as input.
    /// The master is drained concurrently so the UI's rendering writes can
    /// never block on a full pty buffer.
    #[cfg(unix)]
    fn drive_term<T: Send + 'static>(
        keys: &[u8],
        f: impl FnOnce(&mut TermUi) -> T + Send + 'static,
    ) -> T {
        let (master, slave) = open_pty();
        set_raw(slave);
        let drain = std::thread::spawn(move || {
            let mut buf = [0u8; 256];
            loop {
                let n =
                    unsafe { libc::read(master, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
                if n <= 0 {
                    break;
                }
            }
        });
        let handle = std::thread::spawn(move || {
            let mut ui = TermUi::with_fds(slave, slave);
            f(&mut ui)
        });
        let n = unsafe { libc::write(master, keys.as_ptr() as *const libc::c_void, keys.len()) };
        assert_eq!(n, keys.len() as isize, "feed keys");
        let res = handle.join().expect("ui thread");
        unsafe { libc::close(slave) };
        drain.join().expect("drain thread");
        unsafe { libc::close(master) };
        res
    }

    #[cfg(unix)]
    #[test]
    fn term_ui_select_navigates_with_arrows_over_a_pty() {
        let res = drive_term(b"\x1b[B\x1b[B\n", |ui| {
            ui.select("pick", &["a", "b", "c"], 0)
        });
        assert_eq!(res.expect("select"), 2);
    }

    #[cfg(unix)]
    #[test]
    fn term_ui_select_vim_keys_navigate_over_a_pty() {
        let res = drive_term(b"jjk\n", |ui| ui.select("pick", &["a", "b", "c"], 0));
        assert_eq!(res.expect("select"), 1);
    }

    #[cfg(unix)]
    #[test]
    fn term_ui_multi_select_toggles_with_space_over_a_pty() {
        let res = drive_term(b" \x1b[B \n", |ui| {
            ui.multi_select("e", &["a", "b", "c"], &[false, false, false])
        });
        assert_eq!(res.expect("multi_select"), vec![0, 1]);
    }

    #[cfg(unix)]
    #[test]
    fn term_ui_esc_cancels_and_ctrl_c_aborts_over_a_pty() {
        // Esc => Cancel (go back).
        let cancel = drive_term(&[0x1b], |ui| ui.select("pick", &["a", "b"], 0));
        assert!(matches!(cancel, Err(PromptError::Cancel)));
        // Ctrl-C => Abort (quit the wizard).
        let abort = drive_term(&[0x03], |ui| ui.select("pick", &["a", "b"], 0));
        assert!(matches!(abort, Err(PromptError::Abort)));
        // Esc cancels a text field too.
        let text_cancel = drive_term(b"hi\x1b", |ui| ui.text("name", None));
        assert!(matches!(text_cancel, Err(PromptError::Cancel)));
    }

    #[cfg(unix)]
    #[test]
    fn term_ui_text_edits_with_backspace_over_a_pty() {
        let res = drive_term(b"abc\x7f\x7fX\n", |ui| ui.text("name", Some("def")));
        assert_eq!(res.expect("text"), "aX");
        // Blank input falls back to the default.
        let res = drive_term(b"\n", |ui| ui.text("name", Some("def")));
        assert_eq!(res.expect("text"), "def");
    }

    /// Like `drive_term` but also returns everything the UI wrote (the pty
    /// master output), to assert on what is left on screen.
    #[cfg(unix)]
    fn drive_term_capture<T: Send + 'static>(
        keys: &[u8],
        f: impl FnOnce(&mut TermUi) -> T + Send + 'static,
    ) -> (T, Vec<u8>) {
        use std::sync::{Arc, Mutex};
        let (master, slave) = open_pty();
        set_raw(slave);
        let cap = Arc::new(Mutex::new(Vec::new()));
        let capc = Arc::clone(&cap);
        let drain = std::thread::spawn(move || {
            let mut buf = [0u8; 256];
            loop {
                let n =
                    unsafe { libc::read(master, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
                if n <= 0 {
                    break;
                }
                capc.lock().unwrap().extend_from_slice(&buf[..n as usize]);
            }
        });
        let handle = std::thread::spawn(move || {
            let mut ui = TermUi::with_fds(slave, slave);
            f(&mut ui)
        });
        let n = unsafe { libc::write(master, keys.as_ptr() as *const libc::c_void, keys.len()) };
        assert_eq!(n, keys.len() as isize, "feed keys");
        let res = handle.join().expect("ui thread");
        unsafe { libc::close(slave) };
        drain.join().expect("drain thread");
        unsafe { libc::close(master) };
        let bytes = Arc::try_unwrap(cap).unwrap().into_inner().unwrap();
        (res, bytes)
    }

    #[cfg(unix)]
    #[test]
    fn term_ui_select_collapses_menu_to_one_line() {
        let (res, out) = drive_term_capture(b"\x1b[B\x1b[B\n", |ui| {
            ui.select("Pick one", &["alpha", "beta", "gamma"], 0)
        });
        assert_eq!(res.expect("select"), 2);
        let s = String::from_utf8_lossy(&out);
        // The block is erased (clear-to-end-of-display) and replaced by a single
        // bold summary; the per-option lines no longer linger after it.
        assert!(s.contains("\x1b[J"), "menu block must be erased:\n{s:?}");
        assert!(
            s.contains("Pick one \x1b[1mgamma\x1b[0m"),
            "collapsed summary must remain:\n{s:?}"
        );
        let tail = &s[s.rfind("\x1b[J").unwrap()..];
        assert!(
            !tail.contains("alpha") && !tail.contains("beta"),
            "unchosen options must not survive the collapse:\n{tail:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn term_ui_cancel_erases_the_menu() {
        let (res, out) =
            drive_term_capture(&[0x1b], |ui| ui.select("Pick one", &["alpha", "beta"], 0));
        assert!(matches!(res, Err(PromptError::Cancel)));
        let s = String::from_utf8_lossy(&out);
        // After erasing, nothing of the menu is left past the final clear.
        let tail = &s[s.rfind("\x1b[J").unwrap()..];
        assert!(
            !tail.contains("alpha") && !tail.contains("Pick one"),
            "cancelled menu must be wiped:\n{tail:?}"
        );
    }
}
