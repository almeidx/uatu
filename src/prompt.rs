//! Interactive prompts for the configuration wizard (`config wizard` /
//! `init --interactive`).
//!
//! Two backends behind one [`Ui`] trait:
//!
//! - [`TermUi`] — a real terminal UI backed by the `inquire` crate: a
//!   highlighted cursor moved with the arrow keys or `j`/`k`, type-to-filter,
//!   Enter to choose, Space to toggle multi-selects, masked password entry,
//!   **Esc to go back, Ctrl-C to quit**. Used when stdin and stdout are a TTY.
//! - [`LinePrompt`] — a line-at-a-time reader (type the option number / `y`,`n`,
//!   press Enter) for pipes, redirected input, CI, and every test, so the wizard
//!   stays fully scriptable. Generic over `BufRead`/`Write`; it never cancels or
//!   aborts.
//!
//! Input methods return [`PromptResult`]: `Err(PromptError::Cancel)` for Esc
//! (the wizard returns to its menu) and `Err(PromptError::Abort)` for Ctrl-C
//! (the wizard exits without saving). [`cmd_configure`](crate::commands::configure)
//! picks the backend with [`stdio_is_tty`]. Blank input selects the default.

use std::io::{self, BufRead, IsTerminal, Write};

use inquire::{Confirm, InquireError, MultiSelect, Password, PasswordDisplayMode, Select, Text};

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

    /// A secret value (e.g. an SMTP password). The default delegates to plain
    /// text; the terminal backend masks the input so it is not echoed.
    fn secret(&mut self, question: &str) -> PromptResult<String> {
        self.text(question, None)
    }

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

/// True only when stdin, stdout, and stderr are all terminals — the condition
/// for the `inquire`-backed [`TermUi`]. inquire renders prompts to **stderr**,
/// so a redirected stderr (`wizard 2>file`) would otherwise produce an invisible
/// prompt; any redirected stream falls back to the scriptable [`LinePrompt`].
pub fn stdio_is_tty() -> bool {
    io::stdin().is_terminal() && io::stdout().is_terminal() && io::stderr().is_terminal()
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
// Terminal backend (the `inquire` crate)
// ---------------------------------------------------------------------------

/// Map inquire's error to the wizard's cancel/abort signal. Esc cancels the
/// current prompt; Ctrl-C interrupts; everything else is an I/O failure.
fn map_err(e: InquireError) -> PromptError {
    match e {
        InquireError::OperationCanceled => PromptError::Cancel,
        InquireError::OperationInterrupted => PromptError::Abort,
        InquireError::IO(io) => PromptError::Io(io),
        other => PromptError::Io(io::Error::other(other.to_string())),
    }
}

#[derive(Default)]
pub struct TermUi;

impl TermUi {
    pub fn new() -> TermUi {
        TermUi
    }
}

impl Ui for TermUi {
    fn say(&mut self, line: &str) -> io::Result<()> {
        let mut out = io::stdout().lock();
        writeln!(out, "{line}")?;
        out.flush()
    }

    fn at_eof(&self) -> bool {
        false
    }

    fn text(&mut self, question: &str, default: Option<&str>) -> PromptResult<String> {
        let mut t = Text::new(question);
        if let Some(d) = default {
            if !d.is_empty() {
                t = t.with_default(d);
            }
        }
        t.prompt().map_err(map_err)
    }

    fn secret(&mut self, question: &str) -> PromptResult<String> {
        Password::new(question)
            .without_confirmation()
            .with_display_mode(PasswordDisplayMode::Masked)
            .with_help_message("input hidden \u{00b7} enter submits \u{00b7} esc back")
            .prompt()
            .map_err(map_err)
    }

    fn confirm(&mut self, question: &str, default: bool) -> PromptResult<bool> {
        Confirm::new(question)
            .with_default(default)
            .prompt()
            .map_err(map_err)
    }

    fn select(&mut self, question: &str, options: &[&str], default: usize) -> PromptResult<usize> {
        if options.is_empty() {
            return Ok(0);
        }
        let opts: Vec<String> = options.iter().map(|s| s.to_string()).collect();
        let start = default.min(opts.len() - 1);
        Select::new(question, opts)
            .with_starting_cursor(start)
            .with_vim_mode(true)
            .with_help_message("\u{2191}\u{2193}/jk move \u{00b7} enter select \u{00b7} esc back")
            .raw_prompt()
            .map(|o| o.index)
            .map_err(map_err)
    }

    fn multi_select(
        &mut self,
        question: &str,
        options: &[&str],
        default_selected: &[bool],
    ) -> PromptResult<Vec<usize>> {
        if options.is_empty() {
            // inquire errors on an empty list; match select / LinePrompt instead.
            return Ok(Vec::new());
        }
        let opts: Vec<String> = options.iter().map(|s| s.to_string()).collect();
        let defaults: Vec<usize> = default_indices(default_selected, opts.len());
        MultiSelect::new(question, opts)
            .with_default(&defaults)
            .with_vim_mode(true)
            .with_help_message(
                "\u{2191}\u{2193}/jk move \u{00b7} space toggle \u{00b7} enter confirm \u{00b7} esc back",
            )
            .raw_prompt()
            .map(|sel| sel.into_iter().map(|o| o.index).collect())
            .map_err(map_err)
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
    fn secret_default_delegates_to_text() {
        let (v, _) = run("hunter2\n", |p| p.secret("Password"));
        assert_eq!(v, "hunter2");
    }

    #[test]
    fn map_err_translates_cancel_abort_and_io() {
        // The wizard's whole cancel-to-go-back / Ctrl-C-quit contract rides on
        // this mapping; the deleted pty tests were the only prior coverage.
        assert!(matches!(
            map_err(InquireError::OperationCanceled),
            PromptError::Cancel
        ));
        assert!(matches!(
            map_err(InquireError::OperationInterrupted),
            PromptError::Abort
        ));
        assert!(matches!(
            map_err(InquireError::IO(io::Error::other("boom"))),
            PromptError::Io(_)
        ));
        // Any other inquire error degrades to a plain I/O error, not a cancel.
        assert!(matches!(map_err(InquireError::NotTTY), PromptError::Io(_)));
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
}
