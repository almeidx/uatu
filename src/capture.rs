//! Output capture (SPEC §6): concurrent with raw passthrough, on its own
//! thread per stream, fed chunks over an unbounded channel so passthrough
//! never waits on line assembly, redaction, or disk I/O.
//!
//! `capped` mode keeps the first `head_bytes` on disk as the run progresses
//! plus the last `tail_bytes` in an in-memory ring written at run end, with a
//! truncation marker recording the omitted byte count. Capture write errors
//! degrade that stream only (SPEC §6 mid-run I/O failure).

use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::config::CaptureMode;
use crate::db::CaptureMeta;
use crate::redact::{LineAssembler, Redactor, LINE_BUF_CAP};

pub struct CaptureTask {
    pub handle: JoinHandle<CaptureMeta>,
}

pub struct CaptureSpec {
    pub mode: CaptureMode,
    pub head_bytes: u64,
    pub tail_bytes: u64,
    pub path: PathBuf,
}

struct Ring {
    buf: VecDeque<u8>,
    cap: usize,
    total_pushed: u64,
}

impl Ring {
    fn new(cap: usize) -> Ring {
        Ring {
            buf: VecDeque::new(),
            cap,
            total_pushed: 0,
        }
    }

    fn push(&mut self, data: &[u8]) {
        self.total_pushed += data.len() as u64;
        if self.cap == 0 {
            return;
        }
        if data.len() >= self.cap {
            self.buf.clear();
            self.buf.extend(&data[data.len() - self.cap..]);
            return;
        }
        let overflow = (self.buf.len() + data.len()).saturating_sub(self.cap);
        if overflow > 0 {
            self.buf.drain(..overflow);
        }
        self.buf.extend(data);
    }

    fn omitted(&self) -> u64 {
        self.total_pushed - self.buf.len() as u64
    }
}

struct Sink {
    mode: CaptureMode,
    file: Option<File>,
    path: PathBuf,
    head_remaining: u64,
    ring: Ring,
    stored: u64,
    omitted: u64,
    reason: Option<String>,
}

impl Sink {
    fn degrade(&mut self, why: String) {
        if self.reason.is_none() {
            self.reason = Some(why);
        }
        self.file = None;
    }

    fn write_file(&mut self, data: &[u8]) -> bool {
        if let Some(f) = self.file.as_mut() {
            match f.write_all(data) {
                Ok(()) => {
                    self.stored += data.len() as u64;
                    return true;
                }
                Err(e) => self.degrade(format!("capture write error: {e}")),
            }
        }
        false
    }

    /// One redacted line (or fragment) arrives here.
    fn accept(&mut self, data: &[u8]) {
        match self.mode {
            CaptureMode::Off => {}
            CaptureMode::Full => {
                if self.file.is_some() {
                    self.write_file(data);
                } else {
                    self.omitted += data.len() as u64;
                }
            }
            CaptureMode::Capped => {
                if self.reason.is_some() {
                    self.omitted += data.len() as u64;
                    return;
                }
                let mut data = data;
                if self.head_remaining > 0 {
                    let take = (self.head_remaining as usize).min(data.len());
                    if self.write_file(&data[..take]) {
                        self.head_remaining -= take as u64;
                        data = &data[take..];
                    } else {
                        self.omitted += data.len() as u64;
                        return;
                    }
                }
                if !data.is_empty() {
                    self.ring.push(data);
                }
            }
        }
    }

    fn finalize(mut self) -> CaptureMeta {
        if self.mode == CaptureMode::Capped {
            if self.reason.is_none() && self.ring.total_pushed > 0 {
                let omitted = self.ring.omitted();
                if omitted > 0 {
                    let marker =
                        format!("\n--- uatu: {omitted} bytes omitted (capped capture) ---\n");
                    self.write_file(marker.as_bytes());
                    // marker bytes are bookkeeping, not stored content
                    self.stored = self.stored.saturating_sub(marker.len() as u64);
                }
                let (a, b) = self.ring.buf.as_slices();
                let (a, b) = (a.to_vec(), b.to_vec());
                self.write_file(&a);
                self.write_file(&b);
                self.omitted += omitted;
            } else if self.reason.is_some() {
                // Degraded: anything still in the ring is lost too.
                self.omitted += self.ring.total_pushed;
            }
        }
        if let Some(f) = self.file.take() {
            let _ = f.sync_data();
        }
        let have_file = self.mode != CaptureMode::Off
            && (self.stored > 0 || self.reason.is_none() || self.path.exists());
        CaptureMeta {
            path: if have_file {
                Some(self.path.to_string_lossy().into_owned())
            } else {
                None
            },
            bytes_total: 0, // filled by the caller from the pump's raw counter
            bytes_stored: self.stored,
            bytes_omitted: self.omitted,
            reason: self.reason,
        }
    }
}

/// Spawn the capture thread for one stream. `rx` receives raw chunks from the
/// pump; `raw_total` is the pump's raw byte counter, merged into the metadata
/// at the end.
pub fn spawn_capture(
    spec: CaptureSpec,
    redactor: Arc<Redactor>,
    rx: Receiver<Vec<u8>>,
    raw_total: Arc<AtomicU64>,
) -> CaptureTask {
    let handle = std::thread::spawn(move || {
        let file = if spec.mode == CaptureMode::Off {
            None
        } else {
            match std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .mode(0o600)
                .open(&spec.path)
            {
                Ok(f) => Some(f),
                Err(_) if spec.mode == CaptureMode::Off => None,
                Err(e) => {
                    // Degraded from the start; keep consuming to count bytes.
                    let mut sink = Sink {
                        mode: spec.mode,
                        file: None,
                        path: spec.path.clone(),
                        head_remaining: spec.head_bytes,
                        ring: Ring::new(0),
                        stored: 0,
                        omitted: 0,
                        reason: Some(format!("cannot open capture file: {e}")),
                    };
                    drain(&mut sink, &redactor, rx);
                    let mut meta = sink.finalize();
                    meta.bytes_total = raw_total.load(Ordering::SeqCst);
                    return meta;
                }
            }
        };
        let mut sink = Sink {
            mode: spec.mode,
            file,
            path: spec.path.clone(),
            head_remaining: spec.head_bytes,
            ring: Ring::new(spec.tail_bytes as usize),
            stored: 0,
            omitted: 0,
            reason: None,
        };
        drain(&mut sink, &redactor, rx);
        let sink = std::mem::replace(
            &mut sink,
            Sink {
                mode: CaptureMode::Off,
                file: None,
                path: PathBuf::new(),
                head_remaining: 0,
                ring: Ring::new(0),
                stored: 0,
                omitted: 0,
                reason: None,
            },
        );
        let mut meta = sink.finalize();
        meta.bytes_total = raw_total.load(Ordering::SeqCst);
        meta
    });
    CaptureTask { handle }
}

fn drain(sink: &mut Sink, redactor: &Redactor, rx: Receiver<Vec<u8>>) {
    let mut asm = LineAssembler::new(LINE_BUF_CAP);
    let mut emit = |line: &[u8], complete: bool| {
        let mut red = redactor.redact_bytes(line).into_owned();
        if complete {
            red.push(b'\n');
        }
        sink.accept(&red);
    };
    for chunk in rx {
        asm.push(&chunk, &mut emit);
    }
    asm.finish(&mut emit);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn run_capture(
        mode: CaptureMode,
        head: u64,
        tail: u64,
        chunks: Vec<Vec<u8>>,
        redactor: Redactor,
    ) -> (CaptureMeta, PathBuf, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stdout.log");
        let (tx, rx) = mpsc::channel();
        let raw = Arc::new(AtomicU64::new(0));
        for c in &chunks {
            raw.fetch_add(c.len() as u64, Ordering::SeqCst);
        }
        let task = spawn_capture(
            CaptureSpec {
                mode,
                head_bytes: head,
                tail_bytes: tail,
                path: path.clone(),
            },
            Arc::new(redactor),
            rx,
            raw,
        );
        for c in chunks {
            tx.send(c).unwrap();
        }
        drop(tx);
        let meta = task.handle.join().unwrap();
        (meta, path, dir)
    }

    #[test]
    fn capped_head_marker_tail_accounting() {
        // 10-byte head, 10-byte tail, 35 bytes of lines -> omitted 15.
        let lines: Vec<Vec<u8>> = (0..7).map(|i| format!("li{i}e\n").into_bytes()).collect();
        let (meta, path, _d) = run_capture(CaptureMode::Capped, 10, 10, lines, Redactor::empty());
        let content = std::fs::read(&path).unwrap();
        let text = String::from_utf8_lossy(&content);
        assert!(text.starts_with("li0e\nli1e\n"), "head kept: {text}");
        assert!(
            text.contains("--- uatu: 15 bytes omitted (capped capture) ---"),
            "{text}"
        );
        assert!(text.ends_with("li5e\nli6e\n"), "tail kept: {text}");
        assert_eq!(meta.bytes_total, 35);
        assert_eq!(meta.bytes_stored, 20);
        assert_eq!(meta.bytes_omitted, 15);
        assert!(meta.reason.is_none());
    }

    #[test]
    fn capped_small_output_stored_whole() {
        let (meta, path, _d) = run_capture(
            CaptureMode::Capped,
            1024,
            1024,
            vec![b"hello\nworld\n".to_vec()],
            Redactor::empty(),
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"hello\nworld\n");
        assert_eq!(meta.bytes_stored, 12);
        assert_eq!(meta.bytes_omitted, 0);
    }

    #[test]
    fn redaction_applied_before_disk() {
        let red = Redactor::new(&["token123".into()], &[], &[]).unwrap();
        let (_meta, path, _d) = run_capture(
            CaptureMode::Full,
            0,
            0,
            vec![b"the token123 leaked\n".to_vec()],
            red,
        );
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "the [REDACTED] leaked\n");
    }

    #[test]
    fn full_mode_unbounded() {
        let chunks: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("line {i}\n").into_bytes())
            .collect();
        let (meta, path, _d) = run_capture(CaptureMode::Full, 0, 0, chunks, Redactor::empty());
        assert_eq!(meta.bytes_stored, std::fs::metadata(&path).unwrap().len());
        assert_eq!(meta.bytes_omitted, 0);
    }

    #[test]
    fn open_failure_degrades_with_reason() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing-dir").join("stdout.log");
        let (tx, rx) = mpsc::channel();
        let raw = Arc::new(AtomicU64::new(6));
        let task = spawn_capture(
            CaptureSpec {
                mode: CaptureMode::Capped,
                head_bytes: 10,
                tail_bytes: 10,
                path,
            },
            Arc::new(Redactor::empty()),
            rx,
            raw,
        );
        tx.send(b"hello\n".to_vec()).unwrap();
        drop(tx);
        let meta = task.handle.join().unwrap();
        assert!(meta.reason.as_deref().unwrap().contains("cannot open"));
        assert_eq!(meta.bytes_stored, 0);
        assert_eq!(meta.bytes_total, 6);
    }
}
