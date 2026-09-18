//! Fallible worker creation (SPEC §3, §6): resource exhaustion must degrade observation,
//! never panic or replace the child process's result.

use std::io;
use std::thread::{self, JoinHandle};

pub(crate) fn spawn<F, T>(name: &'static str, work: F) -> io::Result<JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    #[cfg(test)]
    if FAIL_NAME.with(|fail| fail.get() == Some(name)) {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    }
    thread::Builder::new().name(name.into()).spawn(work)
}

#[cfg(test)]
thread_local! {
    pub(crate) static FAIL_NAME: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}
