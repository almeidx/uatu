# Changelog

All notable changes to this project will be documented in this file.


## [0.1.1-alpha.2] - 2026-06-15

- Switch release workflow to gh release create

## [0.1.1-alpha.1] - 2026-06-15

- Split CI lint/test and move release publishing
- docs: add AGENTS.md (CLAUDE.md symlink) for coding agents
- init: create config 0600 and config dir 0700
- report: redact delivery errors before storing last_error
- report: parse Retry-After totally; saturate duration→ms casts
- Fix CI: clippy lint, and a fresh-db WAL-conversion race losing runs
- first commit

