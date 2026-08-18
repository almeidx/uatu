# Changelog

All notable changes to this project will be documented in this file.


## [0.1.1-alpha.7] - 2026-08-18

- Refactor digest notifications and configuration workflows (#43)
- deps: Lock file maintenance (#42)
- deps: Update Rust crate rusqlite to 0.40.2 (#41)
- deps: Lock file maintenance (#40)
- deps: Update patch/minor dependencies (#39)
- deps: Lock file maintenance (#38)
- deps: Update patch/minor dependencies (#37)
- chore: schedule Renovate updates for Saturday mornings


## [0.1.1-alpha.6] - 2026-07-27

- deps: Update Rust crate jiff to 0.2.35 (#35)
- deps: Lock file maintenance (#34)
- deps: Update patch/minor dependencies (#33)
- deps: Update Rust crate serde_json to 1.0.151 (#32)
- deps: Update Rust crate jiff to 0.2.34 (#31)
- deps: Update patch/minor dependencies (#30)
- deps: Update Rust crate anyhow to 1.0.104 (#29)
- deps: Lock file maintenance (#28)
- deps: Update patch/minor dependencies (#27)
- deps: Update Rust crate ulid to v3 (#26)
- deps: Update Rust crate toml to 1.1.3 (#25)
- deps: Update Rust crate ulid to v2 (#24)
- deps: Lock file maintenance (#23)
- deps: Update Rust crate regex to 1.13.0 (#22)
- deps: Update Rust crate jiff to 0.2.32 (#21)
- deps: Lock file maintenance (#20)
- deps: Update Rust crate rand to 0.10.2 (#19)
- deps: Update Rust crate jiff to 0.2.31 (#18)
- deps: Lock file maintenance (#17)
- deps: Update Rust crate anyhow to 1.0.103 (#16)


## [0.1.1-alpha.5] - 2026-06-23

- feat(config): cover digest options in the interactive wizard (#14)
- deps: Update patch/minor dependencies to 0.2.29 (#13)
- deps: Lock file maintenance (#12)
- docs: prefer homebrew install


## [0.1.1-alpha.4] - 2026-06-18

- ci: build macos release artifacts
- deps: Update actions/checkout action to v7 (#10)


## [0.1.1-alpha.3] - 2026-06-18

- chore: require rust 1.96
- deps: Lock file maintenance (#8)
- chore: align cli infra
- feat(config): interactive configuration wizard (#7)
- feat: add digest notification cadence controls (#6)
- deps: Update patch/minor dependencies (#5)
- deps: Update Rust crate toml to v1 (#3)
- chore(license): remove appendix


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

