# HelixDB Storage Workspace

HelixDB is a Rust workspace for storage-engine work. The repository is intentionally small right now and serves as the root for the storage crate under `crates/helixdb-storage`.

## What is here

- a Rust workspace root in `Cargo.toml`
- the `crates/helixdb-storage` workspace member
- room to grow into more crates as the project matures

## Repository Layout

```text
.
├── Cargo.toml
└── crates/
    └── helixdb-storage/
```

## Development

From the workspace root:

```powershell
cargo fmt
cargo test
```

If you are extending the workspace, keep new code small, testable, and isolated by responsibility. That makes it easier to evolve the storage layer without turning the repo into a single large crate.

## Notes

This README stays deliberately factual: the repo is currently a workspace foundation, not a finished database product. As the storage crate grows, add details for on-disk layout, recovery, replication, and test coverage here.
