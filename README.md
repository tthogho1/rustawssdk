# rustawssdk

Small CLI utilities using AWS SDK for Rust — S3 + DynamoDB helpers.

## Build

From the project root:

```bash
cargo build
# or release build
cargo build --release
```

## Executable

- Debug binary: `target/debug/rustawssdk`
- Release binary: `target/release/rustawssdk`

You can also run via `cargo run -- <command> ...`.

## Commands

Usage:

```text
rustawssdk <command> [...]
```

Supported commands (selected):

- `list-buckets` — list all S3 buckets
- `list-s3 <bucket>` — list objects in an S3 bucket
- `describe-table <table>` — print DynamoDB table schema
- `list-tables` — list DynamoDB tables
- `scan-table <table>` — print all items in a DynamoDB table
- `scan-table-csv <table>` — print table items as CSV
- `scan-table-tsv <table>` — print table items as TSV
- `item-exists <table> <key1=value1> [key2=value2 ...]` — check whether an item exists
- `set-attr <table> <attribute> <value> <key1=value1> ...` — set a single attribute on an item

Examples:

```bash
# describe a table
cargo run -- describe-table YoutubeList

# check item exists (string key)
cargo run -- item-exists YoutubeList video_id=abcd1234

# set numeric attribute 'transcribed' to 1
cargo run -- set-attr YoutubeList transcribed 1 video_id=abcd1234

# list S3 buckets
cargo run -- list-buckets
```

Notes:

- The CLI parses keys and values as strings by default; `set-attr` now infers booleans and numbers for the attribute `value` argument (e.g. `1` -> number).
- AWS credentials and region are provided via the usual environment variables or `~/.aws/` config (e.g. `AWS_PROFILE`, `AWS_REGION`).

## Helpers

- `check_prefixes.sh` — simple script that reads `prefixes.txt` and runs `item-exists` for each `video_id` (skip blank lines and lines starting with `#`).

## Development

- Requires Rust (rustup + cargo)
- `cargo build` then run commands with `cargo run -- <command>`

## License

MIT-style (add your license if desired).
