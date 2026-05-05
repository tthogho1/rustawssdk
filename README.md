# rustawssdk

Small CLI utilities using the AWS Rust SDK for quick inspections and operations.

New commands added:

- `list-ecs-clusters` — list ECS cluster ARNs in the current account/region.
- `list-ecs-services <cluster>` — list service ARNs for the given ECS cluster (cluster name or ARN).
- `list-ecr-repos` — list ECR repositories (name and URI).

- `create-bucket <bucket> [region]` — create an S3 bucket; optional `region` sets a LocationConstraint for the bucket.

New IAM user removal commands:

- `delete-user <user>` — best-effort remove resources attached to the specified IAM user, then delete the user.
- `delete-users <user1> [user2 ...]` — delete multiple users sequentially; prints a summary of successes/failures.

Build & run

Install Rust toolchain and run:

```bash
cargo build
cargo run -- <command> [args]
# examples
cargo run -- list-ecs-clusters
cargo run -- list-ecs-services my-cluster
# rustawssdk

Small CLI utilities built on the AWS SDK for Rust. This repository provides a compact command-line tool and helper scripts to inspect and operate on common AWS resources (S3, DynamoDB, ECS, ECR, etc.).

## Overview

- Single binary `rustawssdk` exposing multiple subcommands for quick, script-friendly operations.
- Intended for tooling, ad-hoc inspection, and automation helpers (not a full-featured production product).

## Prerequisites

- Rust toolchain (rustup + cargo)
- AWS credentials accessible via the standard SDK configuration chain (environment variables, `~/.aws/credentials`, or instance role).

## Build

From the project root:

```bash
cargo build
# for an optimized binary
cargo build --release
```

The produced binaries are at `target/debug/rustawssdk` and `target/release/rustawssdk`.

You can also run commands directly with `cargo run -- <command> [args]`.

## Usage

General form:

```text
rustawssdk <command> [args]
```

Selected commands (see the source for the full list):

- `list-buckets` — list all S3 buckets
- `list-s3 <bucket>` — list objects in an S3 bucket
- `list-tables` — list DynamoDB tables
- `describe-table <table>` — print DynamoDB table schema
- `scan-table <table>` — print all items in a DynamoDB table
- `scan-table-csv <table>` / `scan-table-tsv <table>` — CSV/TSV exports of table items
- `item-exists <table> <key1=value1> [key2=value2 ...]` — check whether an item exists
- `set-attr <table> <attribute> <value> <key1=value1> ...` — set one attribute on an item (value infers numbers/booleans)
- `list-ecs-clusters` — list ECS cluster ARNs
- `list-ecs-services <cluster>` — list ECS services for a cluster
- `list-ecr-repos` — list ECR repositories (name and URI)

### Examples

```bash
# describe a DynamoDB table
cargo run -- describe-table YoutubeList

# check whether an item exists
cargo run -- item-exists YoutubeList video_id=abcd1234

# set numeric attribute 'transcribed' to 1
cargo run -- set-attr YoutubeList transcribed 1 video_id=abcd1234

# list S3 buckets
cargo run -- list-buckets

# delete a single IAM user (destructive — verify credentials and target carefully)
cargo run -- delete-user alice

# delete multiple IAM users
cargo run -- delete-users userA userB
```

## AWS credentials & region

The CLI uses the AWS SDK's default provider chain. Ensure your environment has credentials and region set, for example:

```bash
export AWS_PROFILE=default
export AWS_REGION=us-east-1
```

Important: `delete-user` performs destructive operations. It attempts to remove common resources that block `DeleteUser` (login profile, access keys, signing certificates, SSH public keys, service-specific credentials, MFA devices, inline & managed policies, and group memberships). The command is best-effort and will log errors for individual cleanup steps; verify results in the AWS console or via `aws iam` before running in production.

## Helper scripts

- `check_prefixes.sh` — iterates `prefixes.txt` and uses `item-exists` to check `video_id` entries.
- `check_mp4s.sh`, `update_transcribed.sh`, and other scripts are included for common workflows — read each script header for usage.

## Development

- Use `cargo build` / `cargo run` as shown above.
- See `Cargo.toml` for crate dependencies (aws-config, aws-service crates, etc.).

## Contributing

Contributions are welcome. Open issues or pull requests for feature requests, bug fixes, or documentation improvements.

## License

MIT-style (no license file included in the repository). Add a `LICENSE` file if you need an explicit grant.
