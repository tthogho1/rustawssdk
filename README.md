# rustawssdk

Small CLI utilities built on the AWS SDK for Rust. This repository provides a compact command-line tool and helper scripts to inspect and operate on common AWS resources (S3, DynamoDB, ECS, ECR, IAM, Organizations, Identity Center, etc.).

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

### S3

- `list-buckets` — list all S3 buckets
- `list-s3 <bucket>` — list objects in an S3 bucket
- `ls-s3 <bucket> [prefix]` — list one "folder" level (sub-folders and files) like `aws s3 ls`; shows the first 1000 entries
- `count-s3 <bucket> [prefix]` — count objects under a bucket/prefix and report their total size
- `info-s3 <bucket> <key>` — show object metadata (size, content type, last-modified, ETag, storage class, user metadata)
- `create-bucket <bucket> [region]` — create an S3 bucket

### DynamoDB

- `list-tables` — list DynamoDB tables
- `describe-table <table>` — print DynamoDB table schema
- `scan-table <table>` — print all items in a DynamoDB table (paginated)
- `scan-table-csv <table>` / `scan-table-tsv <table>` — CSV/TSV exports of table items
- `item-exists <table> <key1=value1> [key2=value2 ...]` — check whether an item exists
- `set-attr <table> <attribute> <value> <key1=value1> ...` — set one attribute on an item
- `delete-all <table>` — delete all items in a table

### ECS / ECR

- `list-ecs-clusters` — list ECS cluster ARNs
- `list-ecs-services <cluster>` — list ECS services for a cluster
- `list-ecr-repos` — list ECR repositories (name and URI)

### EC2 (VPC / Subnet / Security Group)

- `list-vpcs` — list VPCs
- `create-vpc <cidr-block>` — create a VPC
- `list-subnets` — list subnets
- `create-subnet <vpc-id> <cidr-block> [availability-zone]` — create a subnet
- `list-security-groups` — list security groups
- `show-security-group <group-id>` — show details of a security group
- `create-security-group <name> <description> [vpc-id]` — create a security group
- `delete-security-group <group-id>` — delete a security group

### AWS Batch

- `list-compute-envs` — list Batch compute environments
- `list-job-queues` — list Batch job queues
- `list-job-defs` — list Batch job definitions
- `create-compute-env <name> <max-vcpus> <subnets,comma-sep> <sgs,comma-sep> [service-role-arn]`
- `create-job-queue <name> <compute-env-arn-or-name> <priority>`
- `register-job-def <name> <ecr-image-uri> <vcpus> <memory-mib> <exec-role-arn> <log-group> <log-region> <log-prefix>`
- `submit-job <job-name> <job-queue> <job-definition>`
- `describe-job <job-id>`
- `wait-compute-env <name> [timeout-secs] [poll-interval-secs]`
- `delete-compute-env <name>`

### IAM

- `get-role <role-name>` — show IAM role details
- `list-role-policies <role-name>` — list attached policies for a role
- `delete-policy <policy-arn>` — detach, remove non-default versions, then delete a managed IAM policy
- `delete-user <user>` — remove all attached resources then delete an IAM user
- `delete-users <user1> [user2 ...]` — delete multiple IAM users sequentially

### Organizations

- `list-ous` — list all OUs in the organization
- `delete-ou <ou-id>` — recursively delete an OU and its contents
- `delete-ou-dry-run <ou-id>` — dry-run of `delete-ou`

### Identity Center (SSO)

- `list-permission-sets <instance-arn>` — list all Permission Set ARNs for an Identity Center instance
- `list-identity-store-users <identity-store-id>` — list all users (ID, UserName, DisplayName) in an Identity Store

## Examples

```bash
# describe a DynamoDB table
cargo run -- describe-table YoutubeList

# check whether an item exists
cargo run -- item-exists YoutubeList video_id=abcd1234

# set numeric attribute 'transcribed' to 1
cargo run -- set-attr YoutubeList transcribed 1 video_id=abcd1234

# list S3 buckets
cargo run -- list-buckets

# count objects under a prefix and see how much they take up
cargo run -- count-s3 audio4input NQsldKLWj1M/
# -> Total: 3 object(s), 51380224 bytes (49.00 MiB)

# inspect a single object
cargo run -- info-s3 audio4input NQsldKLWj1M/NQsldKLWj1M.mp4

# delete a single IAM user (destructive)
cargo run -- delete-user alice

# delete multiple IAM users
cargo run -- delete-users userA userB

# delete a managed IAM policy (destructive)
cargo run -- delete-policy arn:aws:iam::123456789012:policy/MyPolicy

# list all Permission Sets for an Identity Center instance
cargo run -- list-permission-sets arn:aws:sso:::instance/ssoins-xxxxxxxxxx

# list all users in an Identity Store
cargo run -- list-identity-store-users d-xxxxxxxxxx
```

## S3 Explorer (desktop GUI)

`s3-explorer/` is a [Tauri v2](https://tauri.app) desktop app for browsing S3 like a file
explorer: bucket list → folders → files, with back/forward/up navigation, a breadcrumb
path bar, sortable columns, a name filter, and "Load more" paging (1000 entries per page).
It is read-only for now.

The Rust side is a thin wrapper around the `rustawssdk::explorer` library
(`src/explorer.rs`), which the `ls-s3` CLI command also uses. The UI is plain
HTML/CSS/JS in `s3-explorer/ui/`, so no npm build step is required.

```bash
# run the GUI (the repo root is a Cargo workspace; plain `cargo build` builds only the CLI)
AWS_PROFILE=<profile> cargo run -p s3-explorer

# package a .app/.dmg (macOS) or .msi/.exe (Windows); needs `cargo install tauri-cli --version "^2"`
cd s3-explorer && cargo tauri build
```

- Credentials come from the default provider chain, like the CLI. A GUI launched from
  Finder/Explorer does not see your shell's `AWS_REGION`, so the app falls back to
  `us-east-1` for ListBuckets; each bucket is then accessed through a client for its own region.
- If your credentials lack `s3:ListAllMyBuckets`, type `s3://bucket/path` into the
  "Open" box in the sidebar to open a bucket or folder directly.

## AWS credentials & region

The CLI uses the AWS SDK's default provider chain. Ensure your environment has credentials and region set, for example:

```bash
export AWS_PROFILE=default
export AWS_REGION=us-east-1
```

> **Warning:** `delete-user`, `delete-policy`, `delete-ou` perform destructive operations. Verify credentials and targets carefully before running in production.

## Helper scripts

- `check_prefixes.sh` — iterates `prefixes.txt` and uses `item-exists` to check `video_id` entries.
- `check_mp4s.sh`, `update_transcribed.sh`, and other scripts are included for common workflows — read each script header for usage.

## Development

- Use `cargo build` / `cargo run` as shown above.
- See `Cargo.toml` for crate dependencies (aws-config, aws-sdk-* crates, etc.).

## Contributing

Contributions are welcome. Open issues or pull requests for feature requests, bug fixes, or documentation improvements.

## License

MIT-style (no license file included in the repository). Add a `LICENSE` file if you need an explicit grant.
