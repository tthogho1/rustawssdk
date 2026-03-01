#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="prefixes.txt"

if [ ! -f "$INPUT_FILE" ]; then
  echo "File not found: $INPUT_FILE" >&2
  exit 1
fi

if [ ! -f Cargo.toml ]; then
  echo "Cargo.toml not found in current directory; run this script from the project root" >&2
  exit 1
fi

# Use built executable instead of `cargo run`
BIN="./target/debug/rustawssdk"
if [ ! -x "$BIN" ]; then
  echo "Executable not found or not built: $BIN" >&2
  echo "Build it with: cargo build" >&2
  exit 1
fi

# File to collect video_ids that exist
FOUND_FILE="found.txt"
# start fresh
: > "$FOUND_FILE"
# File to collect video_ids that do not exist
NOT_FOUND_FILE="not_found.txt"
: > "$NOT_FOUND_FILE"

while IFS= read -r video_id || [ -n "$video_id" ]; do
  # Trim leading/trailing whitespace
  video_id="$(printf '%s' "$video_id" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"

  # Skip empty lines and comments
  case "$video_id" in
    ''|\#*) continue ;;
  esac

  echo "Checking video_id=${video_id} ..."
  # capture output (expected: "true" or "false")
  if ! out="$($BIN item-exists YoutubeList "video_id=${video_id}" 2>&1)"; then
    echo "Error: executable failed for video_id=${video_id}: $out" >&2
    exit 1
  fi

  # trim output
  out_trimmed="$(printf '%s' "$out" | tr -d '\r' | tr -d '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  if [ "$out_trimmed" = "true" ]; then
    echo "Found: ${video_id} -> $out_trimmed"
    echo "${video_id}" >> "$FOUND_FILE"
  else
    echo "Not found: ${video_id} -> $out_trimmed"
    echo "${video_id}" >> "$NOT_FOUND_FILE"
  fi
done < "$INPUT_FILE"
