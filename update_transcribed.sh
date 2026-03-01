#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="found.txt"
# allow overriding binary path via env var
BIN="${BIN:-./target/debug/rustawssdk}"

if [ ! -f "$INPUT_FILE" ]; then
  echo "File not found: $INPUT_FILE" >&2
  exit 1
fi

if [ ! -x "$BIN" ]; then
  echo "Executable not found or not built: $BIN" >&2
  echo "Build it with: cargo build" >&2
  exit 1
fi

while IFS= read -r video_id || [ -n "$video_id" ]; do
  # Trim leading/trailing whitespace
  video_id="$(printf '%s' "$video_id" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"

  # Skip empty lines and comments
  case "$video_id" in
    ''|\#*) continue ;;
  esac

  echo "Updating video_id=${video_id} ..."
  if ! "$BIN" set-attr YoutubeList transcribed 1 "video_id=${video_id}"; then
    echo "Error: update failed for video_id=${video_id}" >&2
    exit 1
  fi
done < "$INPUT_FILE"
