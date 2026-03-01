#!/usr/bin/env bash
set -euo pipefail

# Usage: ./check_mp4s.sh [input_file]
# Default input file: output.gladia

INPUT_FILE="${1:-output.gladia}"
BIN="./target/debug/rustawssdk"

if [ ! -f "$INPUT_FILE" ]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 2
fi

while IFS= read -r line || [ -n "$line" ]; do
  # Trim whitespace
  file=$(printf '%s' "$line" | awk '{$1=$1};1')
  [ -z "$file" ] && continue

  # Remove any trailing CR (Windows line endings) and get basename safely
  file="${file%$'\r'}"
  base=$(basename -- "$file")
  id="${base%.*}"

  # Skip if id is empty
  [ -z "$id" ] && continue

  # Prefer an executable specified by $BIN, otherwise call `item-exists` directly.
  cmd="${BIN-}"
  if [ -z "$cmd" ]; then
    cmd="item-exists"
  fi

  # Run the command and capture its output (expected: "true" or "false").
  if ! out="$($cmd item-exists YoutubeList "video_id=${id}" 2>&1)"; then
    echo "Error: executable failed for video_id=${id}: $out" >&2
    exit 1
  fi

  # Normalize output (trim) and decide
  out=$(printf '%s' "$out" | awk '{$1=$1};1')
  case "$out" in
    true|True|TRUE)
      printf '%s: exists\n' "$id"
      ;;
    false|False|FALSE)
      printf '%s: missing\n' "$id"
      ;;
    *)
      printf '%s: unknown (output: %s)\n' "$id" "$out"
      ;;
  esac
done < "$INPUT_FILE"

exit 0
