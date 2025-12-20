#!/bin/bash
# download_voteview.sh - Download Voteview data files
#
# Downloads congressional voting data from voteview.com:
# - HSall_members.csv: Legislator records (1789-present)
# - HSall_votes.csv: Individual votes (1789-present)
# - HSall_rollcalls.csv: Roll call metadata (1789-present)
#
# Source: https://voteview.com/data
# License: CC BY-NC 4.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT_DIR="$PROJECT_ROOT/data/raw/voteview"
BACKUP_DIR="$PROJECT_ROOT/database"

# Create directories if needed
mkdir -p "$OUTPUT_DIR"
mkdir -p "$BACKUP_DIR"

BASE_URL="https://voteview.com/static/data/out"

echo "Downloading Voteview data to: $OUTPUT_DIR"
echo ""

# Download members file
echo "Downloading HSall_members.csv..."
curl -L -o "$OUTPUT_DIR/HSall_members.csv" \
  "$BASE_URL/members/HSall_members.csv"
echo "  Size: $(du -h "$OUTPUT_DIR/HSall_members.csv" | cut -f1)"

# Download votes file (large - ~700MB)
echo "Downloading HSall_votes.csv (this may take a while)..."
curl -L -o "$OUTPUT_DIR/HSall_votes.csv" \
  "$BASE_URL/votes/HSall_votes.csv"
echo "  Size: $(du -h "$OUTPUT_DIR/HSall_votes.csv" | cut -f1)"

# Download rollcalls file
echo "Downloading HSall_rollcalls.csv..."
curl -L -o "$OUTPUT_DIR/HSall_rollcalls.csv" \
  "$BASE_URL/rollcalls/HSall_rollcalls.csv"
echo "  Size: $(du -h "$OUTPUT_DIR/HSall_rollcalls.csv" | cut -f1)"

# Create backup copies in database/ directory
echo ""
echo "Creating backup copies in $BACKUP_DIR..."
cp "$OUTPUT_DIR/HSall_members.csv" "$BACKUP_DIR/"
cp "$OUTPUT_DIR/HSall_votes.csv" "$BACKUP_DIR/"
cp "$OUTPUT_DIR/HSall_rollcalls.csv" "$BACKUP_DIR/"

echo ""
echo "Download complete!"
echo ""
echo "Files downloaded:"
ls -lh "$OUTPUT_DIR"/*.csv
echo ""
echo "Backup copies:"
ls -lh "$BACKUP_DIR"/HSall_*.csv
