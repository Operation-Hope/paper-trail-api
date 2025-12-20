#!/bin/bash
# download_contributions.sh - Download DIME contribution data from S3
#
# Downloads campaign contribution data from the private S3 bucket:
# - contrib_YYYY_filtered.parquet: Contribution records by election cycle
# - recipients_filtered.parquet: Politician/recipient records
# - contributors_all.parquet: Donor records
#
# PREREQUISITES:
#   1. AWS CLI installed: brew install awscli
#   2. AWS credentials configured: aws configure
#   3. Access granted to s3://paper-trail-dime/ bucket
#
# To request S3 access, contact the repository maintainers.
#
# Source: DIME (Database on Ideology, Money in Politics, and Elections)
# Original: https://data.stanford.edu/dime

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CONTRIBUTIONS_DIR="$PROJECT_ROOT/data/raw/contributions"
RECIPIENTS_DIR="$PROJECT_ROOT/data/raw/recipients"
CONTRIBUTORS_DIR="$PROJECT_ROOT/data/raw/contributors"

S3_BUCKET="s3://paper-trail-dime/filtered-parquet"

# Check AWS credentials
if ! aws sts get-caller-identity &>/dev/null; then
  echo "ERROR: AWS credentials not configured or invalid."
  echo ""
  echo "To configure AWS credentials:"
  echo "  aws configure"
  echo ""
  echo "To request S3 bucket access, contact the repository maintainers."
  exit 1
fi

# Verify bucket access
echo "Verifying S3 bucket access..."
if ! aws s3 ls "$S3_BUCKET/" &>/dev/null; then
  echo "ERROR: Cannot access $S3_BUCKET"
  echo ""
  echo "Your AWS credentials may not have access to this bucket."
  echo "Contact the repository maintainers to request access."
  exit 1
fi

# Create directories
mkdir -p "$CONTRIBUTIONS_DIR"
mkdir -p "$RECIPIENTS_DIR"
mkdir -p "$CONTRIBUTORS_DIR"

echo "Downloading DIME data from S3..."
echo ""

# Download contribution files (23 election cycles, ~24GB total)
echo "Downloading contribution files (~24GB total, this will take a while)..."
YEARS=(1980 1982 1984 1986 1988 1990 1992 1994 1996 1998 2000 2002 2004 2006 2008 2010 2012 2014 2016 2018 2020 2022 2024)

for year in "${YEARS[@]}"; do
  FILE="contrib_${year}_filtered.parquet"
  echo "  Downloading $FILE..."
  aws s3 cp "$S3_BUCKET/contributions/$FILE" "$CONTRIBUTIONS_DIR/$FILE"
done

echo ""
echo "Downloading recipients file..."
aws s3 cp "$S3_BUCKET/recipients/recipients_filtered.parquet" \
  "$RECIPIENTS_DIR/recipients_filtered.parquet"

echo ""
echo "Downloading contributors file (~2.1GB)..."
aws s3 cp "$S3_BUCKET/contributors/contributors_all.parquet" \
  "$CONTRIBUTORS_DIR/contributors_all.parquet"

echo ""
echo "Download complete!"
echo ""
echo "Contribution files:"
ls -lh "$CONTRIBUTIONS_DIR"/*.parquet | head -5
echo "  ... and $(ls "$CONTRIBUTIONS_DIR"/*.parquet | wc -l | tr -d ' ') total files"
echo ""
echo "Recipients file:"
ls -lh "$RECIPIENTS_DIR"/*.parquet
echo ""
echo "Contributors file:"
ls -lh "$CONTRIBUTORS_DIR"/*.parquet
echo ""
echo "Total size:"
du -sh "$PROJECT_ROOT/data/raw"
