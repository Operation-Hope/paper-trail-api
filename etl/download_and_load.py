#!/usr/bin/env python3
"""
Download and optionally transform/load political contribution data.

Downloads DIME data from S3 bucket with year-based filtering for contributions.
"""

import argparse
import subprocess
import sys
import logging
from pathlib import Path
from typing import Tuple
import time
import os

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Election years (biennial, 1980-2024)
ELECTION_YEARS = list(range(1980, 2025, 2))

# S3 bucket configuration
S3_BUCKET = "s3://paper-trail-dime/filtered-parquet"


class DownloadConfig:
    """Configuration for download operations"""

    def __init__(self, year_range: Tuple[int, int], force: bool, dry_run: bool):
        self.year_range = year_range
        self.force = force
        self.dry_run = dry_run


class Prerequisites:
    """Check and validate prerequisites"""

    @staticmethod
    def check_aws_cli() -> bool:
        """Check if AWS CLI is installed"""
        try:
            result = subprocess.run(
                ['aws', '--version'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                logger.info(f"AWS CLI: {result.stdout.strip()}")
                return True
            else:
                logger.error("AWS CLI not found. Install with: brew install awscli")
                return False
        except FileNotFoundError:
            logger.error("AWS CLI not found. Install with: brew install awscli")
            return False

    @staticmethod
    def check_aws_credentials() -> bool:
        """Verify AWS credentials are configured"""
        try:
            result = subprocess.run(
                ['aws', 'sts', 'get-caller-identity'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                logger.info("AWS credentials: OK")
                return True
            else:
                logger.error("AWS credentials not configured or invalid")
                logger.error("Run: aws configure")
                return False
        except Exception as e:
            logger.error(f"AWS credentials check failed: {e}")
            return False

    @staticmethod
    def check_s3_access() -> bool:
        """Verify access to S3 bucket"""
        try:
            result = subprocess.run(
                ['aws', 's3', 'ls', f'{S3_BUCKET}/'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                logger.info("S3 bucket access: OK")
                return True
            else:
                logger.error(f"Cannot access {S3_BUCKET}")
                logger.error("Contact repository maintainers for bucket access")
                return False
        except Exception as e:
            logger.error(f"S3 bucket access check failed: {e}")
            return False

    @staticmethod
    def check_disk_space(required_gb: float) -> bool:
        """Check available disk space"""
        stat = os.statvfs('.')
        available_gb = (stat.f_bavail * stat.f_frsize) / (1024 ** 3)

        if available_gb < required_gb:
            logger.error(f"Insufficient disk space: {available_gb:.1f} GB available, {required_gb:.1f} GB required")
            return False

        logger.info(f"Disk space OK: {available_gb:.1f} GB available")
        return True

    @staticmethod
    def check_database_connection() -> bool:
        """Test DATABASE_URL connection if set"""
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            logger.warning("DATABASE_URL not set (required for --load)")
            return False

        try:
            import psycopg
            with psycopg.connect(database_url) as conn:
                logger.info("Database connection: OK")
                return True
        except ImportError:
            logger.error("psycopg not installed. Run: uv add psycopg")
            return False
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False


class DatabaseSetup:
    """Setup and initialize database"""

    @staticmethod
    def parse_database_url(database_url: str) -> dict:
        """Parse DATABASE_URL into components"""
        from urllib.parse import urlparse

        parsed = urlparse(database_url)
        return {
            'user': parsed.username,
            'password': parsed.password,
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path.lstrip('/') if parsed.path else 'postgres'
        }

    @staticmethod
    def ensure_database_exists(project_root: Path) -> bool:
        """Ensure database exists and has schema, create if needed"""
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            logger.error("DATABASE_URL not set")
            return False

        try:
            import psycopg
            from psycopg import sql
        except ImportError:
            logger.error("psycopg not installed. Run: uv add psycopg")
            return False

        # Parse DATABASE_URL
        db_params = DatabaseSetup.parse_database_url(database_url)
        target_db = db_params['database']

        logger.info(f"Checking database '{target_db}'...")

        # Connect to 'postgres' database to check if target exists
        server_url = database_url.rsplit('/', 1)[0] + '/postgres'

        try:
            with psycopg.connect(server_url) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    # Check if database exists
                    cur.execute(
                        "SELECT 1 FROM pg_database WHERE datname = %s",
                        (target_db,)
                    )
                    exists = cur.fetchone() is not None

                    if not exists:
                        logger.info(f"Creating database '{target_db}'...")
                        cur.execute(
                            sql.SQL("CREATE DATABASE {}").format(
                                sql.Identifier(target_db)
                            )
                        )
                        logger.info(f"✓ Database '{target_db}' created")
                    else:
                        logger.info(f"✓ Database '{target_db}' exists")

        except Exception as e:
            logger.error(f"Failed to check/create database: {e}")
            return False

        # Connect to target database and check for schema
        try:
            with psycopg.connect(database_url) as conn:
                with conn.cursor() as cur:
                    # Check if canonical_politician table exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_name = 'canonical_politician'
                        )
                    """)
                    schema_exists = cur.fetchone()[0]

                    if not schema_exists:
                        logger.info("Creating database schema...")
                        schema_path = project_root / "database/schema.sql"

                        if not schema_path.exists():
                            logger.error(f"Schema file not found: {schema_path}")
                            return False

                        # Read and execute schema.sql
                        schema_sql = schema_path.read_text()
                        cur.execute(schema_sql)
                        conn.commit()
                        logger.info("✓ Database schema created")
                    else:
                        logger.info("✓ Database schema exists")

        except Exception as e:
            logger.error(f"Failed to setup database schema: {e}")
            return False

        return True


class DimeDownloader:
    """Download DIME data from S3"""

    def __init__(self, config: DownloadConfig, project_root: Path):
        self.config = config
        self.contrib_dir = project_root / "data/raw/contributions"
        self.recipients_dir = project_root / "data/raw/recipients"
        self.contributors_dir = project_root / "data/raw/contributors"

    def download_all(self) -> Tuple[int, float]:
        """
        Download all DIME data files.
        Returns: (file_count, total_gb)
        """
        total_bytes = 0
        file_count = 0

        # Create directories
        self.contrib_dir.mkdir(parents=True, exist_ok=True)
        self.recipients_dir.mkdir(parents=True, exist_ok=True)
        self.contributors_dir.mkdir(parents=True, exist_ok=True)

        logger.info("\nDownloading DIME data from S3...")

        # Download recipients (always needed)
        logger.info("\nDownloading recipients file...")
        bytes_downloaded = self._download_file(
            f"{S3_BUCKET}/recipients/recipients_filtered.parquet",
            self.recipients_dir / "recipients_filtered.parquet",
            estimated_size=14 * 1024 * 1024  # ~14 MB
        )
        if bytes_downloaded > 0:
            total_bytes += bytes_downloaded
            file_count += 1

        # Download contributors (always needed)
        logger.info("\nDownloading contributors file...")
        bytes_downloaded = self._download_file(
            f"{S3_BUCKET}/contributors/contributors_all.parquet",
            self.contributors_dir / "contributors_all.parquet",
            estimated_size=2.1 * 1024 * 1024 * 1024  # ~2.1 GB
        )
        if bytes_downloaded > 0:
            total_bytes += bytes_downloaded
            file_count += 1

        # Download contribution files (year-filtered)
        logger.info("\nDownloading contribution files...")
        start_year, end_year = self.config.year_range
        years = [y for y in ELECTION_YEARS if start_year <= y <= end_year]

        for year in years:
            filename = f"contrib_{year}_filtered.parquet"
            logger.info(f"  Downloading {filename}...")
            bytes_downloaded = self._download_file(
                f"{S3_BUCKET}/contributions/{filename}",
                self.contrib_dir / filename,
                estimated_size=1.0 * 1024 * 1024 * 1024  # ~1 GB per year
            )
            if bytes_downloaded > 0:
                total_bytes += bytes_downloaded
                file_count += 1

        total_gb = total_bytes / (1024 ** 3)
        return file_count, total_gb

    def _download_file(self, s3_path: str, local_path: Path, estimated_size: int = 0) -> int:
        """
        Download a single file from S3.
        Returns: bytes downloaded (0 if skipped or dry-run)
        """
        # Skip if file exists and not forcing
        if local_path.exists() and not self.config.force:
            logger.info(f"  Skipping (already exists): {local_path.name}")
            return 0

        if self.config.dry_run:
            logger.info(f"  Would download: {s3_path} → {local_path} (~{estimated_size/(1024**2):.0f} MB)")
            return estimated_size

        try:
            # Download with AWS CLI
            result = subprocess.run(
                ['aws', 's3', 'cp', s3_path, str(local_path)],
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                logger.error(f"  Failed to download {s3_path}")
                logger.error(f"  Error: {result.stderr}")
                return 0

            # Get actual file size
            size_bytes = local_path.stat().st_size
            size_mb = size_bytes / (1024 ** 2)
            logger.info(f"  Downloaded {local_path.name} ({size_mb:.1f} MB)")
            return size_bytes

        except Exception as e:
            logger.error(f"  Error downloading {s3_path}: {e}")
            return 0


class VoteviewDownloader:
    """Download Voteview member data"""

    def __init__(self, config: DownloadConfig, project_root: Path):
        self.config = config
        self.voteview_dir = project_root / "data/raw/voteview"
        self.base_url = "https://voteview.com/static/data/out"

    def download_members(self) -> Tuple[int, float]:
        """
        Download HSall_members.csv.
        Returns: (file_count, total_gb)
        """
        self.voteview_dir.mkdir(parents=True, exist_ok=True)

        filename = "HSall_members.csv"
        local_path = self.voteview_dir / filename
        url = f"{self.base_url}/members/{filename}"

        # Skip if file exists and not forcing
        if local_path.exists() and not self.config.force:
            logger.info(f"Skipping (already exists): {filename}")
            return 0, 0.0

        if self.config.dry_run:
            logger.info(f"Would download: {filename} (~15 MB)")
            return 1, 0.015

        logger.info(f"Downloading {filename}...")

        try:
            result = subprocess.run(
                ['curl', '-L', '-o', str(local_path), url],
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                logger.error(f"Failed to download {filename}")
                return 0, 0.0

            size_bytes = local_path.stat().st_size
            size_mb = size_bytes / (1024 ** 2)
            logger.info(f"Downloaded {filename} ({size_mb:.1f} MB)")

            return 1, size_bytes / (1024 ** 3)

        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            return 0, 0.0


class TransformOrchestrator:
    """Run transformation scripts"""

    def __init__(self, project_root: Path, verbose: bool):
        self.project_root = project_root
        self.scripts_dir = project_root / "etl"
        self.verbose = verbose

    def transform_all(self) -> bool:
        """Run all required transform scripts"""
        scripts = [
            "transform_politicians.py",
            "transform_donors.py",
            "transform_contributions.py"
        ]

        logger.info("\n" + "=" * 60)
        logger.info("TRANSFORM PHASE")
        logger.info("=" * 60)

        for i, script_name in enumerate(scripts, 1):
            logger.info(f"\n[{i}/{len(scripts)}] Running {script_name}...")
            if not self._run_script(script_name):
                logger.error(f"Transform failed at {script_name}")
                return False

        logger.info("\nAll transformations completed successfully")
        return True

    def _run_script(self, script_name: str) -> bool:
        """Run a single transform script with output streaming"""
        script_path = self.scripts_dir / script_name

        if not script_path.exists():
            logger.error(f"Script not found: {script_path}")
            return False

        try:
            # Run script with uv
            cmd = ['uv', 'run', str(script_path)]

            # Stream output if verbose, otherwise capture
            if self.verbose:
                result = subprocess.run(cmd, cwd=self.project_root)
            else:
                result = subprocess.run(
                    cmd,
                    cwd=self.project_root,
                    capture_output=True,
                    text=True
                )
                # Show output if failed
                if result.returncode != 0:
                    logger.error(f"Script output:\n{result.stdout}")
                    logger.error(f"Script errors:\n{result.stderr}")

            return result.returncode == 0

        except Exception as e:
            logger.error(f"Error running {script_name}: {e}")
            return False


class LoadOrchestrator:
    """Run database load scripts"""

    def __init__(self, project_root: Path, verbose: bool):
        self.project_root = project_root
        self.scripts_dir = project_root / "etl"
        self.verbose = verbose

    def load_all(self) -> bool:
        """Run all required load scripts"""
        scripts = [
            "load_politicians.py",
            "load_donors.py",
            "load_contributions_optimized.py"
        ]

        logger.info("\n" + "=" * 60)
        logger.info("LOAD PHASE")
        logger.info("=" * 60)

        for i, script_name in enumerate(scripts, 1):
            logger.info(f"\n[{i}/{len(scripts)}] Running {script_name}...")
            if not self._run_script(script_name):
                logger.error(f"Load failed at {script_name}")
                return False

        logger.info("\nAll load scripts completed successfully")
        return True

    def _run_script(self, script_name: str) -> bool:
        """Run a single load script with output streaming"""
        script_path = self.scripts_dir / script_name

        if not script_path.exists():
            logger.error(f"Script not found: {script_path}")
            return False

        try:
            # Run script with uv
            cmd = ['uv', 'run', str(script_path)]

            # Stream output if verbose, otherwise capture
            if self.verbose:
                result = subprocess.run(cmd, cwd=self.project_root)
            else:
                result = subprocess.run(
                    cmd,
                    cwd=self.project_root,
                    capture_output=True,
                    text=True
                )
                # Show output if failed
                if result.returncode != 0:
                    logger.error(f"Script output:\n{result.stdout}")
                    logger.error(f"Script errors:\n{result.stderr}")

            return result.returncode == 0

        except Exception as e:
            logger.error(f"Error running {script_name}: {e}")
            return False


def estimate_download_size(start: int, end: int) -> float:
    """Estimate download size in GB for given year range"""
    years = [y for y in ELECTION_YEARS if start <= y <= end]

    # Base files (always downloaded)
    size_gb = 0.014  # recipients (~14 MB)
    size_gb += 2.1   # contributors (~2.1 GB)

    # Contribution files per year (~1 GB each)
    size_gb += len(years) * 1.0

    return size_gb


def main():
    parser = argparse.ArgumentParser(
        description="Download political contribution data from DIME S3 bucket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all data (1980-2024)
  %(prog)s

  # Download specific year
  %(prog)s --year 2020

  # Download year range
  %(prog)s --start-year 2016 --end-year 2024

  # Download + transform + load
  %(prog)s --year 2020 --transform --load

  # Dry run (show what would be downloaded)
  %(prog)s --year 2020 --dry-run

  # Force re-download existing files
  %(prog)s --force

Data sources:
  - DIME contributions from S3 (s3://paper-trail-dime/)
  - Voteview federal legislators

Prerequisites:
  - AWS CLI installed: brew install awscli
  - AWS credentials configured: aws configure
  - Access to S3 bucket (contact maintainers)
  - DATABASE_URL set (for --load)
"""
    )

    # Year selection
    year_group = parser.add_mutually_exclusive_group()
    year_group.add_argument("--year", type=int, help="Download single year (1980-2024)")
    year_group.add_argument("--start-year", type=int, help="Start year (requires --end-year)")
    parser.add_argument("--end-year", type=int, help="End year (requires --start-year)")

    # Pipeline control
    parser.add_argument("--transform", action="store_true",
                       help="Transform data after download")
    parser.add_argument("--load", action="store_true",
                       help="Load into database (requires --transform)")
    parser.add_argument("--skip-download", action="store_true",
                       help="Skip download, use existing files")

    # Options
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be downloaded without downloading")
    parser.add_argument("--force", action="store_true",
                       help="Re-download existing files")
    parser.add_argument("--verbose", action="store_true",
                       help="Show detailed logging from scripts")

    args = parser.parse_args()

    # Validate arguments
    if args.end_year and not args.start_year:
        parser.error("--end-year requires --start-year")

    if args.start_year and not args.end_year:
        parser.error("--start-year requires --end-year")

    if args.load and not args.transform:
        parser.error("--load requires --transform")

    # Determine year range
    if args.year:
        if args.year not in ELECTION_YEARS:
            parser.error(f"Year must be an election year: {', '.join(map(str, ELECTION_YEARS))}")
        year_range = (args.year, args.year)
    elif args.start_year and args.end_year:
        if args.start_year not in ELECTION_YEARS or args.end_year not in ELECTION_YEARS:
            parser.error(f"Years must be election years: {', '.join(map(str, ELECTION_YEARS))}")
        if args.start_year > args.end_year:
            parser.error("--start-year must be <= --end-year")
        year_range = (args.start_year, args.end_year)
    else:
        # Default: all years
        year_range = (ELECTION_YEARS[0], ELECTION_YEARS[-1])

    # Calculate election years in range
    years_in_range = [y for y in ELECTION_YEARS if year_range[0] <= y <= year_range[1]]

    # Display configuration
    logger.info("=" * 60)
    logger.info("Political Contribution Data Download & ETL")
    logger.info("=" * 60)
    logger.info(f"Year range: {year_range[0]}-{year_range[1]}")
    logger.info(f"Election years: {years_in_range}")
    logger.info(f"Download: {'No' if args.skip_download else 'Yes'}")
    logger.info(f"Transform: {'Yes' if args.transform else 'No'}")
    logger.info(f"Load: {'Yes' if args.load else 'No'}")
    logger.info(f"Dry run: {'Yes' if args.dry_run else 'No'}")
    logger.info("=" * 60)
    logger.info("Data sources:")
    logger.info("  - DIME contributions, recipients, contributors (S3)")
    logger.info("  - Voteview (federal legislators)")
    logger.info("=" * 60)

    # Estimate download size
    estimated_size = estimate_download_size(*year_range)
    logger.info(f"Estimated download size: {estimated_size:.1f} GB")
    logger.info("")

    # Check prerequisites (if downloading)
    if not args.skip_download:
        logger.info("Checking prerequisites...")

        if not Prerequisites.check_aws_cli():
            return 1

        if not Prerequisites.check_aws_credentials():
            return 1

        if not Prerequisites.check_s3_access():
            return 1

        if not Prerequisites.check_disk_space(estimated_size * 1.5):  # 1.5x for safety
            return 1

    project_root = Path(__file__).parent.parent

    # Setup database if loading
    if args.load:
        logger.info("\nSetting up database...")
        if not DatabaseSetup.ensure_database_exists(project_root):
            logger.error("Database setup failed")
            return 1
    config = DownloadConfig(year_range, args.force, args.dry_run)

    start_time = time.time()

    # DOWNLOAD PHASE
    if not args.skip_download:
        logger.info("\n" + "=" * 60)
        logger.info("DOWNLOAD PHASE")
        logger.info("=" * 60)

        # Download DIME data
        dime_downloader = DimeDownloader(config, project_root)
        dime_files, dime_gb = dime_downloader.download_all()

        # Download Voteview data
        voteview_downloader = VoteviewDownloader(config, project_root)
        logger.info("\nDownloading Voteview data...")
        voteview_files, voteview_gb = voteview_downloader.download_members()

        total_files = dime_files + voteview_files
        total_gb = dime_gb + voteview_gb

        if not args.dry_run:
            logger.info(f"\nDownload complete: {total_files} files, {total_gb:.2f} GB")
        else:
            logger.info(f"\nWould download: {total_files} files, {total_gb:.2f} GB")

    # TRANSFORM PHASE
    if args.transform:
        orchestrator = TransformOrchestrator(project_root, args.verbose)
        if not orchestrator.transform_all():
            logger.error("Transformation failed")
            return 1

    # LOAD PHASE
    if args.load:
        orchestrator = LoadOrchestrator(project_root, args.verbose)
        if not orchestrator.load_all():
            logger.error("Load failed")
            return 1

    # SUMMARY
    elapsed = time.time() - start_time
    minutes = elapsed / 60

    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)

    if not args.skip_download:
        if not args.dry_run:
            logger.info(f"Downloaded: {total_files} files ({total_gb:.2f} GB)")
        else:
            logger.info(f"Would download: {total_files} files ({total_gb:.2f} GB)")

    if args.transform:
        logger.info("Transformed: politicians, donors, contributions")

    if args.load:
        logger.info("Loaded: canonical_politician, donors, contributions tables")

    logger.info(f"Total time: {minutes:.1f} minutes")
    logger.info("=" * 60)

    # Next steps
    if not args.dry_run:
        logger.info("\nNext steps:")
        if not args.transform:
            logger.info("  1. Transform data: --transform")
            logger.info("  2. Load into database: --transform --load")
        elif not args.load:
            logger.info("  1. Load into database: --load (with --transform)")

        logger.info("\nData files location:")
        logger.info("  DIME contributions: data/raw/contributions/")
        logger.info("  DIME recipients: data/raw/recipients/")
        logger.info("  DIME contributors: data/raw/contributors/")
        logger.info("  Voteview: data/raw/voteview/")

    return 0


if __name__ == "__main__":
    sys.exit(main())
