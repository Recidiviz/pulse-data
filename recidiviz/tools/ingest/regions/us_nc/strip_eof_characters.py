#!/usr/bin/env python3
# Recidiviz - a data platform for criminal justice reform.
# Copyright (C) 2025 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# =============================================================================
"""
Script to download files from GCS bucket, strip Windows EOF characters,
and re-upload them back to the same bucket.

We can delete this script after TODO(#53673) is completed.

Usage:
    python -m recidiviz.tools.ingest.regions.us_nc.strip_eof_characters \
        --bucket recidiviz-staging-direct-ingest-state-us-nc \
        [--dry-run]
"""

import argparse
import logging
import sys
from typing import Optional

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def strip_windows_eof(content: bytes) -> bytes:
    """
    Strip Windows EOF character from the end of content.

    Removes the DOS EOF marker (\\x1a / Ctrl+Z) if it is the last character.

    Args:
        content: The file content as bytes

    Returns:
        The cleaned content as bytes
    """
    # Only remove DOS EOF marker (0x1a) if it's the very last character
    if content and content[-1:] == b"\x1a":
        content = content[:-1]

    return content


def process_bucket(
    gcsfs: GCSFileSystem,
    bucket_name: str,
    dry_run: bool = False,
) -> None:
    """
    Process all files in a GCS bucket, stripping Windows EOF characters.

    Args:
        gcsfs: The GCSFileSystem instance
        bucket_name: Name of the bucket to process
        dry_run: If True, don't actually upload files, just report what would be done
    """
    # List all files in the bucket
    paths = gcsfs.ls(bucket_name)

    if not paths:
        logger.info("No files found in bucket: %s", bucket_name)
        return

    # Filter to only files (skip directories)
    file_paths = [path for path in paths if isinstance(path, GcsfsFilePath)]

    if not file_paths:
        logger.info("No files found in bucket: %s", bucket_name)
        return

    logger.info("Found %d files in bucket: %s", len(file_paths), bucket_name)

    processed_count = 0
    skipped_count = 0
    error_count = 0

    for file_path in file_paths:
        try:
            file_name = file_path.file_name
            logger.info("Processing: %s", file_path)

            # Download file content
            content = gcsfs.download_as_bytes(file_path)
            original_content = content

            # Strip Windows EOF characters
            content = strip_windows_eof(content)

            # Check if content changed
            if content == original_content:
                logger.info("  No Windows EOF characters found, skipping")
                skipped_count += 1
                continue

            # Calculate statistics
            size_diff = len(original_content) - len(content)
            logger.info("  Stripped %d bytes of EOF characters", size_diff)

            if dry_run:
                logger.info("  [DRY RUN] Would re-upload %s", file_name)
            else:
                # Re-upload the cleaned file
                # Decode bytes to string for upload
                content_str = content.decode("utf-8")
                gcsfs.upload_from_string(
                    file_path, content_str, content_type="text/plain"
                )
                logger.info("  Successfully re-uploaded %s", file_name)

            processed_count += 1

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error processing %s: %s", file_path, e)
            error_count += 1

    # Summary
    logger.info("=" * 60)
    logger.info("Processing Summary:")
    logger.info("  Processed: %d", processed_count)
    logger.info("  Skipped: %d", skipped_count)
    logger.info("  Errors: %d", error_count)
    if dry_run:
        logger.info("  [DRY RUN MODE - No files were actually uploaded]")
    logger.info("=" * 60)


def main(argv: Optional[list[str]] = None) -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Strip Windows EOF characters from files in a GCS bucket"
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket name",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be done without making changes",
    )

    args = parser.parse_args(argv)

    try:
        gcsfs = GcsfsFactory.build()
        process_bucket(gcsfs, args.bucket, dry_run=args.dry_run)
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Fatal error: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
