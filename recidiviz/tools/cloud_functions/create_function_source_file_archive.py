# Recidiviz - a data platform for criminal justice reform
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
This module provides functionality to create a zip archive of source files with a deterministic
checksum and upload the new archive to Google Cloud Storage (GCS) when it changes.

This module is intended to be run as a script with command-line arguments for
managing cloud function source files.

Usage:
python -m recidiviz.tools.cloud_functions.create_function_source_file_archive \
    --project_id {recidiviz-staging|recidiviz-123} \
    --dry_run {True|False} \
    --result_path {path_to_result_file}
"""
import argparse
import base64
import hashlib
import logging
import os
import pathlib
import tempfile
import zipfile
from io import BytesIO

import recidiviz
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.tools.file_dependencies import get_entrypoint_source_files
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.params import str_to_bool

ROOT = os.path.abspath(os.path.join(os.path.dirname(recidiviz.__file__), "../"))

# A fixed timestamp used for deterministic zipping
FIXED_DATE_TIME = (2025, 1, 1, 0, 0, 0)

# Status indicating the source has changed
CLOUD_FUNCTION_SOURCE_CHANGED = "changed"
# Status indicating the source is unchanged
CLOUD_FUNCTION_SOURCE_UNCHANGED = "unchanged"

# Path to the YAML file explicitly listing source files to copy (mainly for non-Python files)
SOURCE_FILE_YAML_PATH = os.path.join(
    os.path.dirname(__file__),
    "cloud_function_source_files_to_copy.yaml",
)


def get_cloud_function_source_archive_path() -> GcsfsFilePath:
    return GcsfsFilePath(
        bucket_name=f"{metadata.project_id()}-cloud-functions",
        blob_name="recidiviz-cloud-function-source.zip",
    )


def create_deterministic_zip(output_path: str, file_paths: set[str]) -> None:
    """Creates a zip archive with a fixed timestamp for reproducibility and returns its MD5 checksum."""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as zipf:
        for file_path in sorted(list(file_paths)):
            archive_location = str(pathlib.Path(file_path).relative_to(str(ROOT)))

            info = zipfile.ZipInfo(archive_location)
            info.date_time = FIXED_DATE_TIME
            info.compress_type = zipfile.ZIP_STORED

            with open(file_path, "rb") as f:
                data = f.read()

            logging.info("Adding file to zip: %s", archive_location)
            zipf.writestr(info, data)

    # Write to disk and calculate MD5
    with open(output_path, "wb") as f:
        f.write(zip_buffer.getvalue())


def upload_if_md5_differs(destination: GcsfsFilePath, source_file_path: str) -> bool:
    """Uploads a file to GCS only if its MD5 checksum differs from the existing file in GCS."""
    client = GcsfsFactory.build()
    with open(source_file_path, "rb") as f:
        # Calculate the MD5 checksum of the file
        file_data = f.read()
        local_md5 = base64.b64encode(
            hashlib.md5(file_data, usedforsecurity=False).digest()
        ).decode("utf-8")

    # Check if blob exists
    remote_md5: str | None = None
    if client.exists(destination):
        blob_metadata = client.get_metadata(destination)
        remote_md5 = (
            blob_metadata.get("md5_hash", None) if blob_metadata else None
        )  # base64-encoded MD5 from GCS

    logging.info("MD5 checksums local: %s remote: %s", local_md5, remote_md5)

    if local_md5 == remote_md5:
        return False

    logging.info("Uploading file to GCS: %s", destination)
    # Upload the file to GCS
    client.upload_from_contents_handle_stream(
        path=destination,
        contents_handle=LocalFileContentsHandle(local_file_path=source_file_path),
        content_type="application/zip",
        metadata={"md5_hash": local_md5},
    )

    return True


def create_function_source_file_archive(
    *,
    dry_run: bool,
    result_path: str | None = None,
) -> None:
    """
    Takes in arguments and copies appropriate files to the appropriate environment.
    """
    output_path = get_cloud_function_source_archive_path()
    with tempfile.TemporaryDirectory() as directory:
        source_files = get_entrypoint_source_files(
            [
                ("recidiviz/cloud_functions", "ingest_filename_normalization.py"),
            ],
            SOURCE_FILE_YAML_PATH,
        )

        zipfile_path = f"{directory}/{output_path.file_name}"
        create_deterministic_zip(zipfile_path, file_paths=source_files)

        if dry_run:
            logging.info("Dry run mode, not uploading to GCS.")
            return

        changed = upload_if_md5_differs(
            destination=output_path,
            source_file_path=zipfile_path,
        )

        if result_path:
            with open(result_path, "w", encoding="utf-8") as f:
                f.write(
                    CLOUD_FUNCTION_SOURCE_CHANGED
                    if changed
                    else CLOUD_FUNCTION_SOURCE_UNCHANGED
                )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Runs copy in dry-run mode, only prints the file copies it would do.",
    )

    parser.add_argument(
        "--dry_run",
        default=True,
        type=str_to_bool,
        help="Runs copy in dry-run mode, only prints the file copies it would do.",
    )

    parser.add_argument(
        "--result_path",
        required=False,
        type=str,
        help="Specifies the path to write the build results file to",
    )

    return parser


def _main() -> None:
    logging.getLogger().setLevel(logging.INFO)
    parser = create_parser()

    args = parser.parse_args()
    with metadata.local_project_id_override(args.project_id):
        create_function_source_file_archive(
            dry_run=args.dry_run,
            result_path=args.result_path,
        )


if __name__ == "__main__":
    _main()
