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
"""Script to scrub state data from all JSON files in a bucket. Each file in the bucket must contain
lines of JSON values, and the JSON must have a state_code attribute.

Example usage:
python -m recidiviz.tools.delete_state_records_from_gcs_json_files --bucket_name recidiviz-staging-snooze-status-archive --state_code US_OR
"""
import argparse
import json
import logging
import sys

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


def delete_state_records(bucket_name: str, state_code: str) -> None:
    gcsfs = GcsfsFactory.build()
    files = gcsfs.ls(bucket_name)
    for file in files:
        if not isinstance(file, GcsfsFilePath):
            continue
        if file.extension != "json":
            raise RuntimeError(
                f"Expected all files in bucket to have .json extension, found {file.file_name}"
            )
        new_file_contents = ""
        with gcsfs.open(file) as f:
            rows_to_keep = [
                row
                for row in f.readlines()
                if json.loads(row).get("state_code") != state_code
            ]
            new_file_contents = "".join(rows_to_keep)
        gcsfs.upload_from_string(file, new_file_contents, "application/json")
        logging.info("Removed %s data from %s", state_code, file.abs_path())


def parse_arguments(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bucket_name",
        required=True,
    )

    parser.add_argument(
        "--state_code",
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    delete_state_records(known_args.bucket_name, known_args.state_code)
