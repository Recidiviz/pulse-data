# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
Copies files from aggregate buckets from recidivz-staging and recidiviz-123 to 
justice-counts-staging and justice-counts-production. This is a part of migrating 
Automatic Upload to the new Google Cloud Projects


python -m recidiviz.tools.justice_counts.migrate_spreadsheets \
  --environment=STAGING \
  --dry-run=true
"""

import argparse
import logging

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--environment",
        choices=["STAGING", "PRODUCTION"],
        help="Used to select which environment we should migrate",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def migrate_spreadsheets(environment: str, dry_run: bool) -> None:
    """
    Copies files from aggregate upload buckets in recidiviz-staging
    and recidiviz-123 to justice-counts-staging and justice-counts-production.
    """
    old_bucket_name = (
        "recidiviz-staging-justice-counts-ingest"
        if environment == "STAGING"
        else "recidiviz-123-justice-counts-control-panel-ingest"
    )
    new_bucket_name = (
        "justice-counts-staging-publisher-ingest"
        if environment == "STAGING"
        else "justice-counts-production-publisher-ingest"
    )
    gcs_file_system = GcsfsFactory.build()
    logging.info("Moving files from %s to %s", old_bucket_name, new_bucket_name)
    for blob in gcs_file_system.ls_with_blob_prefix(
        bucket_name=old_bucket_name, blob_prefix=""
    ):

        if isinstance(blob, GcsfsFilePath):
            if dry_run is False:
                logging.info(
                    "Copying %s file",
                    blob.file_name,
                )
                source_path = GcsfsFilePath.from_absolute_path(
                    f"{old_bucket_name}/{blob.file_name}"
                )
                destination_path = GcsfsFilePath.from_absolute_path(
                    f"{new_bucket_name}/{blob.file_name}"
                )
                gcs_file_system.copy(source_path, destination_path)
            if dry_run is True:
                logging.info(
                    "Dry Run: filename is %s",
                    blob.file_name,
                )
    logging.info("Done!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    project_id = (
        GCP_PROJECT_STAGING if args.environment == "STAGING" else GCP_PROJECT_PRODUCTION
    )
    with local_project_id_override(project_id):
        migrate_spreadsheets(
            environment=args.environment,
            dry_run=args.dry_run,
        )
