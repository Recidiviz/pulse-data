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
Copies files from buckets in recidivz-staging and recidiviz-123 to 
justice-counts-staging and justice-counts-production. This can be used to migrate
files from the legacy projects to the new projects.

python -m recidiviz.tools.justice_counts.migrate_bucket \
  --environment=STAGING \
  --source-bucket=recidiviz-staging-justice-counts-control-panel-ingest \
  --destination-bucket=justice-counts-staging-publisher-uploads \
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
    parser.add_argument(
        "--source-bucket",
        help="The bucket you want to copy files from",
        required=True,
    )
    parser.add_argument(
        "--destination-bucket",
        help="The bucket you want to copy files to",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def migrate_files(source_bucket: str, destination_bucket: str, dry_run: bool) -> None:
    """
    Copies files from buckets in recidiviz-staging and recidiviz-123
    to justice-counts-staging and justice-counts-production.
    """
    gcs_file_system = GcsfsFactory.build()
    logging.info("Moving files from %s to %s", source_bucket, destination_bucket)
    counter = 0
    for blob in gcs_file_system.ls(bucket_name=source_bucket):
        if isinstance(blob, GcsfsFilePath):
            source_path = GcsfsFilePath.from_absolute_path(
                f"{source_bucket}/{blob.file_name}"
            )
            destination_path = GcsfsFilePath.from_absolute_path(
                f"{destination_bucket}/{blob.file_name}"
            )
            if gcs_file_system.exists(destination_path) is False:
                # If file does not exist in the new project,
                # copy the file to the new project.
                counter += 1
                if dry_run is False:
                    logging.info(
                        "Copying %s file",
                        blob.file_name,
                    )
                    gcs_file_system.copy(source_path, destination_path)
                if dry_run is True:
                    logging.info(
                        "Dry Run: Would copy %s",
                        blob.file_name,
                    )
    if dry_run is False:
        logging.info(
            "Done! Moved %d files from %s to %s",
            counter,
            source_bucket,
            destination_bucket,
        )
    else:
        logging.info(
            "Done! Would have moved %d files from %s to %s",
            counter,
            source_bucket,
            destination_bucket,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    project_id = (
        GCP_PROJECT_STAGING if args.environment == "STAGING" else GCP_PROJECT_PRODUCTION
    )
    with local_project_id_override(project_id):
        migrate_files(
            dry_run=args.dry_run,
            source_bucket=args.source_bucket,
            destination_bucket=args.destination_bucket,
        )
