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
Moves error/warnings + review json from staging bucket to production bucket if json
was produced by an agency in production. This is necessary as Michelle did not initially
provision a production bucket, rather only a staging one.

python -m recidiviz.tools.justice_counts.migrate_json \
  --dry-run=true
"""

import argparse
import logging

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def migrate_json(dry_run: bool) -> None:
    """
    Moves json from staging error/warnings + review bucket to production
    error/warnings + review bucket.
    """
    staging_bucket_name = "justice-counts-staging-bulk-upload-errors-warnings-json"
    production_bucket_name = (
        "justice-counts-production-bulk-upload-errors-warnings-json"
    )

    # this script should only be run in production
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(schema_type=schema_type):
        with SessionFactory.for_proxy(
            database_key,
            autocommit=False,
        ) as session:
            production_spreadsheets = set(
                spreadsheet_name_tuple[0]
                for spreadsheet_name_tuple in session.query(
                    schema.Spreadsheet.standardized_name
                ).all()
            )

            gcs_file_system = GcsfsFactory.build()
            logging.info(
                "Moving files from %s to %s",
                staging_bucket_name,
                production_bucket_name,
            )
            count_files_moved = 0
            for blob in gcs_file_system.ls(bucket_name=staging_bucket_name):
                if not isinstance(blob, GcsfsFilePath):
                    continue
                spreadsheet_name = blob.file_name.replace(".json", ".xlsx")
                if spreadsheet_name not in production_spreadsheets:
                    continue
                source_path = GcsfsFilePath.from_absolute_path(
                    f"{staging_bucket_name}/{blob.file_name}"
                )
                destination_path = GcsfsFilePath.from_absolute_path(
                    f"{production_bucket_name}/{blob.file_name}"
                )
                if gcs_file_system.exists(destination_path) is False:
                    count_files_moved += 1
                    if dry_run is False:
                        logging.info(
                            "Moving %s file",
                            blob.file_name,
                        )
                        # If prod json does not exist in the prod bucket,
                        # move the file to the prod bucket.
                        gcs_file_system.mv(source_path, destination_path)
                    if dry_run is True:
                        logging.info(
                            "Dry Run: filename is %s",
                            blob.file_name,
                        )
            logging.info("%s total files moved", count_files_moved)
            logging.info("Done!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    project_id = GCP_PROJECT_PRODUCTION
    with local_project_id_override(project_id):
        migrate_json(dry_run=args.dry_run)
