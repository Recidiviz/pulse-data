# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Test script to verify ingest is working correctly.

Example usage:
python -m recidiviz.tests.tools.development_scripts.justice_counts \
    --manifest-file recidiviz/tests/tools/justice_counts/fixtures/report1.yaml
"""

import argparse
import logging

from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.tools.justice_counts import test_utils
from recidiviz.tools.justice_counts import manual_upload
from recidiviz.tools.postgres import local_postgres_helpers


def _create_parser() -> argparse.ArgumentParser:
    """Creates the CLI argument parser."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--manifest-file",
        required=True,
        type=str,
        help="The yaml describing how to ingest the data",
    )
    parser.add_argument(
        "--log",
        required=False,
        default="INFO",
        type=logging.getLevelName,
        help="Set the logging level",
    )
    parser.add_argument(
        "--clean-up-db",
        required=False,
        default=False,
        type=bool,
        help="Automatically cleans up tmp-pgsql-db at end of run",
    )
    return parser


def _configure_logging(level: int) -> None:
    root = logging.getLogger()
    root.setLevel(level)


def cleanup_run(tmp_postgres_db_dir: str, clean_up_db: bool) -> None:
    if clean_up_db:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            tmp_postgres_db_dir
        )
        logging.info("Db automatically cleaned up")
    else:
        # Don't cleanup the database so that user can query the data afterward.
        logging.info(
            "For future cleanup, the postgres data directory is at %s.",
            tmp_postgres_db_dir,
        )
        logging.info(
            "To query the data, connect to the local database with "
            "`psql --dbname=recidiviz_test_db`"
        )


def run_justice_counts_ingest_locally(manifest_file: str, clean_up_db: bool) -> None:
    tmp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
    local_postgres_helpers.use_on_disk_postgresql_database(
        SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
    )

    fs = FakeGCSFileSystem()
    try:
        manual_upload.ingest(fs, test_utils.prepare_files(fs, manifest_file))
    finally:
        cleanup_run(tmp_db_dir, clean_up_db)


if __name__ == "__main__":
    arg_parser = _create_parser()
    arguments = arg_parser.parse_args()

    _configure_logging(arguments.log)

    run_justice_counts_ingest_locally(arguments.manifest_file, arguments.clean_up_db)
