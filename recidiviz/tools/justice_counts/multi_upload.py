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
"""Ingests multiple regions into the Justice Counts database.
How to get the credentials directory can be found in google_drive.py
To get ingest automitically you need to have the environment variable GOOGLE_APPLICATION_CREDENTIALS set,
instructions for that can be found in manual_upload.py

Example usage:
python -m recidiviz.tools.justice_counts.multi_upload \
    --base-directory recidiviz/justice-counts-data \
    --system CORRECTIONS \
    --drive-folder-id abc123 \
    --credentials-directory ~/credentials \
    --project-id recidiviz-staging \
    --filter-type include \
    --regions US_PA US_WI
python -m recidiviz.tools.justice_counts.multi_upload \
    --base-directory recidiviz/justice-counts-data \
    --system CORRECTIONS \
    --drive-folder-id abc123 \
    --credentials-directory ~/credentials \
    --project-id recidiviz-staging \
    --app-url http://127.0.0.1:5000
"""
import argparse
import enum
import logging
import os
import traceback
from typing import List, Optional, Set

import attr

from recidiviz.common.constants import states
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.tools.development_scripts.justice_counts import cleanup_run
from recidiviz.tests.tools.justice_counts import test_utils
from recidiviz.tools.justice_counts import manual_upload
from recidiviz.tools.justice_counts.google_drive import download_data
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils import metadata


class FilterType(enum.Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"


@attr.s
class IngestResult:
    region_code: str = attr.ib()
    success: bool = attr.ib()
    error: Optional[Exception] = attr.ib()


def _get_list_of_regions(
    filter_type: Optional[FilterType], regions: Optional[List[str]]
) -> List[str]:
    """list of acceptable regions to ingest"""
    region_codes = [state_code.value for state_code in StateCode]
    if not filter_type or not regions:
        return region_codes

    if filter_type == FilterType.INCLUDE:
        return regions
    if filter_type == FilterType.EXCLUDE:
        return [region for region in region_codes if region not in regions]

    return region_codes


def _run_ingest(
    fs: FakeGCSFileSystem,
    region_code: str,
    repo_directory: str,
    system: schema.System,
    app_url: Optional[str],
) -> None:
    """
    For each manifest file in state directory, it runs local ingest and if that is successful,
    will then run manual upload.
    """
    region_directory = f"{repo_directory}/{region_code}/{system.value}/"
    manifest_files = [
        os.path.join(region_directory, name)
        for name in os.listdir(region_directory)
        if os.path.isfile(os.path.join(region_directory, name))
        and name.endswith(".yaml")
    ]

    table_file_names = {
        name
        for name in os.listdir(region_directory)
        if os.path.isfile(os.path.join(region_directory, name))
        and (name.endswith(".csv") or name.endswith(".tsv"))
    }

    logging.info("Found manifest files: ")
    logging.info(manifest_files)

    ingested_file_names: Set[str] = set()

    for manifest_file in manifest_files:
        ingested_file_names.update(
            manual_upload.ingest(fs, test_utils.prepare_files(fs, manifest_file))
        )
        manual_upload.main(manifest_file, app_url)

    missed_files = table_file_names.difference(ingested_file_names)

    if len(missed_files) > 0:
        raise ValueError(f"Files not ingested in any manifest file: {missed_files}")


def _full_ingest_region(
    fs: FakeGCSFileSystem,
    region_code: str,
    repo_directory: str,
    system: schema.System,
    drive_folder_id: str,
    credentials_directory: str,
    app_url: Optional[str],
) -> IngestResult:
    """
    For given state code, will attempt to download latest information from google drive,
    test an ingest, and then actually ingest the data
    """
    try:
        logging.info("ingesting %s", region_code)
        download_data(
            states.StateCode(region_code),
            system,
            drive_folder_id,
            repo_directory,
            credentials_directory,
        )

        _run_ingest(fs, region_code, repo_directory, system, app_url)

        logging.info("ingesting %s done", region_code)
        return IngestResult(region_code, True, None)
    except Exception as e:
        if e is KeyboardInterrupt:
            raise
        logging.error("Error occurred while ingesting %s", region_code)
        traceback.print_exc()
        return IngestResult(region_code, False, e)


def _create_parser() -> argparse.ArgumentParser:
    """Creates the CLI argument parser."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--project-id",
        required=True,
        type=str,
        help="The GCP project to ingest the data into",
    )
    parser.add_argument(
        "--app-url", required=False, type=str, help="Override the url of the app."
    )
    parser.add_argument(
        "--system",
        required=True,
        choices=[system.value for system in schema.System],
    )
    parser.add_argument(
        "--base-directory",
        required=True,
        type=str,
        help="The base local directory to write downloaded files to.",
    )
    parser.add_argument(
        "--drive-folder-id",
        required=True,
        type=str,
        help="The id for the folder root Justice Counts Data Collection folder, which contains subdirectories with all "
        "of the states. The id is the last part of the url, e.g. 'abc123' from "
        "'https://drive.google.com/drive/folders/abc123'.",
    )
    parser.add_argument(
        "--credentials-directory",
        required=False,
        default=".",
        type=str,
        help="Directory where the 'credentials.json' live, as well as the cached token.",
    )
    parser.add_argument(
        "--log",
        required=False,
        default="INFO",
        type=logging.getLevelName,
        help="Set the logging level",
    )
    parser.add_argument(
        "--filter-type",
        required=False,
        type=str,
        choices=[filter_type.value for filter_type in FilterType],
        default=FilterType.INCLUDE.value,
        help="state filter, can be 'include' or 'exclude'",
    )
    parser.add_argument(
        "--regions",
        required=False,
        default=[],
        nargs="+",
        help="List of regions ex. 'US_WI US_PA' to either include or exclude.",
    )
    return parser


def _configure_logging(level: str) -> None:
    root = logging.getLogger()
    root.setLevel(level)


def main(
    repo_directory: str,
    system: schema.System,
    base_drive_folder_id: str,
    credentials_directory: str,
    app_url: Optional[str],
    filter_type: Optional[FilterType],
    regions: Optional[List[str]],
) -> None:
    """
    Downloads, tests, and ingests specified regions
    """
    regions_to_ingest = _get_list_of_regions(filter_type, regions)

    logging.info("Starting ingest of regions...")
    logging.info(regions_to_ingest)

    tmp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
    local_postgres_helpers.use_on_disk_postgresql_database(
        SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
    )
    fs = FakeGCSFileSystem()

    region_ingest_summary = []

    try:
        for region in regions_to_ingest:
            region_ingest_summary.append(
                _full_ingest_region(
                    fs,
                    region,
                    repo_directory,
                    system,
                    base_drive_folder_id,
                    credentials_directory,
                    app_url,
                )
            )
    finally:
        cleanup_run(tmp_db_dir, True)

    for ingest_result in region_ingest_summary:
        if ingest_result.success:
            logging.info("%s: success", ingest_result.region_code)
        else:
            logging.error(
                "%s: failed - %s", ingest_result.region_code, ingest_result.error
            )


if __name__ == "__main__":
    arg_parser = _create_parser()
    arguments = arg_parser.parse_args()

    _configure_logging(arguments.log)

    with metadata.local_project_id_override(arguments.project_id):
        main(
            arguments.base_directory,
            schema.System(arguments.system),
            arguments.drive_folder_id,
            arguments.credentials_directory,
            app_url=arguments.app_url,
            filter_type=FilterType(arguments.filter_type),
            regions=arguments.regions,
        )
