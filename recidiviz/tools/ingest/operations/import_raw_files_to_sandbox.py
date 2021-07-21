# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Script for testing that a set of raw files can be properly imported to BQ.

Given a set of state raw data files that have already been uploaded to a testing bucket,
imports those files to a sandbox BigQuery dataset using the specification in the raw
data config files.

This should be used to verify that a set of raw files properly parse. The resulting data
in the sandbox dataset can be compared against existing raw data tables more easily
(e.g. by comparing columns in the INFORMATION_SCHEMA.COLUMNS) tables.

Usage:

python -m recidiviz.tools.ingest.operations.import_raw_files_to_sandbox \
    --state_code US_PA --sandbox_dataset_prefix my_prefix \
    --source_bucket recidiviz-staging-my-test-bucket \
    [--file_tag_filter (tagA|otherTagB)]
"""

import argparse
import datetime
import logging
import re
import sys
from collections import defaultdict
from typing import List, Optional, Tuple

import google

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsDirectoryPath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    filename_parts_from_path,
)
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.regions import get_region


def check_is_valid_sandbox_bucket(bucket: GcsfsBucketPath) -> None:
    if (
        "test" not in bucket.bucket_name
        and "scratch" not in bucket.bucket_name
        and "sandbox" not in bucket.bucket_name
    ):
        raise ValueError(
            f"Invalid bucket [{bucket.bucket_name}] - must have 'test', "
            f"'sandbox', or 'scratch' in the name."
        )


class SandboxDirectIngestRawFileImportManager(DirectIngestRawFileImportManager):
    def __init__(
        self,
        *,
        state_code: StateCode,
        sandbox_dataset_prefix: str,
        test_ingest_bucket: GcsfsBucketPath,
    ):

        check_is_valid_sandbox_bucket(test_ingest_bucket)

        # Required to use the gcsfs.GCSFileSystem() locally
        credentials, _ = google.auth.default()
        super().__init__(
            region=get_region(state_code.value.lower(), is_direct_ingest=True),
            fs=DirectIngestGCSFileSystem(GcsfsFactory.build()),
            ingest_bucket_path=test_ingest_bucket,
            temp_output_directory_path=GcsfsDirectoryPath.from_dir_and_subdir(
                test_ingest_bucket, "temp_raw_data"
            ),
            big_query_client=BigQueryClientImpl(),
            credentials=credentials,
        )
        self.sandbox_dataset = (
            f"{sandbox_dataset_prefix}_{super()._raw_tables_dataset()}"
        )

    def _raw_tables_dataset(self) -> str:
        return self.sandbox_dataset


def do_upload(
    state_code: StateCode,
    sandbox_dataset_prefix: str,
    source_bucket: GcsfsBucketPath,
    file_tag_filter: Optional[str],
) -> None:
    """Imports a set of raw data files in the given source bucket into a sandbox
    dataset.
    """

    input_str = input(
        f"Have you already uploaded raw files to [{source_bucket.uri()}] using script "
        f"`recidiviz.tools.ingest.operations.upload_raw_state_files_to_ingest_bucket_with_date` "
        f"with arg `--destination-bucket {source_bucket.bucket_name}`?. [y/n] "
    )

    if input_str.upper() != "Y":
        return

    import_manager = SandboxDirectIngestRawFileImportManager(
        state_code=state_code,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        test_ingest_bucket=source_bucket,
    )

    bq_client = BigQueryClientImpl()

    # Create the dataset up front with table expiration
    bq_client.create_dataset_if_necessary(
        bq_client.dataset_ref_for_id(dataset_id=import_manager.sandbox_dataset),
        default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
    )

    raw_files_to_import = import_manager.get_unprocessed_raw_files_to_import()

    failures_by_exception = defaultdict(list)

    for i, file_path in enumerate(raw_files_to_import):
        parts = filename_parts_from_path(file_path)
        if file_tag_filter and not re.search(file_tag_filter, parts.file_tag):
            logging.info("** Skipping file with tag [%s] **", parts.file_tag)
            continue

        logging.info("Running file with tag [%s]", parts.file_tag)

        try:
            import_manager.import_raw_file_to_big_query(
                file_path,
                DirectIngestRawFileMetadata(
                    file_id=i,
                    region_code=state_code.value,
                    file_tag=parts.file_tag,
                    processed_time=None,
                    discovery_time=datetime.datetime.now(),
                    normalized_file_name=file_path.file_name,
                    datetimes_contained_upper_bound_inclusive=parts.utc_upload_datetime,
                ),
            )
        except Exception as e:
            logging.exception(e)
            failures_by_exception[str(e)].append(file_path.abs_path())

    if failures_by_exception:
        logging.error("************************* FAILURES ************************")
        total_files = 0
        all_failed_paths = []
        for error, file_list in failures_by_exception.items():
            total_files += len(file_list)
            all_failed_paths += file_list

            logging.error(
                "Failed [%s] files with error [%s]: %s",
                len(file_list),
                error,
                file_list,
            )
            logging.error("***********************************************************")
        raise ValueError(f"Failed to import [{total_files}] files: {all_failed_paths}")


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state_code",
        dest="state_code",
        help="State that these raw files are for, in the form US_XX.",
        type=str,
        choices=[state.value for state in StateCode],
        required=True,
    )

    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        help="A prefix to append to all names of the raw data dataset this data will be"
        "loaded into.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--source_bucket",
        type=str,
        default=True,
        help="A sandbox GCS bucket where raw files live. Files in this bucket must "
        "already have normalized file names.",
    )

    parser.add_argument(
        "--file_tag_filter",
        default=None,
        help="Regex file tag filter - when set, will only import files whose tags "
        "contain a match to this regex.",
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)
    with local_project_id_override(GCP_PROJECT_STAGING):
        do_upload(
            state_code=StateCode(known_args.state_code),
            sandbox_dataset_prefix=known_args.sandbox_dataset_prefix,
            source_bucket=GcsfsBucketPath(known_args.source_bucket),
            file_tag_filter=known_args.file_tag_filter,
        )
