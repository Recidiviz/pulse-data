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
    --state-code US_PA --sandbox-dataset-prefix my_prefix \
    --source-bucket recidiviz-staging-my-test-bucket \
    [--file-tag-filter-regex (tagA|otherTagB)] \
    [--allow-incomplete-configs False]
"""

import argparse
import logging
import re
from typing import List, Optional, Tuple

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import import (
    SandboxImportStatus,
    SandboxRawFileImportResult,
    legacy_import_raw_files_to_bq_sandbox,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def get_unprocessed_raw_files_in_bucket(
    fs: DirectIngestGCSFileSystem,
    bucket_path: GcsfsBucketPath,
    region_raw_file_config: DirectIngestRegionRawFileConfig,
    file_tag_filters: Optional[List[str]],
) -> Tuple[List[GcsfsFilePath], List[SandboxRawFileImportResult]]:
    """Returns a list of paths to unprocessed raw files in the provided bucket that have
    registered file tags for a given region.
    """
    unprocessed_paths = fs.get_unprocessed_raw_file_paths(bucket_path)
    unprocessed_raw_files = []
    skipped_files = []
    for path in unprocessed_paths:
        parts = filename_parts_from_path(path)
        if parts.file_tag in region_raw_file_config.raw_file_tags:
            if file_tag_filters is not None and parts.file_tag not in file_tag_filters:
                skipped_files.append(
                    SandboxRawFileImportResult(
                        path=path,
                        status=SandboxImportStatus.SKIPPED,
                        error_message="Excluded by file_tag_filters",
                    )
                )
            else:
                unprocessed_raw_files.append(path)
        else:
            skipped_files.append(
                SandboxRawFileImportResult(
                    path=path,
                    status=SandboxImportStatus.SKIPPED,
                    error_message="Unrecognized file tag",
                )
            )

    return unprocessed_raw_files, skipped_files


def do_sandbox_raw_file_import(
    state_code: StateCode,
    sandbox_dataset_prefix: str,
    source_bucket: GcsfsBucketPath,
    file_tag_filter_regex: Optional[str],
    allow_incomplete_configs: bool,
) -> None:
    """Imports a set of raw data files in the given source bucket into a sandbox
    dataset.
    """

    file_tag_filters = None
    if file_tag_filter_regex:
        file_tag_filters = []
        region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code=state_code.value.lower()
        )
        for raw_file_tag in region_raw_file_config.raw_file_tags:
            if re.search(file_tag_filter_regex, raw_file_tag):
                file_tag_filters.append(raw_file_tag)

    fs = DirectIngestGCSFileSystem(GcsfsFactory.build())

    files_to_import, skipped_files = get_unprocessed_raw_files_in_bucket(
        fs,
        source_bucket,
        region_raw_file_config,
        file_tag_filters,
    )

    logging.info("******************** Sandbox Plan ***********************")
    logging.info(
        "[%s] Unprocessed files skipped: \n\t-%s",
        len(skipped_files),
        "\n\t-".join(
            [skipped_file.format_for_print() for skipped_file in skipped_files]
        ),
    )
    logging.info(
        "[%s] Unprocessed files to import: \n\t-%s",
        len(files_to_import),
        "\n\t-".join([path.blob_name for path in files_to_import]),
    )

    prompt_for_confirmation(
        f"Proceed with sandbox import of [{len(files_to_import)}] files?"
    )

    sandbox_import_result = legacy_import_raw_files_to_bq_sandbox(
        state_code=state_code,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        files_to_import=files_to_import,
        allow_incomplete_configs=allow_incomplete_configs,
        big_query_client=BigQueryClientImpl(),
        fs=fs,
    )

    # TODO(#34523): add gated update to use new import code

    logging.info("****************** RESULTS ***********************")
    for status in list(SandboxImportStatus):
        for file_result in sandbox_import_result.status_to_imports.get(status, []):
            logging.info("\t-%s", file_result.format_for_print())
    logging.info("***********************************************************")


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state-code",
        help="State that these raw files are for, in the form US_XX.",
        type=str,
        choices=[state.value for state in StateCode],
        required=True,
    )

    parser.add_argument(
        "--sandbox-dataset-prefix",
        help="A prefix to append to all names of the raw data dataset this data will be"
        "loaded into.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--source-bucket",
        type=str,
        required=True,
        help="A sandbox GCS bucket where raw files live. Files in this bucket must "
        "already have normalized file names.",
    )

    parser.add_argument(
        "--file-tag-filter-regex",
        default=None,
        help="Regex file tag filter - when set, will only import files whose tags "
        "contain a match to this regex.",
    )

    # TODO(#34523) update this flag to be --allow-incomplete-columns?
    parser.add_argument("--allow-incomplete-configs", type=str_to_bool, default=True)

    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args = parse_arguments()
    with local_project_id_override(GCP_PROJECT_STAGING):
        do_sandbox_raw_file_import(
            state_code=StateCode(known_args.state_code),
            sandbox_dataset_prefix=known_args.sandbox_dataset_prefix,
            source_bucket=GcsfsBucketPath(known_args.source_bucket),
            file_tag_filter_regex=known_args.file_tag_filter_regex,
            allow_incomplete_configs=known_args.allow_incomplete_configs,
        )
