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
    [--file-tag-filter-regex (tagA|otherTagB)]
"""

import argparse
import logging
import sys
from typing import List, Optional, Tuple

from recidiviz.admin_panel.ingest_operations.ingest_utils import (
    import_raw_files_to_bq_sandbox,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.states import StateCode
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def do_sandbox_raw_file_import(
    state_code: StateCode,
    sandbox_dataset_prefix: str,
    source_bucket: GcsfsBucketPath,
    file_tag_filter_regex: Optional[str],
) -> None:
    """Imports a set of raw data files in the given source bucket into a sandbox
    dataset.
    """
    prompt_for_confirmation(
        f"Have you already uploaded raw files to [{source_bucket.uri()}] using script "
        f"`recidiviz.tools.ingest.operations.upload_raw_state_files_to_ingest_bucket_with_date` "
        f"with arg `--destination-bucket {source_bucket.bucket_name}`?"
    )

    import_raw_files_to_bq_sandbox(
        state_code=state_code,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        source_bucket=source_bucket,
        file_tag_filter_regex=file_tag_filter_regex,
    )


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
        "--file-tag-filter-regex",
        dest="file_tag_filter_regex",
        default=None,
        help="Regex file tag filter - when set, will only import files whose tags "
        "contain a match to this regex.",
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)
    with local_project_id_override(GCP_PROJECT_STAGING):
        do_sandbox_raw_file_import(
            state_code=StateCode(known_args.state_code),
            sandbox_dataset_prefix=known_args.sandbox_dataset_prefix,
            source_bucket=GcsfsBucketPath(known_args.source_bucket),
            file_tag_filter_regex=known_args.file_tag_filter_regex,
        )
