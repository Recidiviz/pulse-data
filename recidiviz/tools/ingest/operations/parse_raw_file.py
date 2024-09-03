# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Verifies that a local raw file can be parsed successfully.

Provided a path (normalized or non-normalized) to a file on the local machine, this will
attempt to parse it in chunks based on the raw file configuration. In the event of
parsing or encoding errors, this allows for the file to be edited manually between calls
to narrow down the problem.

If the file is already uploaded to GCS, or you want to be able to explore the results in
BigQuery, use recidiviz.tools.ingest.operations.import_raw_files_to_sandbox.

Usage:
python -m recidiviz.tools.ingest.operations.parse_raw_file \
    --state-code US_ME \
    --file-path CIS_204_GEN_NOTE.csv \
    [--chunk-size 1000] \
    [--allow-incomplete-configs False]
"""

import argparse
import logging
import os
from typing import Optional
from unittest.mock import create_autospec

import pandas as pd

from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.raw_data.legacy_direct_ingest_raw_file_import_manager import (
    DirectIngestRawDataSplittingGcsfsCsvReaderDelegate,
    DirectIngestRawFileReader,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

TEST_BUCKET_PATH = "recidiviz-test-bucket"


class DebuggingGcsfsCsvReaderDelegate(
    DirectIngestRawDataSplittingGcsfsCsvReaderDelegate
):
    """CSV delegate that logs each call to aid in debugging."""

    def on_start_read_with_encoding(self, encoding: str) -> None:
        logging.info("Starting read...")

    def on_file_stream_normalization(
        self, old_encoding: str, new_encoding: str
    ) -> None:
        logging.info(
            "Stream requires normalization. Old encoding: [%s]. New encoding: [%s]",
            old_encoding,
            new_encoding,
        )

    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        logging.info(
            "Loaded DataFrame chunk [%d] has [%d] rows", chunk_num, df.shape[0]
        )

        df = self.transform_dataframe(df)

        logging.info("Last row processed after transformation: %s", df.tail(1))
        return True

    def on_unicode_decode_error(self, encoding: str, e: UnicodeError) -> bool:
        logging.info("Unable to read file with encoding [%s]", encoding)
        logging.exception(e)
        return False

    def on_exception(self, encoding: str, e: Exception) -> bool:
        logging.error("Failed to read.")
        return True

    def on_file_read_success(self, encoding: str) -> None:
        logging.info("Successfully read file with encoding [%s]", encoding)


def _add_file_to_fake_gcsfs(fs: FakeGCSFileSystem, source_path: str) -> GcsfsFilePath:
    bucket_path = GcsfsBucketPath(TEST_BUCKET_PATH)
    file_path = GcsfsFilePath.from_directory_and_file_name(
        bucket_path, os.path.basename(source_path)
    )
    fs.test_add_path(
        file_path,
        source_path,
    )

    # Normalize the file name, if it is not already.
    ingest_fs = DirectIngestGCSFileSystem(fs)
    if ingest_fs.is_normalized_file_path(file_path):
        return file_path

    return ingest_fs.mv_raw_file_to_normalized_path(file_path)


# TODO(#28239): remove once raw data import dag is fully rolled out
def main(
    state_code: StateCode,
    source_local_path: str,
    allow_incomplete_configs: bool,
    chunk_size: Optional[int],
) -> None:
    fs = FakeGCSFileSystem()
    file_path = _add_file_to_fake_gcsfs(fs, source_local_path)

    file_reader = DirectIngestRawFileReader(
        region_raw_file_config=DirectIngestRegionRawFileConfig(state_code.value),
        csv_reader=GcsfsCsvReader(fs),
        allow_incomplete_configs=allow_incomplete_configs,
    )

    file_reader.read_raw_file_from_gcs(
        path=file_path,
        delegate=DebuggingGcsfsCsvReaderDelegate(
            file_path,
            DirectIngestGCSFileSystem(fs),
            create_autospec(DirectIngestRawFileMetadata),  # unused, so mocked
            create_autospec(GcsfsDirectoryPath),  # unused, so mocked
            False,
        ),
        chunk_size_override=chunk_size,
    )


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state-code",
        help="State that these raw files is for, in the form US_XX.",
        type=str,
        choices=[state.value for state in StateCode],
        required=True,
    )

    parser.add_argument(
        "--file-path",
        type=str,
        help="Local path to file to attempt to parse.",
    )

    parser.add_argument(
        "--chunk-size",
        default=None,
        type=int,
        help="Override the chunk size to use when parsing the file.",
    )

    parser.add_argument(
        "--allow-incomplete-configs",
        type=str_to_bool,
        default=True,
        help="If not set, parsing will fail if there are columns found in the file "
        "that are not in the raw data config.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args = parse_arguments()
    with local_project_id_override(GCP_PROJECT_STAGING):
        main(
            StateCode(known_args.state_code),
            known_args.file_path,
            known_args.allow_incomplete_configs,
            known_args.chunk_size,
        )
