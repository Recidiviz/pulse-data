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
"""Read and validate column headers for ingest raw files"""
import csv
from typing import IO, Any, Dict, List

from recidiviz.big_query.big_query_utils import normalize_column_name_for_bq
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig

DEFAULT_READ_CHUNK_SIZE = (
    10 * 1024
)  # 10 KiB should be more than enough to read the first row


class DirectIngestRawFileHeaderReader:
    """Class for reading, normalizing, and validating column headers for a raw ingest file"""

    def __init__(
        self,
        fs: GCSFileSystem,
        file_path: GcsfsFilePath,
        file_config: DirectIngestRawFileConfig,
        allow_incomplete_configs: bool = False,
    ):
        self.fs = fs
        self.file_path = file_path
        self.file_config = file_config
        # Allow for headers that are not found in the file config
        self.allow_incomplete_configs = allow_incomplete_configs

    def read_and_validate_column_headers(self) -> List[str]:
        """Reads the first row of a CSV file.
        If file_config.infer_columns_from_config is true,
        then return the headers in the order they're found in the file's config.
        Otherwise we can infer the first row of the csv contains the column headers
        and normalize and validate the column headers based on the provided configuration.
        """
        csv_first_row = self._read_csv_first_row()
        return self._normalize_and_validate_column_headers(csv_first_row)

    def _read_csv_first_row(self) -> List[str]:
        """Read csv until \r \n or custom_line_terminator
        then parse values according to file_config
        and return cell values as a list"""
        csv_reader_kwargs = self._get_csv_reader_kwargs()

        try:
            with self.fs.open(
                self.file_path,
                mode="r",
                encoding=self.file_config.encoding,
                chunk_size=DEFAULT_READ_CHUNK_SIZE,
            ) as f:
                updated_f = (
                    self._read_custom_terminated_line(
                        f, self.file_config.custom_line_terminator
                    )
                    if self.file_config.custom_line_terminator
                    else f
                )
                reader = csv.reader(updated_f, **csv_reader_kwargs)
                csv_first_row = next(reader)
        except StopIteration:
            csv_first_row = []
        except UnicodeDecodeError as e:
            raise ValueError(
                f"Unable to read path [{self.file_path.abs_path()}] for encoding {self.file_config.encoding}."
            ) from e

        if not csv_first_row:
            raise ValueError(
                f"File [{self.file_path.abs_path()}] is empty or does not contain valid rows."
            )

        return csv_first_row

    def _normalize_and_validate_column_headers(
        self, csv_first_row: List[str]
    ) -> List[str]:
        """Normalizes and validates the column headers based on the file configuration."""
        lowercase_to_file_config_cols = {
            col.name.lower(): col.name for col in self.file_config.columns
        }
        if self.file_config.infer_columns_from_config:
            return self._get_validated_columns_from_config(
                csv_first_row, lowercase_to_file_config_cols
            )

        normalized_csv_columns = set()
        for i, column_name in enumerate(csv_first_row):
            normalized_col = normalize_column_name_for_bq(column_name)
            caps_normalized_col = lowercase_to_file_config_cols.get(
                normalized_col.lower()
            )
            if not caps_normalized_col and not self.allow_incomplete_configs:
                raise ValueError(
                    f"Column name [{normalized_col}] not found in config for {self.file_config.file_tag}."
                )

            column_name = (
                caps_normalized_col
                if caps_normalized_col is not None
                else normalized_col
            )
            if column_name in normalized_csv_columns:
                raise ValueError(
                    f"Multiple columns with name [{column_name}] after normalization."
                )

            normalized_csv_columns.add(column_name)
            csv_first_row[i] = column_name

        return csv_first_row

    def _get_validated_columns_from_config(
        self, csv_first_row: List[str], lowercase_to_file_config_cols: Dict[str, str]
    ) -> List[str]:
        columns_from_file_config = [column.name for column in self.file_config.columns]
        if len(columns_from_file_config) != len(csv_first_row):
            raise ValueError(
                f"Found {len(columns_from_file_config)} columns defined in {self.file_config.file_tag} "
                f"but found {len(csv_first_row)} in the CSV. Make sure all expected columns are "
                f"defined in the raw data configuration."
            )

        # Check if any of the values in the first row are found in the file config
        # if so we can assume that this is a header row
        config_columns_set = set(columns_from_file_config)
        for possible_column_name in csv_first_row:
            if not possible_column_name:
                continue
            normalized_col = normalize_column_name_for_bq(possible_column_name)
            caps_normalized_col = lowercase_to_file_config_cols.get(
                normalized_col.lower()
            )
            if caps_normalized_col in config_columns_set:
                raise ValueError(
                    "Found unexpected header in the CSV. Please remove the header row from the CSV"
                )

        return columns_from_file_config

    @staticmethod
    def _read_custom_terminated_line(f: IO, line_terminator: str) -> List[str]:
        """Python csv reader is hard-coded to recognise either '\r' or '\n' as end-of-line
        https://docs.python.org/3/library/csv.html#csv.Dialect.lineterminator

        So if there is a custom line terminator manually parse until we encounter it or EOF
        """
        line = ""
        while True:
            char = f.read(1)
            if not char:
                break
            line += char
            if line.endswith(line_terminator):
                break
        return [line.rstrip(line_terminator)]

    def _get_csv_reader_kwargs(self) -> Dict[str, Any]:
        return {
            "delimiter": self.file_config.separator,
            "quoting": (
                csv.QUOTE_NONE if self.file_config.ignore_quotes else csv.QUOTE_MINIMAL
            ),
        }
