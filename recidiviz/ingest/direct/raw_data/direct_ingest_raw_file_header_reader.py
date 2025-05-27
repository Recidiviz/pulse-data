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

import attr

from recidiviz.big_query.big_query_utils import normalize_column_name_for_bq
from recidiviz.big_query.constants import BQ_TABLE_COLUMN_NAME_MAX_LENGTH
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig

DEFAULT_READ_CHUNK_SIZE = (
    10 * 1024
)  # 10 KiB should be more than enough to read the first row


@attr.define
class DirectIngestRawFileHeaderReader:
    """Class for reading, normalizing, and validating column headers for a raw ingest file.

    Attributes:
        fs: GCSFileSystem object for reading the file.
        file_config: DirectIngestRawFileConfig object containing the file configuration.
        infer_schema_from_csv: If False, will raise a ValueError if a column found in the raw
            file is not found in the raw file config. Defaults to False.
    """

    fs: GCSFileSystem
    file_config: DirectIngestRawFileConfig
    infer_schema_from_csv: bool = False

    def read_and_validate_column_headers(
        self, gcs_file_path: GcsfsFilePath
    ) -> List[str]:
        """Reads the first row of a GCS CSV file according to the encoding and line terminator configured
        in the raw file config. Validates the column headers according to the column names as they appeared
        in the raw file config at the upload datetime of the file.

        Args:
            gcs_file_path: GcsfsFilePath object representing the path to the file.
        Returns:
            List[str]: the column names as they appeared in the raw file config (normalized for BQ column name
              standards) at the time of file upload, in the order they appear in the file if a header row is present.
        """

        file_upload_datetime = filename_parts_from_path(
            gcs_file_path
        ).utc_upload_datetime

        lowercase_col_name_to_expected_col_name = {
            col.lower(): col
            for col in self.file_config.column_names_at_datetime(file_upload_datetime)
        }

        if not lowercase_col_name_to_expected_col_name:
            raise ValueError(
                f"Found raw file config [{self.file_config.file_tag}] that had no valid "
                f"columns at [{file_upload_datetime.isoformat()}]."
            )

        csv_first_row = self._read_csv_first_row(gcs_file_path)

        if self.file_config.infer_columns_from_config:
            self._validate_csv_no_expected_header_row(
                csv_first_row, lowercase_col_name_to_expected_col_name
            )
            return list(lowercase_col_name_to_expected_col_name.values())

        return self._validate_and_normalize_column_headers(
            csv_first_row,
            lowercase_col_name_to_expected_col_name,
        )

    def _read_csv_first_row(self, gcs_file_path: GcsfsFilePath) -> List[str]:
        """Reads the first row of a GCS CSV file using to the encoding specified in the file config.
        Reads until we encounter the file_config.custom_line_terminator if one is configured,
        otherwise reads until we encounter either '\r' or '\n'.

        Args:
            gcs_file_path: GcsfsFilePath object representing the path to the file.
        Returns:
            List[str]: the first row of the CSV file as a list of strings.
        """
        try:
            with self.fs.open(
                gcs_file_path,
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
                reader = csv.reader(updated_f, **self._get_csv_reader_kwargs())
                csv_first_row = next(reader, [])
        except UnicodeDecodeError as e:
            raise ValueError(
                f"Unable to read path [{gcs_file_path.abs_path()}] for encoding {self.file_config.encoding}."
            ) from e

        if not csv_first_row:
            raise ValueError(
                f"File [{gcs_file_path.abs_path()}] is empty, contains an empty first line, or does not contain valid rows."
            )

        return csv_first_row

    def _validate_and_normalize_column_headers(
        self,
        csv_first_row: List[str],
        lowercase_col_name_to_expected_col_name: dict[str, str],
    ) -> List[str]:
        """
        Validate that all of the columns found in |expected_column_names| are present in
        the raw file and that there are no duplicate column names. If infer_schema_from_csv
        is False, will also validate that all columns found in the file are present in |expected_column_names|.

        Args:
            csv_first_row: The first row of the CSV file.
            expect_column_names: A list of column names as they appeared in the raw file config at the
                upload datetime of the CSV file.
        Returns:
            List[str]: the column names as they appear in the raw file config (normalized for BQ
            column name standards), in the order they appear in the file if a header row is present.

        """

        if len(csv_first_row) > len(lowercase_col_name_to_expected_col_name) * 4:
            raise ValueError(
                "Found at least four times more columns in the first row of the raw file "
                "than we there are columns in the config. This typically is an indication "
                "that the file was sent with the wrong delimiters, line terminators or "
                "encoding which meant we could not properly parse the file."
            )

        header_validation_errors: list[Exception] = []
        normalized_csv_columns = set()
        for i, column_name in enumerate(csv_first_row):
            try:
                normalized_col = normalize_column_name_for_bq(column_name)
            except Exception as e:
                header_validation_errors.append(e)
                continue

            if len(normalized_col) > BQ_TABLE_COLUMN_NAME_MAX_LENGTH:
                raise ValueError(
                    f"Found a column longer than max column length of [{BQ_TABLE_COLUMN_NAME_MAX_LENGTH}]. "
                    f"This typically is an indication that the file was sent with the "
                    f"wrong delimiters, line terminators or encoding which meant we "
                    f"could not properly parse the file."
                )

            caps_normalized_col = lowercase_col_name_to_expected_col_name.get(
                normalized_col.lower()
            )
            if not caps_normalized_col and not self.infer_schema_from_csv:
                header_validation_errors.append(
                    ValueError(
                        f"Column name [{normalized_col}] not found in config for [{self.file_config.file_tag}]."
                    )
                )
                continue

            column_name = (
                caps_normalized_col
                if caps_normalized_col is not None
                else normalized_col
            )
            if column_name in normalized_csv_columns:
                header_validation_errors.append(
                    ValueError(
                        f"Multiple columns with name [{column_name}] after normalization."
                    )
                )

            normalized_csv_columns.add(column_name)
            csv_first_row[i] = column_name

        if (
            difference := set(lowercase_col_name_to_expected_col_name.values())
            - normalized_csv_columns
        ):
            header_validation_errors.append(
                ValueError(
                    f"Columns [{', '.join(difference)}] found in config for [{self.file_config.file_tag}] "
                    f"were not found in the raw data file."
                )
            )

        if header_validation_errors:
            if len(header_validation_errors) == 1:
                raise header_validation_errors[0]

            raise ExceptionGroup(
                "CSV Headers did not match specification in raw file config",
                header_validation_errors,
            )

        return csv_first_row

    def _validate_csv_no_expected_header_row(
        self,
        csv_first_row: List[str],
        lowercase_col_name_to_expected_col_name: Dict[str, str],
    ) -> None:
        """We do not expect the file to contain a header row, so validate that none of the values
        in the first row are found in the expected columns, and validate that the number of columns
        found in the file match the number of expected columns.

        Args:
            csv_first_row: The first row of the CSV file.
            lowercase_col_name_to_expected_col_name: A mapping of lowercase column names to the
              expected column names.
        """

        if len(lowercase_col_name_to_expected_col_name.items()) != len(csv_first_row):
            raise ValueError(
                f"Found {len(lowercase_col_name_to_expected_col_name.items())} columns defined in [{self.file_config.file_tag}] "
                f"but found {len(csv_first_row)} in the CSV. Make sure all expected columns are "
                f"defined in the raw data configuration."
            )

        # Check if any of the values in the first row are found in the file config
        # if so we can assume that this is a header row
        for possible_column_name in csv_first_row:
            if not possible_column_name:
                continue
            try:
                normalized_col = normalize_column_name_for_bq(possible_column_name)
            except ValueError:
                # If we are unable to normalize the column name, we can assume that this is not a header value
                continue

            if normalized_col.lower() in lowercase_col_name_to_expected_col_name:
                raise ValueError(
                    f"Found unexpected header [{normalized_col}] in the CSV. Please remove the header row from the CSV."
                )

    @staticmethod
    def _read_custom_terminated_line(f: IO, line_terminator: str) -> List[str]:
        """Python csv reader is hard-coded to recognize either '\r' or '\n' as end-of-line
        https://docs.python.org/3/library/csv.html#csv.Dialect.lineterminator

        So if there is a custom line terminator manually parse until we encounter it or EOF,
        and replace any newlines with spaces to avoid parsing issues.
        """
        line = ""
        while True:
            char = f.read(1)
            if not char:
                # if we read nothing, we're at the end of the file
                break

            if len(line) >= DEFAULT_READ_CHUNK_SIZE:
                raise ValueError(
                    f"Could not find a line terminator after reading more than "
                    f"[{DEFAULT_READ_CHUNK_SIZE}] characters. This is likely an indication "
                    f"that this file was sent with the wrong line terminators or encoding "
                    f"which meant we could not properly parse the file."
                )

            line += char
            if line.endswith(line_terminator):
                break
        line_without_terminator = line.rstrip(line_terminator)
        # For files with custom terminated lines they may have newlines in unquoted fields
        # so we need to replace them in order to properly parse the csv
        return [line_without_terminator.replace("\n", " ").replace("\r", " ")]

    def _get_csv_reader_kwargs(self) -> Dict[str, Any]:
        return {
            "delimiter": self.file_config.separator,
            "quoting": (
                csv.QUOTE_NONE if self.file_config.ignore_quotes else csv.QUOTE_MINIMAL
            ),
        }
