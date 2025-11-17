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
"""
Functionality to standardize an Excel workbook so that it can be ingested via
Bulk Upload into the Justice Counts database.
"""

import datetime
from io import BytesIO, StringIO
from typing import Any, Dict, Hashable, Iterator, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from thefuzz import fuzz

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import (
    FUZZY_MATCHING_SCORE_CUTOFF,
    MONTH_NAMES,
    NORMALIZERS,
)
from recidiviz.justice_counts.bulk_upload.bulk_upload_metadata import BulkUploadMetadata
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    get_metricfile_by_sheet_name,
)
from recidiviz.justice_counts.types import BulkUploadFileType
from recidiviz.justice_counts.utils.constants import (
    BREAKDOWN,
    BREAKDOWN_CATEGORY,
    MAXIMUM_CSV_FILE_NAME_LENGTH,
)
from recidiviz.justice_counts.utils.system_filename_breakdown_to_metricfile import (
    SYSTEM_METRIC_BREAKDOWN_PAIR_TO_METRICFILE,
)
from recidiviz.persistence.database.schema.justice_counts import schema


class WorkbookStandardizer:
    """Standardizes Excel workbooks to comply with the Justice Counts technical specification."""

    def __init__(self, metadata: BulkUploadMetadata) -> None:
        self.metadata = metadata
        self.invalid_sheet_names: Set[str] = set()
        self.is_csv_upload = False
        self.canonical_file_name_to_metric_key = {
            m.canonical_filename: m.definition.key for m in self.metadata.metric_files
        }
        self.curr_expected_columns: List[str] = []
        self.sheet_name_to_standardized_column_headers: Dict[
            str, Optional[List[str]]
        ] = {}

    def _get_datapoint_chunks_for_excel_file(
        self, file: Any
    ) -> Iterator[Tuple[str, pd.DataFrame]]:
        """
        Converts an excel file to an iterator that yields (sheet_name, dataframe) tuples.

        Args:
            file (Any): The input Excel file.

        Yields:
            Iterator[Tuple[str, pd.DataFrame]]: An iterator of (sheet_name, DataFrame dataframe).
        """
        # Pandas 2.0 requires bytes to be wrapped in BytesIO for read_excel
        if isinstance(file, bytes):
            file = BytesIO(file)

        excel_file = pd.ExcelFile(file)

        # Sort sheet names alphabetically to ensure consistent processing order
        sorted_sheets = sorted(excel_file.sheet_names)

        for sheet_name in sorted_sheets:
            chunk_start = 0
            column_headers = None  # Store column headers from the first chunk

            while True:
                chunk = pd.read_excel(
                    file,
                    sheet_name=sheet_name,
                    skiprows=chunk_start,
                    nrows=self.metadata.chunk_size,
                    header=(
                        0 if column_headers is None else None
                    ),  # Read headers only once
                )

                if chunk.empty:
                    break  # Stop processing when there is no more data

                if column_headers is None:
                    # Save the column headers from the first chunk
                    column_headers = chunk.columns
                else:
                    # Ensure subsequent chunks align with the original column headers
                    chunk.columns = column_headers

                yield sheet_name, chunk

                if chunk_start == 0:
                    # Account for the header row when determining the next starting row
                    chunk_start += 1

                # Move to the next chunk
                chunk_start += self.metadata.chunk_size

    def _get_datapoint_chunks_for_csv_file(
        self, file: Any, file_name: str
    ) -> Iterator[Tuple[str, pd.DataFrame]]:
        """
        Converts a CSV file to an iterator that yields (sheet_name, dataframe) tuples.

        Args:
            file (Any): The input CSV file.
            file_name (str): The name of the CSV file.

        Yields:
            Iterator[Tuple[str, pd.DataFrame]]: An iterator of (sheet_name, DataFrame dataframe).
        """
        # Extract a meaningful sheet name from the file name and ensure it’s within Excel's 31-char limit
        sheet_name = file_name.rsplit(".", 1)[0][:MAXIMUM_CSV_FILE_NAME_LENGTH]

        chunks = pd.read_csv(file, chunksize=self.metadata.chunk_size)
        column_headers = None  # Store headers from the first chunk

        for chunk in chunks:
            if column_headers is None:
                column_headers = chunk.columns  # Store headers from the first chunk
            else:
                chunk.columns = column_headers  # Ensure consistency across chunks

            yield sheet_name, chunk

    def get_dataframe_chunks(
        self, file: Any, file_name: str
    ) -> Iterator[Tuple[str, pd.DataFrame]]:
        """
        Converts a file to an iterator that yields (sheet_name, dataframe) tuples.

        Args:
            file (Any): The input file.
            file_name (str): The name of the file.

        Yields:
            Iterator[Tuple[str, pd.DataFrame]]: An iterator of (sheet_name, DataFrame dataframe).
        """
        file_type = self.get_file_type_from_file_name(file_name=file_name)

        if file_type != BulkUploadFileType.CSV:
            return self._get_datapoint_chunks_for_excel_file(file=file)

        self.is_csv_upload = True
        if isinstance(file, bytes):
            s = str(file, "utf-8")
            file = StringIO(s)
        else:
            file.stream.seek(0)

        return self._get_datapoint_chunks_for_csv_file(file=file, file_name=file_name)

    def get_file_type_from_file_name(self, file_name: str) -> BulkUploadFileType:
        """
        Determines the file type based on the file name suffix.

        Args:
            file_name (str): The name of the file.

        Returns:
            BulkUploadFileType: The type of the file.
        """
        return BulkUploadFileType.from_suffix(file_name.rsplit(".", 1)[1].lower())

    def _standardize_string(
        self, value: Any, add_underscore: Optional[bool] = True
    ) -> Any:
        """
        Standardizes the given string by:
        1) Removing leading and trailing whitespace.
        2) Converting all text to lower case.
        3) Replacing spaces with underscores (optional for specific cases).

        Parameters:
        value (str): The string to be standardized.

        Returns:
        str: The standardized string
        """
        if not isinstance(value, str):
            return value
        value = value.strip().lower()
        if add_underscore is True:
            value = value.replace(" ", "_")
        return value

    def _add_unexpected_column_errors(
        self,
        df: pd.DataFrame,
        metric_key: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Set[BulkUploadMessageType]:
        """
        Detects unexpected columns in a DataFrame and logs corresponding errors or warnings.

        This method verifies whether each column in the provided DataFrame is among the expected columns. If an
        unexpected column is found, an error message is recorded. However, if the column is named 'month' and
        contains data, a warning is issued instead, indicating that the data will be interpreted as having a
        monthly reporting frequency.

        Errors and warnings are added to the `metric_key_to_errors` mapping in the metadata,
        associating them with the relevant `metric_key`.

        Args:
            df (pd.DataFrame):
                The DataFrame to be checked for unexpected columns.
            metric_key (Optional[str], optional):
                The key identifying the metric being processed. Used for categorizing errors.
            sheet_name (Optional[str], optional):
                The name of the sheet being processed. Included in error messages for context.

        Returns:
            Set[BulkUploadMessageType]:
                A set of message types (`ERROR` or `WARNING`) generated during processing.
        """

        message_types = set()

        for column in df.columns:
            if column == "row_number":
                continue

            message_type = BulkUploadMessageType.ERROR
            description = f"A '{column}' column was found in your sheet. Only the following columns were expected in your sheet: {', '.join(self.curr_expected_columns)}. "
            standardized_column_header = self._standardize_string(
                column, add_underscore=True
            )
            if standardized_column_header not in self.curr_expected_columns:
                if standardized_column_header == "month":
                    if bool(df[column].isna().all()) is False:
                        # If there is a month column with data in it, update the description to say
                        # that the data will be recorded as monthly.
                        description += "Since you provided month data, your data will be saved for a monthly reporting frequency"
                    message_type = BulkUploadMessageType.WARNING

                message_types.add(message_type)
                self.metadata.metric_key_to_errors[metric_key].append(
                    JusticeCountsBulkUploadException(
                        title=f"Unexpected '{column}' Column",
                        description=description,
                        message_type=message_type,
                        sheet_name=sheet_name,
                    )
                )
        return message_types

    def _add_missing_column_errors(
        self,
        df: pd.DataFrame,
        metric_key: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Set[BulkUploadMessageType]:
        """
        Checks for missing or empty expected columns in a DataFrame and adds corresponding error or warning messages.

        Args:
            df : pd.DataFrame
                The DataFrame representing the sheet to be checked for missing or empty columns.
            metric_key : Optional[str], optional
                The key corresponding to the metric being processed. This is used to categorize errors.
            sheet_name : Optional[str], optional
                The name of the sheet being processed. This is included in error messages for context.

        Returns:
            Set[BulkUploadMessageType]
                A set of message types (`ERROR` or `WARNING`) generated during the processing of missing or empty columns.
        """

        message_types = set()
        standardized_column_actual_column = {
            self._standardize_string(col): col for col in df.columns
        }

        for column in self.curr_expected_columns:
            message_type = BulkUploadMessageType.ERROR
            title = ""
            description = ""

            if column not in standardized_column_actual_column.keys():
                title = f"Missing '{column}' Column"
                description = (
                    f"This sheet should contain a column named '{column}' populated with data. "
                    f"Only the following columns were found in the sheet: "
                    f"{', '.join(df.columns)}. "
                )
            elif (
                bool(df[standardized_column_actual_column[column]].isna().all()) is True
            ):
                title = f"Empty '{column}' Column"
                description = (
                    f"No data was provided in the '{column}' column in your sheet."
                    if metric_key is None
                    else f"No data was provided in the '{column}' column for this metric. "
                )
            else:
                continue

            if column == "month":
                description += "Since the 'month' column is missing, your data will be recorded with an annual reporting frequency. The 'month' column should include integers ranging from 1 to 12."
                message_type = BulkUploadMessageType.WARNING

            message_types.add(message_type)
            self.metadata.metric_key_to_errors[metric_key].append(
                JusticeCountsBulkUploadException(
                    title=title,
                    description=description,
                    message_type=message_type,
                    sheet_name=sheet_name,
                )
            )
        return message_types

    def should_sheet_have_month_column(
        self,
        metric_keys: List[str],
    ) -> bool:
        """
        Determines if a DataFrame sheet should include a 'month' column based on the reporting frequency.

        Args:
            metric_key : Optional[str], optional
                The key corresponding to the metric being processed. If provided, the method checks the reporting
                frequency of this specific metric.

        Returns:
            bool
                True if the sheet should include a 'month' column, False otherwise.
        """
        reporting_frequencies = set()

        for metric_key in metric_keys:
            if (
                len(self.metadata.child_agency_name_to_agency) > 0
                and self.metadata.system != schema.System.SUPERAGENCY
            ):
                # There should be a month column if the agency is a super agency and either
                # the super agency of the child agencies have the metric configured with a
                # monthly reporting
                reporting_frequencies.update(
                    {
                        metric_key_to_metric_interface[
                            metric_key
                        ].get_reporting_frequency_to_use()[0]
                        for metric_key_to_metric_interface in self.metadata.child_agency_id_to_metric_key_to_metric_interface.values()
                        if metric_key in metric_key_to_metric_interface
                    }
                )
            else:
                # There should be a month column if it a regular agency with standard
                # upload format and the metric is reported monthly.
                return (
                    self.metadata.metric_key_to_metric_interface[
                        metric_key
                    ].get_reporting_frequency_to_use()[0]
                    == schema.ReportingFrequency.MONTHLY
                )
        return schema.ReportingFrequency.MONTHLY in reporting_frequencies

    def get_expected_columns(
        self, df: pd.DataFrame, metric_file: Optional[MetricFile]
    ) -> List[str]:
        """
        Determines the expected column names for a given DataFrame.

        This method generates a list of expected column names based on the upload structure,
        the upload type (single-page or multi-page), the metric file definition, and the agency's
        reporting requirements.

        Args:
            df (pd.DataFrame):
                The DataFrame representing the sheet to be processed.
            metric_file (Optional[MetricFile]):
                The metric file associated with the sheet. If provided, its definition helps
                determine the expected columns.

        Returns:
            List[str]:
                A list of expected column names for the given sheet.
        """
        if metric_file is not None:
            metric_keys = [metric_file.definition.key]
        else:
            metric_keys = (
                [
                    key
                    for m in df["metric"].unique()
                    for key in [self.canonical_file_name_to_metric_key.get(m)]
                    if key is not None
                ]
                if "metric" in df.columns
                else []
            )

        expected_columns = (
            ["year"]
            if self.metadata.is_single_page_upload is False
            else ["metric", "year"]
        )

        if self.should_sheet_have_month_column(metric_keys=metric_keys) is True:
            expected_columns.append("month")

        if (
            len(self.metadata.child_agency_name_to_agency) > 0
            and self.metadata.system != schema.System.SUPERAGENCY
        ):
            expected_columns.append("agency")

        if (
            self.metadata.system == schema.System.SUPERVISION
            and AgencyInterface.does_supervision_agency_report_for_subsystems(
                agency=self.metadata.agency
            )
            is True
        ):
            disaggregated_metric_by_supervision_subsystem = (
                self._get_disaggregated_metric_by_supervision_subsystem_set(
                    metric_keys=metric_keys
                )
            )
            if True in disaggregated_metric_by_supervision_subsystem:
                expected_columns.append("system")

        if (
            self.metadata.is_single_page_upload is False
            and metric_file is not None
            and metric_file.disaggregation_column_name is not None
        ):
            expected_columns.append(metric_file.disaggregation_column_name)

        if self.metadata.is_single_page_upload is True:
            # If there is a 'breakdown column' we expect a 'breakdown_category' column (and vice versa).
            if BREAKDOWN_CATEGORY in df.columns or BREAKDOWN in df.columns:
                expected_columns += [BREAKDOWN_CATEGORY, BREAKDOWN]

        expected_columns.append("value")
        return expected_columns

    def _get_disaggregated_metric_by_supervision_subsystem_set(
        self, metric_keys: List[str]
    ) -> Set[Optional[bool]]:
        """
        Generates a set of disaggregated_by_supervision_subsystems values for a metric.

        Args:
            metric_key : Optional[str]
                The metric key for the metric we are evaluating

        Returns:
            Set[Optional[bool]]
                A set of disaggregated_metric_by_supervision_subsystem for the metric.
                If metric_key is None, we will add the disaggregated_metric_by_supervision_subsystem for
                all metrics to the set. If metric_key is not None, the set will only contain
                the disaggregated_by_supervision_subsystems for the specified metric. metric_key will
                be None for single-page format uploads.
        """
        return {
            self.metadata.metric_key_to_metric_interface[
                metric_key
            ].disaggregated_by_supervision_subsystems
            for metric_key in metric_keys
        }

    def _add_column_header_errors(
        self,
        df: pd.DataFrame,
        sheet_name: Optional[str] = None,
        metric_key: Optional[str] = None,
    ) -> bool:
        """
        Validates column headers in a DataFrame by identifying missing or unexpected columns.

        Args:
            df (pd.DataFrame):
                The DataFrame to validate.
            sheet_name (Optional[str], optional):
                The name of the sheet being processed, used for error reporting.
            metric_key (Optional[str], optional):
                The key corresponding to the metric being processed, used to categorize errors.

        Returns:
            bool:
                `True` if no errors were found, otherwise `False`.
        """

        missing_column_error_messages = self._add_missing_column_errors(
            metric_key=metric_key,
            df=df,
            sheet_name=sheet_name,
        )

        unexpected_column_error_messages = self._add_unexpected_column_errors(
            metric_key=metric_key,
            df=df,
            sheet_name=sheet_name,
        )

        error_messages = missing_column_error_messages.union(
            unexpected_column_error_messages
        )

        return BulkUploadMessageType.ERROR not in error_messages

    def _add_row_value_error(  # pylint: disable=too-many-positional-arguments
        self,
        column_name: str,
        value: Any,
        additional_description: str = "",
        row_number: Optional[int] = None,
        metric_key: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> None:
        """
        Adds an error message for a specific row in the sheet when a value in a column is either
        missing or invalid. This error is recorded in the metadata for the current metric.

        Args:
            column_name (str): The name of the column that has the error.
            row_number (int): The number of the row where the error occurred.
            metric_key (str): The metric key to which the error corresponds.
            sheet_name (Optional[str]): The name of the sheet where the error occurred (optional).
            value (Optional[Any]): The value in the cell that triggered the error (optional).
                                If None, it represents an empty cell.

        Returns:
            None
        """
        description = f"Row {row_number}: " if row_number is not None else ""

        if value is None:  # representing an empty cell
            title = f"Empty '{column_name}' Column Value"
            description += f"The '{column_name}' column should contain a value. "
        else:
            title = f"Invalid '{column_name}' Column Value"
            description += f"The '{column_name}' column has an invalid value: {value}. "

        self.metadata.metric_key_to_errors[metric_key].append(
            JusticeCountsBulkUploadException(
                title=title,
                description=description + additional_description,
                message_type=BulkUploadMessageType.ERROR,
                sheet_name=sheet_name,
            )
        )

    def _get_year_from_row(
        self, row: Dict[Hashable, Any], today: datetime.date
    ) -> Optional[int]:
        """
        Extracts and validates the year from a row dictionary.

        This function attempts to extract a year from the provided row. If the year is in
        a fiscal year format (e.g., '2023-2024'), the first year is extracted. Otherwise,
        it tries to convert the "year" value to an integer and ensures the year is valid
        (between 1600 and the current year).

        Args:
            row (Dict[Hashable, Any]): A dictionary representing a row, with a "year" key.

        Returns:
            Optional[int]: The valid year as an integer if it falls between 1600 and the
            current year, or None if the year is invalid or cannot be parsed.
        """

        if row.get("year") is None:
            return None

        if "-" in str(row["year"]) and str(row["year"]).index("-") != 0:
            # Get Annual year from the Fiscal Year
            return int(row["year"][0 : str(row["year"]).index("-")])

        try:
            year_value = int(row["year"])  # Try to convert "year" to an int
            if 1600 <= year_value <= today.year:
                return year_value
            return None
        except ValueError:
            return None

    def standardize_rows(
        self,
        df: pd.DataFrame,
        metric_file: Optional[MetricFile],
        sheet_name: Optional[str],
    ) -> List[Dict[str, Any]]:
        """
        Standardizes and validates rows from an Excel sheet DataFrame.

        Rows that pass validation are returned as standardized dictionaries, while those with errors
        are omitted and logged.

        Args:
            df (pd.DataFrame):
                The DataFrame containing the raw data to be standardized.
            metric_file (Optional[MetricFile]):
                The metric file associated with the sheet, used for determining expected fields.
            sheet_name (Optional[str]):
                The name of the sheet being processed, included in error messages for context.

        Returns:
            List[Dict[str, Any]]:
                A list of validated and standardized rows, formatted as dictionaries.
        """
        # Replace Nan with None
        sheet_df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})
        rows = sheet_df.to_dict("records")

        valid_rows = []
        today = datetime.date.today()
        for row in rows:
            if self.metadata.is_single_page_upload is True:
                metric_file = SYSTEM_METRIC_BREAKDOWN_PAIR_TO_METRICFILE.get(
                    (
                        self.metadata.system,
                        row["metric"],
                        self._standardize_string(row.get(BREAKDOWN_CATEGORY)),
                    )
                )
            metric_key = (
                metric_file.definition.key
                if metric_file is not None
                else self.canonical_file_name_to_metric_key.get(row["metric"])
            )
            row_number = row["row_number"]
            # Check for invalid 'year' values
            num_errors_before = len(
                self.metadata.metric_key_to_errors.get(metric_key, [])
            )

            year = self._get_year_from_row(row=row, today=today)
            row["year"] = year
            if year is None:
                self._add_row_value_error(
                    column_name="year",
                    value=row["year"],
                    metric_key=metric_key,
                    sheet_name=sheet_name,
                    row_number=row_number,
                    additional_description=f"The year column should only contain integer values between 1600 and {today.year}.",
                )

            # Check for invalid 'month' values
            if row.get("month") is not None:
                additional_description = "The month column should have either numbers between 1 and 12 or the name of the month (i.e January)."
                try:
                    month_value = int(row["month"])  # Try to convert "month" to an int
                    if month_value < 1 or month_value > 12:
                        self._add_row_value_error(
                            column_name="month",
                            value=row["month"],
                            metric_key=metric_key,
                            sheet_name=sheet_name,
                            row_number=row_number,
                            additional_description=additional_description,
                        )
                except ValueError:
                    if isinstance(row["month"], str):
                        column_value = self.fuzzy_match_against_options(
                            column_name="month",
                            text=row["month"],
                            options=MONTH_NAMES[
                                1:
                            ],  # start at 1 because the first element is an empty string,
                            row_number=row_number,
                            metric_key=metric_key,
                            sheet_name=sheet_name,
                            additional_description=additional_description,
                        )
                        if column_value is not None:
                            row["month"] = MONTH_NAMES.index(column_value)
                    else:
                        self._add_row_value_error(
                            column_name="month",
                            value=row["month"],
                            metric_key=metric_key,
                            sheet_name=sheet_name,
                            row_number=row_number,
                            additional_description="Data from this row will be saved with an Annual reporting frequency.",
                        )

            # Check for invalid 'system' values
            if (
                "system" in self.curr_expected_columns
                and metric_key is not None
                and self.metadata.metric_key_to_metric_interface[
                    metric_key
                ].disaggregated_by_supervision_subsystems
                is True
                and row.get("system") not in self.metadata.agency.systems
            ):

                systems_string = ", ".join(self.metadata.agency.systems)
                if row.get("system") is None:
                    message = (
                        "You must provide one of the following sectors for each row in this sheet. "
                        + systems_string
                    )
                else:
                    message = (
                        f"Your agency does not report for the {row.get('system')} sector. Each rows should contain one of the following values: "
                        + systems_string
                    )
                self._add_row_value_error(
                    column_name="system",
                    value=row.get("system"),
                    metric_key=metric_key,
                    sheet_name=sheet_name,
                    row_number=row_number,
                    additional_description=message,
                )

            # Check for invalid 'agency' values
            if "agency" in self.curr_expected_columns:
                original_agency_name = row["agency"]
                row["agency"] = self._standardize_string(
                    original_agency_name, add_underscore=False
                )

                agency = self.metadata.child_agency_name_to_agency.get(row["agency"])
                if agency is None:
                    self._add_row_value_error(
                        column_name="agency",
                        value=original_agency_name,
                        metric_key=metric_key,
                        sheet_name=sheet_name,
                        row_number=row_number,
                        additional_description=(
                            ""
                            if not row.get("agency") is not None
                            else f"Your agency does not have permissions to report for {original_agency_name}."
                        ),
                    )

            if not isinstance(row.get("value"), int) or not isinstance(
                row.get("value"), float
            ):
                try:
                    value = float(row["value"])
                    if value >= 0:
                        row["value"] = value
                    else:
                        raise ValueError
                except (TypeError, ValueError):
                    self._add_row_value_error(
                        column_name="value",
                        value=row.get("value"),
                        metric_key=metric_key,
                        sheet_name=sheet_name,
                        row_number=row_number,
                        additional_description="",
                    )

            # Check for invalid 'metric' values
            if (
                self.metadata.is_single_page_upload is True
                and row.get("metric") is None
                or (
                    row.get("metric") is not None
                    and self.canonical_file_name_to_metric_key.get(row["metric"])
                    is None
                )
            ):
                valid_metrics = ", ".join(self.canonical_file_name_to_metric_key)
                self._add_row_value_error(
                    column_name="metric",
                    value=row.get("metric"),
                    metric_key=metric_key,
                    sheet_name=sheet_name,
                    row_number=row_number,
                    additional_description=f"The metric in this row does not correspond to a metric for this agency. The valid values for this row are {valid_metrics}.",
                )
                # If the metric is invalid, don't bother checking breakdown / breakdown category
                continue

            # Check for invalid disaggregation values
            if metric_file is None:
                possible_disaggregation_categories_for_metric = ", ".join(
                    [
                        m.disaggregation_column_name
                        for m in self.metadata.metric_files
                        if m.definition.key == metric_key
                        and m.disaggregation_column_name is not None
                    ]
                )
                # Add error if breakdown category does not correspond to the metric.
                self._add_row_value_error(
                    column_name=BREAKDOWN_CATEGORY,
                    value=row[BREAKDOWN_CATEGORY],
                    row_number=row_number,
                    metric_key=metric_key,
                    sheet_name=sheet_name,
                    additional_description=f"The breakdown category does not correspond to the metric. Possible breakdown categories include: {possible_disaggregation_categories_for_metric}.",
                )

            if (
                metric_file is not None
                and metric_file.disaggregation_column_name is not None
            ):
                text = (
                    row.get(metric_file.disaggregation_column_name)
                    if self.metadata.is_single_page_upload is False
                    else row.get(BREAKDOWN)
                )
                column_name = (
                    metric_file.disaggregation_column_name
                    if self.metadata.is_single_page_upload is False
                    else BREAKDOWN
                )
                options = [d.value for d in list(metric_file.disaggregation)]  # type: ignore[call-overload]
                disaggregation_value = self.fuzzy_match_against_options(
                    text=text,
                    column_name=column_name,
                    options=options,
                    sheet_name=sheet_name,
                    row_number=row_number,
                    metric_key=metric_key,
                )
                if disaggregation_value is not None:
                    row[column_name] = disaggregation_value
                else:
                    # Add error if breakdown does not correspond to the metric.
                    options_string = ", ".join(options)
                    self._add_row_value_error(
                        column_name=column_name,
                        value=text,
                        row_number=row_number,
                        metric_key=metric_key,
                        sheet_name=sheet_name,
                        additional_description=f"The breakdown does not correspond to the metric. Possible breakdowns include: {options_string}.",
                    )

            num_errors_after = len(
                self.metadata.metric_key_to_errors.get(metric_key, [])
            )
            if num_errors_before == num_errors_after:
                valid_rows.append(row)

        return valid_rows

    def standardize_dataframe(
        self, df: pd.DataFrame, sheet_name: str, current_index: int
    ) -> List[Dict[str, Any]]:
        """
            Standardizes a DataFrame by ensuring expected columns are present,
            unexpected columns are flagged, and row numbers are correctly assigned.

            If the sheet contains errors that prevent ingestion, it will not be processed further.
        Otherwise, the function returns the standardized rows.

        Args:
        df (pd.DataFrame):
            The DataFrame representing the rows to be standardized.
        sheet_name (str):
            The name of the sheet being standardized, included in error messages for context.
        current_index (int):
            The starting index used to calculate row numbers for tracking.

        Returns:
        Tuple[bool, List[Dict[str, Any]]]:
            A tuple where:
            - The first element is a boolean indicating whether the sheet can be ingested (True if no errors, False otherwise).
            - The second element is a list of standardized row dictionaries if ingestion is successful, otherwise an empty list.
        """

        standardized_sheet_name = self._standardize_string(sheet_name)

        if "row_number" not in df.columns:
            # Insert a 'row_number' column to capture the original row numbers from the sheet.
            # This is necessary because, for single-page uploads, the file will be reformatted into
            # the standard format, and row numbers will change during that process. In the standard
            # format, the row numbers will align with the actual data rows.
            # We add 2 to the index because:
            # 1) Row numbering starts at 1 (not 0), and
            # 2) The first row in the DataFrame represents the column headers.
            df.insert(0, "row_number", df.index + current_index + 2)

        # Convert all column names to strings
        df.columns = df.columns.astype(str)
        # Remove unnamed columns
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

        metric_file = get_metricfile_by_sheet_name(
            sheet_name=standardized_sheet_name, system=self.metadata.system
        )

        if metric_file is None and self.metadata.is_single_page_upload is False:
            # 1) Don't write sheet with invalid sheet name to new excel object
            # 2) Add sheet name to invalid sheet names
            self.invalid_sheet_names.add(sheet_name)
            return []

        if (
            sheet_name not in self.sheet_name_to_standardized_column_headers
        ):  # Only perform this check once per sheet because we want to log errors once
            # and avoid redundant computations.
            self.curr_expected_columns = self.get_expected_columns(
                df=df, metric_file=metric_file
            )

            can_ingest_sheet = self._add_column_header_errors(
                df=df,
                sheet_name=sheet_name,
                metric_key=(
                    metric_file.definition.key if metric_file is not None else None
                ),
            )

            df.columns = df.columns.map(self._standardize_string)

            self.sheet_name_to_standardized_column_headers[sheet_name] = (
                df.columns if can_ingest_sheet is True else None
            )
        else:
            can_ingest_sheet = (
                self.sheet_name_to_standardized_column_headers[sheet_name] is not None
            )

        if can_ingest_sheet is False:
            return []

        df.columns = self.sheet_name_to_standardized_column_headers[sheet_name]

        return self.standardize_rows(
            df=df,
            sheet_name=sheet_name,
            metric_file=metric_file,
        )

    def add_invalid_name_error(self) -> None:
        """
        Adds an error to the list of errors indicating that the provided CSV file or sheet name is invalid.

        Parameters:
        None

        Returns:
        None

        The method handles three types of errors:
        1. CSV File Name Error: Occurs when the provided `sheet_name` does not match any of the expected
        metric file names for the agency during a CSV upload. CSVs are processed by converting them into an
        excel workbook with one sheet, the sheet name being the name of the file.
        2. Invalid Sheet Name Error: Occurs when the provided `sheet_name` does not correspond to any
        known metric sheet names for an Excel Workbook upload.
        """

        if len(self.invalid_sheet_names) == 0:
            return

        valid_file_names = ", ".join(
            [
                metric_file.canonical_filename
                for metric_file in self.metadata.metric_files
            ]
        )
        if self.is_csv_upload:
            csv_error_description = (
                f"The file name '{self.invalid_sheet_names.pop()}' does not correspond to a metric for your agency. "
                f"For CSV uploads, the file name should exactly match one of the following options: {valid_file_names}."
            )
            self.metadata.metric_key_to_errors[None].append(
                # The None key in metric_key_to_errors is designated for non-metric
                # errors.These are errors that cannot be attributed to a particular
                # metric, such as this one, where we are not able to tell what metric
                # the user is attempting to upload for.
                JusticeCountsBulkUploadException(
                    title="Invalid File Name for CSV",
                    message_type=BulkUploadMessageType.ERROR,
                    description=csv_error_description,
                )
            )
            return

        invalid_sheet_name_error = (
            f"The following sheet names do not correspond to a metric for your agency: "
            f"{', '.join(self.invalid_sheet_names)}. "
            f"Valid options include {valid_file_names}."
        )

        self.metadata.metric_key_to_errors[None].append(
            JusticeCountsBulkUploadException(
                title="Invalid Sheet Name",
                message_type=BulkUploadMessageType.ERROR,
                description=invalid_sheet_name_error,
            ),
        )
        return

    def fuzzy_match_against_options(  # pylint: disable=too-many-positional-arguments
        self,
        text: Any,
        column_name: str,
        options: List[str],
        sheet_name: Optional[str] = None,
        row_number: Optional[int] = None,
        metric_key: Optional[str] = None,
        additional_description: Optional[str] = None,
    ) -> Optional[str]:
        """Given a piece of input text and a list of options, uses
        fuzzy matching to calculate a match score between the input
        text and each option. Returns the option with the highest
        score, as long as the score is above a cutoff.
        """
        option_to_score = {
            option: fuzz.ratio(  # type: ignore[attr-defined]
                self.metadata.text_analyzer.normalize_text(
                    str(text), stem_tokens=True, normalizers=NORMALIZERS
                ),
                self.metadata.text_analyzer.normalize_text(
                    option, stem_tokens=True, normalizers=NORMALIZERS
                ),
            )
            for option in options
        }

        best_option = max(option_to_score, key=option_to_score.get)  # type: ignore[arg-type]
        if option_to_score[best_option] < FUZZY_MATCHING_SCORE_CUTOFF:
            options_string = ", ".join(options)
            self._add_row_value_error(
                column_name=column_name,
                value=text,
                metric_key=metric_key,
                sheet_name=sheet_name,
                row_number=row_number,
                additional_description=(
                    f"Valid options for this row are: {options_string}."
                    if additional_description is None
                    else additional_description
                ),
            )
            return None

        return best_option
