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
from io import StringIO
from typing import Any, Dict, Hashable, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from thefuzz import fuzz

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import (
    FUZZY_MATCHING_SCORE_CUTOFF,
    MONTH_NAMES,
    NORMALIZERS,
    separate_file_name_from_folder,
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

    def get_new_file_name_and_sheet_name(self, file_name: str) -> Tuple[str, str]:
        """
        Generates a new file name and corresponding sheet name for a converted Excel file.

        Args:
            file_name (str): The original file name.

        Returns:
            Tuple[str, str]: The new file name and the sheet name.
        """
        new_file_name = separate_file_name_from_folder(file_name=file_name).rsplit(".", 1)[0] + ".xlsx"  # type: ignore[union-attr]
        sheet_name = new_file_name.rsplit(".", 1)[0].split("/")[-1]
        return new_file_name, sheet_name

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

    def _convert_file_to_pandas_excel_file(
        self, file: Any, file_name: str
    ) -> pd.ExcelFile:
        """
        Converts a file to an Excel file if necessary, and validates the file name.

        Args:
            file (Any): The input file.
            file name (str): The name of the file.

        Returns:
            Tuple[pd.ExcelFile, str]: A tuple containing the Excel file and
            the new file name
        """
        # Determine the file type from the file name suffix
        file_type = self.get_file_type_from_file_name(file_name=file_name)

        if file_type != BulkUploadFileType.CSV:
            # If file is already in Excel format, return it
            return pd.ExcelFile(file)

        # Create new file name for the converted Excel file
        new_file_name, sheet_name = self.get_new_file_name_and_sheet_name(
            file_name=file_name
        )

        # Convert bytes to string if necessary
        self.is_csv_upload = True
        if isinstance(file, bytes):
            s = str(file, "utf-8")
            file = StringIO(s)
        else:
            file.stream.seek(0)

        # Read CSV file and convert it to Excel
        csv_df = pd.read_csv(file)
        if len(new_file_name) > MAXIMUM_CSV_FILE_NAME_LENGTH:
            # csv_df.to_excel will throw an error if the file name is > 31 characters
            new_file_name = (
                new_file_name[0 : MAXIMUM_CSV_FILE_NAME_LENGTH - len(".xlsx")] + ".xlsx"
            )
            sheet_name = sheet_name[0:MAXIMUM_CSV_FILE_NAME_LENGTH]

        csv_df.to_excel(new_file_name, sheet_name=sheet_name, index=False)
        return pd.ExcelFile(new_file_name)

    def _add_unexpected_column_errors(
        self,
        expected_columns: List[str],
        sheet_df: pd.DataFrame,
        metric_key: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Set[BulkUploadMessageType]:
        """
        Identifies unexpected columns in a DataFrame and adds corresponding error or warning messages.

        This method checks each column in the provided `sheet_df` DataFrame to determine if it matches any of the
        `expected_columns`. If a column is found that is not expected, an error message is created and added to
        the `metric_key_to_errors` mapping in the metadata. If the column is a 'month' column and contains data,
        a warning message is generated instead, indicating that the data will be recorded as having a monthly
        reporting frequency.

        Args:
            expected_columns : List[str]
                A list of expected column names for the sheet.
            sheet_df : pd.DataFrame
                The DataFrame representing the sheet to be checked for unexpected columns.
            metric_key : Optional[str], optional
                The key corresponding to the metric being processed. This is used to categorize errors.
            sheet_name : Optional[str], optional
                The name of the sheet being processed. This is included in error messages for context.

        Returns:
            Set[BulkUploadMessageType]
                A set of message types (`ERROR` or `WARNING`) generated during the processing of unexpected columns.
        """

        message_types = set()

        for column in sheet_df.columns:
            if column == "row_number":
                continue

            message_type = BulkUploadMessageType.ERROR
            description = f"A '{column}' column was found in your sheet. Only the following columns were expected in your sheet: {', '.join(expected_columns)}. "
            standardized_column_header = self._standardize_string(
                column, add_underscore=True
            )
            if standardized_column_header not in expected_columns:
                if standardized_column_header == "month":
                    if bool(sheet_df[column].isna().all()) is False:
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
        expected_columns: List[str],
        sheet_df: pd.DataFrame,
        metric_key: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> Set[BulkUploadMessageType]:
        """
        Checks for missing or empty expected columns in a DataFrame and adds corresponding error or warning messages.

        Args:
            expected_columns : List[str]
                A list of expected column names that should be present in the sheet.
            sheet_df : pd.DataFrame
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
            self._standardize_string(col): col for col in sheet_df.columns
        }

        for column in expected_columns:
            message_type = BulkUploadMessageType.ERROR
            title = ""
            description = ""

            if column not in standardized_column_actual_column.keys():
                title = f"Missing '{column}' Column"
                description = (
                    f"This sheet should contain a column named '{column}' populated with data. "
                    f"Only the following columns were found in the sheet: "
                    f"{', '.join(sheet_df.columns)}. "
                )
            elif (
                bool(sheet_df[standardized_column_actual_column[column]].isna().all())
                is True
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
                # monthly reporting frequency
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
        self,
        sheet_df: pd.DataFrame,
        metric_file: Optional[MetricFile] = None,
    ) -> List[str]:
        """
        Generates a list of expected column names for a given DataFrame sheet.

        Args:
            sheet_df : pd.DataFrame
                The DataFrame representing the sheet to be checked.
            metric_file : Optional[MetricFile], optional
                An optional `MetricFile` object that contains the metric definition and disaggregation details.

        Returns:
            List[str]
                A list of expected column names for the sheet.
        """
        if metric_file is not None:
            metric_keys = [metric_file.definition.key]
        else:
            metric_keys = (
                [
                    key
                    for m in sheet_df["metric"].unique()
                    for key in [self.canonical_file_name_to_metric_key.get(m)]
                    if key is not None
                ]
                if "metric" in sheet_df.columns
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
            if BREAKDOWN_CATEGORY in sheet_df.columns or BREAKDOWN in sheet_df.columns:
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

    def standardize_column_headers(
        self,
        sheet_df: pd.DataFrame,
        expected_columns: List[str],
        sheet_name: Optional[str] = None,
        metric_key: Optional[str] = None,
    ) -> bool:

        missing_column_error_messages = self._add_missing_column_errors(
            expected_columns=expected_columns,
            metric_key=metric_key,
            sheet_df=sheet_df,
            sheet_name=sheet_name,
        )

        unexpected_column_error_messages = self._add_unexpected_column_errors(
            expected_columns=expected_columns,
            metric_key=metric_key,
            sheet_df=sheet_df,
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
        sheet_df: pd.DataFrame,
        metric_file: Optional[MetricFile],
        sheet_name: Optional[str],
        expected_columns: List[str],
    ) -> pd.DataFrame:
        """
        Standardize and validate rows from an Excel sheet DataFrame.

        This method processes each row in the provided DataFrame, validates specific fields
        ('year', 'month', 'system', 'agency', '<breakdown column name>' and 'value'), and corrects
        or reports errors based on the Justice Counts technical specification. Rows with valid
        data are returned, while rows containing invalid values are dropped and logged as
        errors or warnings.

        Parameters
        ----------
        sheet_df : pd.DataFrame
            The DataFrame containing rows to be standardized.
        sheet_name : Optional[str]
            The name of the sheet being processed, used for error reporting.
        expected_columns : List[str]
            A list of expected column names in the DataFrame, including 'system' and 'agency' for validation.
        metric_file : Optional[MetricFile], default None
            The associated MetricFile object used for disaggregation and metric-specific validation.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing rows that passed validation, with errors and warnings logged separately.
        """
        # Replace Nan with None
        sheet_df = sheet_df.replace({np.nan: None, pd.NA: None, pd.NaT: None})
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
                disaggregation_value = self.fuzzy_match_against_options(
                    text=text,
                    column_name=column_name,
                    options=[d.value for d in list(metric_file.disaggregation)],  # type: ignore[call-overload]
                    sheet_name=sheet_name,
                    row_number=row_number,
                    metric_key=metric_key,
                )

                if disaggregation_value is not None:
                    row[metric_file.disaggregation_column_name] = disaggregation_value

            # Check for invalid 'system' values
            if (
                "system" in expected_columns
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
            if "agency" in expected_columns:
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

            num_errors_after = len(
                self.metadata.metric_key_to_errors.get(metric_key, [])
            )
            if num_errors_before == num_errors_after:
                valid_rows.append(row)

        return pd.DataFrame(valid_rows)

    def standardize_sheet(
        self,
        sheet_df: pd.DataFrame,
        metric_file: Optional[MetricFile] = None,
        sheet_name: Optional[str] = None,
    ) -> Tuple[bool, pd.DataFrame]:
        """
        Standardizes a DataFrame sheet by ensuring expected columns are present and
        unexpected columns are flagged.

        This method removes unnamed columns, checks for missing and unexpected columns,
        and standardizes the column headers. It generates error or warning messages if
        any issues are found during the standardization process.

        Args:
            sheet_df : pd.DataFrame
                The DataFrame representing the sheet to be standardized.
            metric_file : Optional[MetricFile], optional
                An optional `MetricFile` object that contains the metric definition.
            sheet_name : Optional[str], optional
                The name of the sheet being standardized. This is included in error messages for context.

        Returns:
            Tuple[bool, pd.DataFrame]
                A tuple where the first element is a boolean indicating whether any errors were found
                (True if no errors, False otherwise), and the second element is the standardized DataFrame.
        """
        # Convert all column names to strings
        sheet_df.columns = sheet_df.columns.astype(str)

        # Remove unnamed columns
        sheet_df = sheet_df.loc[:, ~sheet_df.columns.str.contains("^Unnamed")]

        if "row_number" not in sheet_df.columns:
            # Insert a 'row_number' column to capture the original row numbers from the sheet.
            # This is necessary because, for single-page uploads, the file will be reformatted into
            # the standard format, and row numbers will change during that process. In the standard
            # format, the row numbers will align with the actual data rows.
            # We add 2 to the index because:
            # 1) Row numbering starts at 1 (not 0), and
            # 2) The first row in the DataFrame represents the column headers.
            sheet_df.insert(0, "row_number", sheet_df.index + 2)

        expected_columns = self.get_expected_columns(
            sheet_df=sheet_df, metric_file=metric_file
        )

        can_rows_be_ingested = self.standardize_column_headers(
            sheet_df=sheet_df,
            expected_columns=expected_columns,
            sheet_name=sheet_name,
            metric_key=metric_file.definition.key if metric_file is not None else None,
        )

        # Update DF with standardized all column headers
        sheet_df.columns = sheet_df.columns.map(self._standardize_string)

        if can_rows_be_ingested is False:
            return can_rows_be_ingested, sheet_df

        sheet_df = self.standardize_rows(
            sheet_df=sheet_df,
            sheet_name=sheet_name,
            expected_columns=expected_columns,
            metric_file=metric_file,
        )

        return True, sheet_df

    def standardize_workbook(
        self, file: Any, file_name: str
    ) -> dict[str, pd.DataFrame]:
        """
        Standardizes the sheet names, column headers, and cell values in the given Excel workbook.

        Parameters:
        file (Any): The Excel or CSV file to be processed.
        file_name (str): The name of the output Excel file.

        Returns:
        Tuple[pd.ExcelFile, str]: A tuple containing the Excel file and
        the new file name
        """

        standardized_workbook_df: dict[str, pd.DataFrame] = {}
        excel_file = self._convert_file_to_pandas_excel_file(
            file=file, file_name=file_name
        )

        workbook_df = pd.read_excel(excel_file, sheet_name=None)
        single_page_upload_columns = ["metric", "breakdown", "breakdown_category"]
        self.metadata.is_single_page_upload = len(excel_file.sheet_names) == 1 and any(
            col in workbook_df[excel_file.sheet_names[0]].columns
            for col in single_page_upload_columns
        )

        for sheet_name in workbook_df:
            sheet_df = workbook_df[sheet_name]
            standardized_sheet_name = self._standardize_string(sheet_name)
            metric_file = get_metricfile_by_sheet_name(
                sheet_name=standardized_sheet_name, system=self.metadata.system
            )
            if metric_file is None and self.metadata.is_single_page_upload is False:
                # 1) Don't write sheet with invalid sheet name to new excel object
                # 2) Add sheet name to invalid sheet names
                self.invalid_sheet_names.add(sheet_name)
                continue

            (can_sheet_be_ingested, standardized_sheet_df,) = self.standardize_sheet(
                sheet_df=sheet_df,
                metric_file=metric_file,
                sheet_name=sheet_name,
            )

            if can_sheet_be_ingested is True:
                standardized_workbook_df[sheet_name] = standardized_sheet_df

        if len(self.invalid_sheet_names) > 0:
            self._add_invalid_name_error()

        return standardized_workbook_df

    def _add_invalid_name_error(self) -> None:
        """
        Adds an error to the list of errors indicating that the provided CSV file or sheet name is invalid.

        This method constructs a descriptive error message explaining that the provided `sheet_name`
        does not correspond to any known metric for the agency. Depending on whether it's a CSV file
        error or an invalid sheet name error, the appropriate message is created and includes the
        expected file names. The error is then appended to the `metric_key_to_errors` attribute.

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
