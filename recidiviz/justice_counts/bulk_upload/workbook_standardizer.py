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

import math
from collections import defaultdict
from functools import cached_property
from io import StringIO
from typing import Any, Dict, Optional, Set, Tuple

import pandas as pd

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import (
    separate_file_name_from_system,
)
from recidiviz.justice_counts.bulk_upload.bulk_upload_metadata import BulkUploadMetadata
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    get_metricfile_by_sheet_name,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.types import BulkUploadFileType
from recidiviz.justice_counts.utils.constants import BREAKDOWN, BREAKDOWN_CATEGORY
from recidiviz.justice_counts.utils.metric_breakdown_to_sheet_name import (
    METRIC_BREAKDOWN_PAIR_TO_SHEET_NAME,
)
from recidiviz.persistence.database.schema.justice_counts import schema


class WorkbookStandardizer:
    """Standardizes Excel workbooks to comply with the Justice Counts technical specification."""

    def __init__(self, metadata: BulkUploadMetadata) -> None:
        self.metadata = metadata
        self.invalid_sheet_names: Set[str] = set()
        self.is_csv_upload = False

    @cached_property
    def child_agency_name_to_agency(self) -> Dict[str, schema.Agency]:
        """
        Constructs a dictionary mapping normalized child agency names and
        custom child agency names to their corresponding agency objects.

        Returns:
            A dictionary mapping normalized (lowercase, stripped) child agency names to
            their corresponding `schema.Agency` objects.
        """

        child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=self.metadata.session, agency=self.metadata.agency
        )

        child_agency_name_to_agency = {}
        for child_agency in child_agencies:
            child_agency_name_to_agency[
                child_agency.name.strip().lower()
            ] = child_agency
            if child_agency.custom_child_agency_name is not None:
                # Add the custom_child_agency_name of the agency as a key in
                # child_agency_name_to_agency. That way, Bulk Upload will
                # be successful if the user uploads with EITHER the name
                # or the original name of the agency
                child_agency_name_to_agency[
                    child_agency.custom_child_agency_name.strip().lower()
                ] = child_agency

        return child_agency_name_to_agency

    @cached_property
    def metric_key_to_metric_interface(self) -> Dict[str, MetricInterface]:
        return MetricSettingInterface.get_metric_key_to_metric_interface(
            session=self.metadata.session, agency=self.metadata.agency
        )

    def get_new_file_name_and_sheet_name(self, file_name: str) -> Tuple[str, str]:
        """
        Generates a new file name and corresponding sheet name for a converted Excel file.

        Args:
            file_name (str): The original file name.

        Returns:
            Tuple[str, str]: The new file name and the sheet name.
        """
        new_file_name = separate_file_name_from_system(file_name=file_name).rsplit(".", 1)[0] + ".xlsx"  # type: ignore[union-attr]
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

    def _standardize_string(self, value: str) -> str:
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

        value = value.strip().lower()
        value = value.replace(" ", "_")
        return value

    def _convert_file_to_pandas_excel_file(
        self, file: Any, file_name: str
    ) -> Tuple[pd.ExcelFile, str]:
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

        # Create new file name for the converted Excel file
        new_file_name, sheet_name = self.get_new_file_name_and_sheet_name(
            file_name=file_name
        )

        if file_type != BulkUploadFileType.CSV:
            # If file is already in Excel format, return it
            xls = pd.ExcelFile(file)
            return xls, new_file_name

        # Convert bytes to string if necessary
        self.is_csv_upload = True
        if isinstance(file, bytes):
            s = str(file, "utf-8")
            file = StringIO(s)
        else:
            file.stream.seek(0)

        # Read CSV file and convert it to Excel
        csv_df = pd.read_csv(file)
        csv_df.to_excel(new_file_name, sheet_name=sheet_name, index=False)
        xls = pd.ExcelFile(new_file_name)
        return xls, new_file_name

    def _maybe_add_missing_column_error(
        self,
        df: pd.DataFrame,
        column: str,
        metric_key: Optional[str] = None,
    ) -> bool:
        """
        Checks for missing or empty data in a specified column of a DataFrame and reports an error if needed.

        Args:
            df (pd.DataFrame): The DataFrame to check for missing or empty columns.

            column (str): The name of the column to check for presence and data.

            metric_key (Optional[str]): An optional key used to identify the metric in the error reporting.

        Returns:
            bool: True if the column is missing or contains only NaN values, False otherwise.
        """
        is_missing_column = False
        title = ""
        description = ""

        if column not in df.columns:
            title = f"Missing '{column}' Column"
            description = (
                f"This sheet should contain a column named '{column}' populated with data. "
                f"Only the following columns were found in the sheet: "
                f"{', '.join(df.columns)}. "
            )
            is_missing_column = True
        elif df[column].isna().all():
            title = f"Empty '{column}' Column"
            description = (
                f"No data was provided in the '{column}' column in your sheet. "
            )
            is_missing_column = True

        if is_missing_column is True:
            if column == "month":
                description += "Since the 'month' column is missing, your data will be recorded with an annual reporting frequency. The 'month' column should include integers ranging from 1 to 12."
            self.metadata.metric_key_to_errors[metric_key].append(
                JusticeCountsBulkUploadException(
                    title=title,
                    description=description,
                    message_type=BulkUploadMessageType.ERROR,
                )
            )
        return is_missing_column

    def _validate_single_page_upload_column_headers(self, df: pd.DataFrame) -> bool:
        """
        Validates the column headers of a DataFrame for a single-page bulk upload.
        Specifically, it checks for the presence of the 'year' and 'value' columns, and
        verifies that 'breakdown_category' and 'breakdown' columns are present if their
        corresponding data columns contain non-null values.

        Args:
            df (pd.DataFrame): The DataFrame to validate.

        Returns:
            bool: True if all required columns are present and valid, False otherwise.
        """
        is_missing_column = set()
        is_missing_column.add(
            self._maybe_add_missing_column_error(df=df, column="year")
        )
        is_missing_column.add(
            self._maybe_add_missing_column_error(df=df, column="value")
        )

        reporting_frequencies = {
            metric_interface.get_reporting_frequency_to_use()[0]
            for metric_interface in self.metric_key_to_metric_interface.values()
        }

        if schema.ReportingFrequency.MONTHLY in reporting_frequencies:
            is_missing_column.add(
                self._maybe_add_missing_column_error(df=df, column="month")
            )

        if BREAKDOWN_CATEGORY not in df.columns and (
            BREAKDOWN in df.columns and bool(df[BREAKDOWN].isna().all()) is False
        ):
            # If there is a breakdown column with data but no breakdown_category column, throw an error.
            is_missing_column.add(
                self._maybe_add_missing_column_error(df=df, column=BREAKDOWN_CATEGORY)
            )

        if BREAKDOWN not in df.columns and (
            BREAKDOWN_CATEGORY in df.columns
            and bool(df[BREAKDOWN_CATEGORY].isna().all()) is False
        ):
            # If there is a breakdown_category column with data but no breakdown column, throw an error.
            is_missing_column.add(
                self._maybe_add_missing_column_error(df=df, column=BREAKDOWN)
            )

        return True not in is_missing_column

    def _process_metric_df(
        self,
        df: pd.DataFrame,
        breakdown_category: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Helper function to process/convert a dataframe from single-page bulk upload.
        This function drops and renames columns so that they align with what the rest
        of the Bulk Upload flow expects.

        Args:
            df (pd.DataFrame): The DataFrame to be processed. It should contain the columns that may need transformation.

            breakdown_category (Optional[str]): The name to rename the 'breakdown' column to. If None, the column is not renamed.

        Returns:
            pd.DataFrame: The transformed DataFrame with unnecessary columns removed and the 'breakdown' column renamed if applicable.

        """
        # Drop any columns in which the entire column is empty
        # Examples of this:
        # - month column for annual metrics
        # - breakdown_category and breakdown columns for aggregate metrics
        df = df.dropna(
            axis=1,
            how="all",
        )
        # Drop metric column
        # We already used this to break up the single sheet into multiple sheets
        # (1 sheet for each metric)
        # This column would cause an error further in the Bulk Upload flow if kept
        df = df.drop(
            labels=["metric"],
            axis=1,
        )
        # Drop breakdown_category column
        # We already used this column to get the appropriate sheet_name for the given
        # (metric, breakdown) pair
        # This column would cause an error further in the Bulk Upload flow if kept
        if BREAKDOWN_CATEGORY in df.columns:
            df = df.drop(
                labels=[BREAKDOWN_CATEGORY],
                axis=1,
            )
        # Rename breakdown_value, aggregate_value, and breakdown columns if present
        # The rest of the Bulk Upload flow will expect 'value' and the breakdown_category
        df = df.rename(
            {
                BREAKDOWN: breakdown_category,
            },
            axis=1,
        )
        return df

    def _transform_combined_metric_file_upload(
        self,
        df: pd.DataFrame,
    ) -> Dict[str, pd.DataFrame]:
        """
        This function transforms an uploaded file that contains only 1 sheet.
        In this case, the file contains a single sheet that includes data for
        more than 1 metric (distinguished by the 'metric' column). This function breaks
        the data up by metric and breakdown_category (if present). It then exports the
        data into a temporary excel file that contains one sheet per metric/breakdown.
        It then loads the new temporary file and returns the xls to continue the rest of
        the Bulk Upload process.

        Args:
            df (pd.DataFrame): A DataFrame containing the data to be transformed. The DataFrame
            is in the single-page upload format.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary where the keys are sheet names and the values are
            DataFrames containing data in the sheet.

        """
        workbook_dfs: Dict[str, pd.DataFrame] = defaultdict(pd.DataFrame)
        if self._validate_single_page_upload_column_headers(df=df) is False:
            return {}

        for metric in df["metric"].unique():
            metric_df = df[df["metric"] == metric]
            if BREAKDOWN_CATEGORY not in df.columns:
                # Process metric_df and concatenate it to the existing DataFrame
                processed_df = self._process_metric_df(df=metric_df)
                workbook_dfs[metric] = pd.concat(
                    [workbook_dfs[metric], processed_df], axis=0, ignore_index=True
                )
                continue

            for breakdown_category in metric_df[BREAKDOWN_CATEGORY].unique():
                if isinstance(breakdown_category, float) and math.isnan(
                    breakdown_category
                ):
                    # Handle aggregate metrics
                    aggregate_df = metric_df[metric_df[BREAKDOWN_CATEGORY].isnull()]
                    processed_df = self._process_metric_df(df=aggregate_df)
                    workbook_dfs[metric] = pd.concat(
                        [workbook_dfs[metric], processed_df], axis=0, ignore_index=True
                    )
                else:
                    # Handle breakdown metrics
                    breakdown_df = metric_df[
                        metric_df[BREAKDOWN_CATEGORY] == breakdown_category
                    ]
                    try:
                        sheet_name = METRIC_BREAKDOWN_PAIR_TO_SHEET_NAME[
                            metric, breakdown_category
                        ]
                        processed_df = self._process_metric_df(
                            df=breakdown_df, breakdown_category=breakdown_category
                        )
                        workbook_dfs[sheet_name] = pd.concat(
                            [workbook_dfs[sheet_name], processed_df],
                            axis=0,
                            ignore_index=True,
                        )
                    except KeyError:
                        continue
                        # Don't raise error if the we cannot find the sheet name based
                        # upon the metric / breakdown category. The Category Not Recognized Warning
                        # will be added to metric_key_to_error in the fuzzy_match_against_options
                        # method.

                        # TODO(#31610)Raise error if breakdown_category value or breakdown value is invalid.
        return workbook_dfs

    def standardize_workbook(self, file: Any, file_name: str) -> pd.ExcelFile:
        """
        Standardizes the sheet names, column headers, and cell values in the given Excel workbook.

        Parameters:
        file (Any): The Excel or CSV file to be processed.
        file_name (str): The name of the output Excel file.

        Returns:
        Tuple[pd.ExcelFile, str]: A tuple containing the Excel file and
        the new file name
        """
        excel_file, standardized_file_name = self._convert_file_to_pandas_excel_file(
            file=file, file_name=file_name
        )

        # Create a copy of the Excel file that can be updated
        with pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
            standardized_file_name
        ) as writer:
            workbook_df = pd.read_excel(excel_file, sheet_name=None)
            if len(excel_file.sheet_names) == 1:
                sheet_df = workbook_df[excel_file.sheet_names[0]]
                if "metric" in sheet_df.columns:
                    sheet_df.columns = sheet_df.columns.map(
                        self._standardize_string
                    )  # standardize all column headers
                    workbook_df = self._transform_combined_metric_file_upload(
                        df=sheet_df,
                    )

            for sheet_name in workbook_df:
                sheet_df = workbook_df[sheet_name]
                standardized_sheet_name = self._standardize_string(sheet_name)
                if (
                    get_metricfile_by_sheet_name(
                        sheet_name=standardized_sheet_name, system=self.metadata.system
                    )
                    is not None
                ):
                    # Update sheet name in the excel file copy
                    sheet_df.to_excel(
                        writer, sheet_name=standardized_sheet_name, index=False
                    )
                else:
                    # 1) Don't write sheet with invalid sheet name to new excel object
                    # 2) Add sheet name to invalid sheet names
                    self.invalid_sheet_names.add(sheet_name)

        if len(self.invalid_sheet_names) > 0:
            self._add_invalid_name_error()

        return pd.ExcelFile(standardized_file_name)

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
