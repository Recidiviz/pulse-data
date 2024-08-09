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

from collections import defaultdict
from functools import cached_property
from io import StringIO
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.bulk_upload.bulk_upload_helpers import (
    separate_file_name_from_system,
)
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_METRICFILES,
    get_metricfile_by_sheet_name,
)
from recidiviz.justice_counts.types import BulkUploadFileType
from recidiviz.persistence.database.schema.justice_counts import schema


class WorkbookStandardizer:
    """Standardizes Excel workbooks to comply with the Justice Counts technical specification."""

    def __init__(
        self, system: schema.System, agency: schema.Agency, session: Session
    ) -> None:
        self.session = session
        self.system = system
        self.agency = agency
        self.metric_files = SYSTEM_TO_METRICFILES[system]
        self.metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ] = defaultdict(list)
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
            session=self.session, agency=self.agency
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

    def standardize_workbook(
        self, file: Any, file_name: str
    ) -> Tuple[pd.ExcelFile, str]:
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
            for sheet_name in excel_file.sheet_names:
                standardized_sheet_name = self._standardize_string(sheet_name)
                if (
                    get_metricfile_by_sheet_name(
                        sheet_name=standardized_sheet_name, system=self.system
                    )
                    is not None
                ):
                    # Update sheet name in the excel file copy
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    df.to_excel(writer, sheet_name=standardized_sheet_name, index=False)
                else:
                    # 1) Don't write sheet with invalid sheet name to new excel object
                    # 2) Add sheet name to invalid sheet names
                    self.invalid_sheet_names.add(sheet_name)

        if len(self.invalid_sheet_names) > 0:
            self._add_invalid_name_error()

        return pd.ExcelFile(standardized_file_name), standardized_file_name

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
            [metric_file.canonical_filename for metric_file in self.metric_files]
        )
        if self.is_csv_upload:
            csv_error_description = (
                f"The file name '{self.invalid_sheet_names.pop()}' does not correspond to a metric for your agency. "
                f"For CSV uploads, the file name should exactly match one of the following options: {valid_file_names}."
            )
            self.metric_key_to_errors[None].append(
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

        self.metric_key_to_errors[None].append(
            JusticeCountsBulkUploadException(
                title="Invalid Sheet Name",
                message_type=BulkUploadMessageType.ERROR,
                description=invalid_sheet_name_error,
            ),
        )
        return
