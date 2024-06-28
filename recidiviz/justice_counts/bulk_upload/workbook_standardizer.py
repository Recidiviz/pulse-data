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
from typing import Any, Dict, List, Optional, Tuple

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

    def validate_file_name(self, file_name: str) -> bool:
        """
        Validates the file name to ensure it is of an acceptable type and format.

        Args:
            file_name (str): The name of the file to validate.

        Returns:
            bool: True if the file name is valid, False otherwise.
        """
        file_type = self.get_file_type_from_file_name(file_name=file_name)
        if file_type != BulkUploadFileType.CSV:
            return True

        # Create new file name for the converted Excel file
        _, sheet_name = self.get_new_file_name_and_sheet_name(file_name=file_name)

        # Validate that the new sheet name corresponds to a valid metric file
        if (
            get_metricfile_by_sheet_name(sheet_name=sheet_name, system=self.system)
            is None
        ):
            self._add_invalid_csv_file_name_error(sheet_name=sheet_name)
            return False

        return True

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

    def convert_file_to_pandas_excel_file(
        self, file: Any, file_name: str
    ) -> Tuple[pd.ExcelFile, str, BulkUploadFileType]:
        """
        Converts a file to an Excel file if necessary, and validates the file name.

        Args:
            file (Any): The input file.
            file name (str): The name of the file.

        Returns:
            Optional[Tuple[pd.ExcelFile, str, BulkUploadFileType]]: A tuple containing the Excel file,
            the new file name, and the file type, or None if validation fails.
        """
        # Determine the file type from the file name suffix
        file_type = self.get_file_type_from_file_name(file_name=file_name)

        if file_type != BulkUploadFileType.CSV:
            # If file is already in Excel format, return it
            xls = pd.ExcelFile(file)
            return xls, file_name, file_type

        # Create new file name for the converted Excel file
        new_file_name, sheet_name = self.get_new_file_name_and_sheet_name(
            file_name=file_name
        )

        # Convert bytes to string if necessary
        if isinstance(file, bytes):
            s = str(file, "utf-8")
            file = StringIO(s)

        # Read CSV file and convert it to Excel
        file.stream.seek(0)
        csv_df = pd.read_csv(file)
        csv_df.to_excel(new_file_name, sheet_name=sheet_name, index=False)
        xls = pd.ExcelFile(new_file_name)
        return xls, new_file_name, file_type

    def _add_invalid_csv_file_name_error(self, sheet_name: str) -> None:
        """
        Adds an error to the list of errors indicating that the provided CSV file name is invalid.

        This method constructs a descriptive error message explaining that the provided `sheet_name`
        does not correspond to any known metric for the agency. The expected file names are listed
        in the error message. The error is then appended to the `metric_key_to_errors` attribute.

        Parameters:
        sheet_name (str): The name of the CSV file that failed validation.

        Returns:
        None
        """
        # Construct the description message
        valid_file_names = ", ".join(
            [metric_file.canonical_filename for metric_file in self.metric_files]
        )
        description = (
            f"The file name '{sheet_name}' does not correspond to a metric for your agency. "
            f"For CSV uploads, the file name should exactly match one of the following options: {valid_file_names}."
        )

        # Append the error to the list. The None key in metric_key_to_errors
        # is designated for non-metric errors. These are errors that cannot be
        # attributed to a particular metric, such as this one, where we are
        # not able to tell what metric the user is attempting to upload for.
        self.metric_key_to_errors[None].append(
            JusticeCountsBulkUploadException(
                title="Invalid File Name for CSV",
                message_type=BulkUploadMessageType.ERROR,
                description=description,
            )
        )
