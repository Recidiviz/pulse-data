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
"""Helpers for bulk upload functionality."""

import calendar
from typing import List, Optional, Set

from recidiviz.common.text_analysis import (
    REMOVE_MULTIPLE_WHITESPACES,
    REMOVE_WORDS_WITH_DIGITS_WEBSITES_ENCODINGS,
    REMOVE_WORDS_WITH_NON_CHARACTERS,
    Normalizer,
)
from recidiviz.justice_counts.bulk_upload.bulk_upload_metadata import BulkUploadMetadata
from recidiviz.persistence.database.schema.justice_counts import schema

MONTH_NAMES = list(calendar.month_name)
FUZZY_MATCHING_SCORE_CUTOFF = 90
NORMALIZERS: List[Normalizer] = [
    # hyphens with whitespace
    ("-", ""),
    # words with a number, "@", website, or encoding string
    REMOVE_WORDS_WITH_DIGITS_WEBSITES_ENCODINGS,
    # all non characters (numbers, punctuation, non-spaces)
    REMOVE_WORDS_WITH_NON_CHARACTERS,  # remove anything not a character or space
    # multiple whitespaces
    REMOVE_MULTIPLE_WHITESPACES,
    # remove the word "origin" that may show up in race/ethnicity breakdowns
    ("origin", ""),
]


def separate_file_name_from_folder(file_name: str) -> str:
    parts = file_name.split("/")
    if len(parts) > 0:
        return parts[-1]
    return file_name


class BulkUploadResult:
    """
    A class to represent the return type of a bulk upload process.

    This class holds various pieces of data resulting from a bulk upload process,
    including the spreadsheet, existing report IDs, datapoints, errors, and updated or unchanged reports.
    """

    def __init__(
        self,
        spreadsheet: schema.Spreadsheet,
        metadata: BulkUploadMetadata,
        existing_report_ids: Optional[List[int]] = None,
        updated_reports: Optional[Set[schema.Report]] = None,
        unchanged_reports: Optional[Set[schema.Report]] = None,
    ):
        """
        Initializes the BulkUploadResult with the provided data.

        Args:
            spreadsheet (schema.Spreadsheet): The spreadsheet containing the data to be uploaded.
            metadata (BulkUploadMetadata): Metadata related to the bulk upload process,
                including details about the agency, user performing the upload,
                errors encountered, chunk size, and other relevant context.
            existing_report_ids (Optional[List[int]]): A list of IDs of existing reports that were considered
                during the upload process. Defaults to an empty list.
            updated_reports (Optional[Set[schema.Report]]): A set of reports that were modified as a result
                of the upload. Defaults to an empty set.
            unchanged_reports (Optional[Set[schema.Report]]): A set of reports that remained unchanged
                during the upload process. Defaults to an empty set.
        """
        self.metadata = metadata

        self.updated_reports: Set[schema.Report] = (
            updated_reports if updated_reports is not None else set()
        )
        self.unchanged_reports: Set[schema.Report] = (
            unchanged_reports if unchanged_reports is not None else set()
        )
        self.existing_report_ids: List[int] = (
            existing_report_ids if existing_report_ids is not None else []
        )

        self.spreadsheet = spreadsheet
