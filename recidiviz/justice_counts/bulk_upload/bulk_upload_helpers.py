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
from typing import Dict, List, Optional, Set

from recidiviz.common.text_analysis import (
    REMOVE_MULTIPLE_WHITESPACES,
    REMOVE_WORDS_WITH_DIGITS_WEBSITES_ENCODINGS,
    REMOVE_WORDS_WITH_NON_CHARACTERS,
    Normalizer,
)
from recidiviz.justice_counts.exceptions import JusticeCountsBulkUploadException
from recidiviz.justice_counts.types import DatapointJson
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
        metric_key_to_datapoint_jsons: Dict[str, List[DatapointJson]],
        metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ],
        existing_report_ids: Optional[List[int]] = None,
        updated_reports: Optional[Set[schema.Report]] = None,
        unchanged_reports: Optional[Set[schema.Report]] = None,
    ):
        """
        Initializes the BulkUploadResult with the provided data.

        Args:
            spreadsheet (schema.Spreadsheet): The spreadsheet containing the data to be uploaded.
            existing_report_ids (List[int]): A list of IDs of existing reports.
            metric_key_to_datapoint_jsons (Dict[str, List[DatapointJson]]): A dictionary mapping metric keys to lists of datapoint JSON objects.
                Defaults to an empty defaultdict of lists.
            metric_key_to_errors (Dict[Optional[str], List[JusticeCountsBulkUploadException]]): A dictionary mapping metric keys to lists of errors encountered during the bulk upload process.
                Defaults to an empty defaultdict of lists.
            updated_reports (Optional[Set[schema.Report]]): A set of reports that have been updated.
                Defaults to an empty set.
            unchanged_reports (Optional[Set[schema.Report]]): A set of reports that remain unchanged.
                Defaults to an empty set.
        """
        self.metric_key_to_datapoint_jsons = metric_key_to_datapoint_jsons

        self.metric_key_to_errors = metric_key_to_errors

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
