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
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Type

from thefuzz import fuzz

from recidiviz.common.text_analysis import (
    REMOVE_MULTIPLE_WHITESPACES,
    REMOVE_WORDS_WITH_DIGITS_WEBSITES_ENCODINGS,
    REMOVE_WORDS_WITH_NON_CHARACTERS,
    Normalizer,
    TextAnalyzer,
)
from recidiviz.justice_counts.exceptions import (
    BulkUploadMessageType,
    JusticeCountsBulkUploadException,
)
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

PYTHON_TYPE_TO_READABLE_NAME = {"int": "a number", "float": "a number", "str": "text"}


def get_annual_year_from_fiscal_year(fiscal_year: str) -> Optional[str]:
    """Takes as input a string and attempts to find the corresponding year"""
    return fiscal_year[0 : fiscal_year.index("-")]


def fuzzy_match_against_options(
    analyzer: TextAnalyzer,
    text: str,
    options: List[str],
    category_name: str,
    metric_key_to_errors: Dict[Optional[str], List[JusticeCountsBulkUploadException]],
    metric_key: Optional[str] = None,
    time_range: Optional[Tuple[date, date]] = None,
) -> str:
    """Given a piece of input text and a list of options, uses
    fuzzy matching to calculate a match score between the input
    text and each option. Returns the option with the highest
    score, as long as the score is above a cutoff.
    """
    option_to_score = {
        option: fuzz.ratio(  # type: ignore[attr-defined]
            analyzer.normalize_text(text, stem_tokens=True, normalizers=NORMALIZERS),
            analyzer.normalize_text(option, stem_tokens=True, normalizers=NORMALIZERS),
        )
        for option in options
    }

    best_option = max(option_to_score, key=option_to_score.get)  # type: ignore[arg-type]
    if option_to_score[best_option] < FUZZY_MATCHING_SCORE_CUTOFF:
        category_not_recognized_warning = JusticeCountsBulkUploadException(
            title=f"{category_name} Not Recognized",
            description=f"\"{text}\" is not a valid value for {category_name}. The valid values for this column are {', '.join(filter(None, options))}.",
            message_type=BulkUploadMessageType.WARNING,
            time_range=time_range,
        )
        metric_key_to_errors[metric_key].append(category_not_recognized_warning)

    return best_option


def get_column_value(
    row: Dict[str, Any],
    column_name: str,
    column_type: Type,
    analyzer: TextAnalyzer,
    metric_key_to_errors: Dict[Optional[str], List[JusticeCountsBulkUploadException]],
    metric_key: Optional[str] = None,
) -> Any:
    """Given a row, a column name, and a column type, attempts to
    extract a value of the given type from the row."""
    if column_name not in row:
        # This will occur if the expected column is missing from the sheet
        # In this case, a Missing Column error will be thrown in spreadsheet_uploader._check_expected_columns()
        return None

    column_value = row[column_name]
    # Allow numeric values with columns in them (e.g. 1,000)
    if isinstance(column_value, str):
        column_value = column_value.replace(",", "")

    try:
        value = column_type(column_value)
    except Exception as e:
        if column_name == "month":
            # Allow "month" column to be either numbers or month names
            column_value = get_month_value_from_string(
                text_analyzer=analyzer,
                month=column_value,
                metric_key_to_errors=metric_key_to_errors,
                metric_key=metric_key,
            )
            value = column_type(column_value)
        elif column_name == "year" and "-" in str(column_value):
            column_value = get_annual_year_from_fiscal_year(
                fiscal_year=str(column_value)
            )
            value = column_type(column_value)
        else:
            raise JusticeCountsBulkUploadException(
                title="Wrong Value Type",
                message_type=BulkUploadMessageType.ERROR,
                description=f'We expected all values in the column named "{column_name}" to '
                f"be {PYTHON_TYPE_TO_READABLE_NAME.get(column_type.__name__, column_type.__name__)}. Instead we found the value "
                f'"{column_value}", which is {PYTHON_TYPE_TO_READABLE_NAME.get(type(column_value).__name__, type(column_value).__name__)}.',
            ) from e

    # Round numbers to two decimal places
    if isinstance(value, float):
        value = round(value, 2)

    return value


def get_month_value_from_string(
    month: str,
    text_analyzer: TextAnalyzer,
    metric_key_to_errors: Dict[Optional[str], List[JusticeCountsBulkUploadException]],
    metric_key: Optional[str] = None,
) -> int:
    """Takes as input a string and attempts to find the corresponding month
    index using the calendar module's month_names enum. For instance,
    March -> 3. Uses fuzzy matching to handle typos, such as `Febuary`."""
    column_value = month.title()
    if column_value not in MONTH_NAMES:
        column_value = fuzzy_match_against_options(
            analyzer=text_analyzer,
            category_name="Month",
            text=column_value,
            options=MONTH_NAMES,
            metric_key_to_errors=metric_key_to_errors,
            metric_key=metric_key,
        )
    return MONTH_NAMES.index(column_value)


def separate_file_name_from_system(filename: str) -> str:
    parts = filename.split("/")
    if len(parts) == 2 and parts[0] in {system.value for system in schema.System}:
        return parts[1]
    return filename
