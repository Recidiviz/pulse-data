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
"""Helper functions for creating BigQuery views."""
import logging
import string


def normalize_column_name_for_bq(column_name: str) -> str:
    column_name = _remove_non_printable_characters(column_name)
    # Strip whitespace from head/tail of column names
    column_name = column_name.strip()
    column_name = _make_bq_compatible_column_name(column_name)

    # BQ doesn't allow column names to begin with a number, so we prepend an underscore in that case
    if column_name[0] in string.digits:
        column_name = "_" + column_name

    return column_name


def _make_bq_compatible_column_name(column_name: str) -> str:
    """Replaces all non-allowable BigQuery characters with an underscore."""

    def is_bq_allowable_column_char(x: str) -> bool:
        return x in string.ascii_letters or x in string.digits or x == "_"

    column_name = "".join(
        [c if is_bq_allowable_column_char(c) else "_" for c in column_name]
    )
    return column_name


def _remove_non_printable_characters(column_name: str) -> str:
    """Removes all non-printable characters that occasionally show up in column names. This is known to happen in
    random columns"""
    fixed_column = "".join([x for x in column_name if x in string.printable])
    if fixed_column != column_name:
        logging.info(
            "Found non-printable characters in column [%s]. Original: [%s]",
            fixed_column,
            column_name.__repr__(),
        )
    return fixed_column
