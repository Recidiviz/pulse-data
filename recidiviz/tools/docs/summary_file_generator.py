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
"""Utility functions to update docs/SUMMARY.md for a respective catalog."""
from typing import List

SUMMARY_PATH = "docs/SUMMARY.md"


def update_summary_file(catalog: List[str], catalog_title: str) -> None:
    """Updates the docs/SUMMARY.md file to ensure that the contents are up-to-date.
    This function takes in two parameters:
        - catalog - the list of strings to add
        - catalog_title - the title of the section to update"""

    with open(SUMMARY_PATH, "r") as summary_file:
        summary_contents = summary_file.readlines()

    remove_from = next(
        i for i, line in enumerate(summary_contents) if catalog_title in line
    )
    remove_up_until = (
        next(
            i
            for i, line in enumerate(
                summary_contents[remove_from + 1 : len(summary_contents)]
            )
            if line.startswith("#")
        )
        + remove_from
    )

    updated_summary = summary_contents[:remove_from]
    updated_summary.extend(catalog)
    updated_summary.extend(summary_contents[remove_up_until : len(summary_contents)])

    with open(SUMMARY_PATH, "w") as summary_file:
        summary_file.write("".join(updated_summary))
