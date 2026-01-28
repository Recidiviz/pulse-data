# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for documentation_exemptions.py"""
import unittest

from recidiviz.ingest.direct.raw_data.documentation_exemptions import (
    COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS,
    COLUMN_DOCUMENTATION_FILE_LEVEL_EXEMPTIONS,
    COLUMN_DOCUMENTATION_STATE_LEVEL_EXEMPTIONS,
    DUPLICATE_COLUMN_DESCRIPTION_EXEMPTIONS,
    FILE_DOCUMENTATION_EXEMPTIONS,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.utils.string import is_meaningful_docstring


class TestDocumentationExemptions(unittest.TestCase):
    """Tests for documentation_exemptions.py"""

    def test_state_level_exemptions_not_listed_elsewhere(self) -> None:
        """States that are exempt from column documentation at the state-level should
        not have file-level or column-level exemptions (or vice versa).
        """
        for state_code in COLUMN_DOCUMENTATION_STATE_LEVEL_EXEMPTIONS:
            self.assertFalse(state_code in COLUMN_DOCUMENTATION_FILE_LEVEL_EXEMPTIONS)
            self.assertFalse(state_code in COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS)

    def test_file_level_column_exemptions_not_listed_in_column_level(self) -> None:
        """Files that are exempt from column documentation at the file level should not
        have column-level exemptions defined (or vice versa)."""
        for (
            state_code,
            file_level_exemptions,
        ) in COLUMN_DOCUMENTATION_FILE_LEVEL_EXEMPTIONS.items():
            column_level_exemptions_by_file = (
                COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS.get(state_code)
            )
            if column_level_exemptions_by_file is None:
                continue

            for file_tag in file_level_exemptions:
                self.assertFalse(file_tag in column_level_exemptions_by_file)

    def test_column_level_exemptions_necessary(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            if state_code not in COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS:
                continue
            region_config = get_region_raw_file_config(state_code.value)
            for (
                file_tag,
                exempt_columns,
            ) in COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS[state_code].items():
                raw_file_config = region_config.raw_file_configs[file_tag]
                undocumented_columns = {
                    c.name
                    for c in raw_file_config.all_columns
                    if not is_meaningful_docstring(c.description)
                }

                exempt_columns_that_are_documented = (
                    exempt_columns - undocumented_columns
                )
                if exempt_columns_that_are_documented:
                    raise ValueError(
                        f"Found columns exempt from documentation in [{state_code}] in "
                        f"file [{file_tag}] which are now documented: "
                        f"{exempt_columns_that_are_documented}. Remove these from "
                        f"COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS."
                    )

    def test_file_description_exemptions_necessary(self) -> None:
        for state_code, exempt_file_tags in FILE_DOCUMENTATION_EXEMPTIONS.items():
            region_config = get_region_raw_file_config(state_code.value)
            for file_tag in exempt_file_tags:
                raw_file_config = region_config.raw_file_configs[file_tag]
                if is_meaningful_docstring(raw_file_config.file_description):
                    raise ValueError(
                        f"Found file_description exemption for [{file_tag}] for "
                        f"state [{state_code}] in FILE_DOCUMENTATION_EXEMPTIONS. This "
                        f"file now has a valid description and can be removed from "
                        f"FILE_DOCUMENTATION_EXEMPTIONS."
                    )

    def test_all_file_tags_are_valid(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            region_config = get_region_raw_file_config(state_code.value)
            valid_file_tags = region_config.raw_file_tags

            if state_code in COLUMN_DOCUMENTATION_FILE_LEVEL_EXEMPTIONS:
                exempt_file_tags = COLUMN_DOCUMENTATION_FILE_LEVEL_EXEMPTIONS[
                    state_code
                ]

                if invalid_file_tags := exempt_file_tags - valid_file_tags:
                    raise ValueError(
                        f"Found invalid file_tags listed in "
                        f"COLUMN_DOCUMENTATION_FILE_LEVEL_EXEMPTIONS for state "
                        f"[{state_code}]: {invalid_file_tags}"
                    )

            if state_code in FILE_DOCUMENTATION_EXEMPTIONS:
                exempt_file_tags = FILE_DOCUMENTATION_EXEMPTIONS[state_code]

                if invalid_file_tags := exempt_file_tags - valid_file_tags:
                    raise ValueError(
                        f"Found invalid file_tags listed in "
                        f"FILE_DOCUMENTATION_EXEMPTIONS for state "
                        f"[{state_code}]: {invalid_file_tags}"
                    )

            if state_code in COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS:
                exempt_file_tags = set(
                    COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS[state_code]
                )

                if invalid_file_tags := exempt_file_tags - valid_file_tags:
                    raise ValueError(
                        f"Found invalid file_tags listed in "
                        f"COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS for state "
                        f"[{state_code}]: {invalid_file_tags}"
                    )

            if state_code in DUPLICATE_COLUMN_DESCRIPTION_EXEMPTIONS:
                exempt_file_tags = set(
                    DUPLICATE_COLUMN_DESCRIPTION_EXEMPTIONS[state_code]
                )

                if invalid_file_tags := exempt_file_tags - valid_file_tags:
                    raise ValueError(
                        f"Found invalid file_tags listed in "
                        f"DUPLICATE_COLUMN_DESCRIPTION_EXEMPTIONS for state "
                        f"[{state_code}]: {invalid_file_tags}"
                    )

    def test_duplicate_column_description_exemptions(
        self,
    ) -> None:
        for (
            state_code,
            exemptions_by_file,
        ) in DUPLICATE_COLUMN_DESCRIPTION_EXEMPTIONS.items():
            region_config = get_region_raw_file_config(state_code.value)
            for (
                file_tag,
                description_to_exempt_columns,
            ) in exemptions_by_file.items():
                column_to_expected_description = {}
                for description, columns in description_to_exempt_columns.items():
                    if sorted(set(columns)) != sorted(columns):
                        raise ValueError(
                            f"Found duplicates in the columns list in "
                            f"DUPLICATE_COLUMN_DESCRIPTION_EXEMPTIONS for [{file_tag}] "
                            f"in state [{state_code.value}] for description "
                            f"[{description}]."
                        )

                    for column_name in columns:
                        if column_name in column_to_expected_description:
                            raise ValueError(
                                f"Found column [{column_name}] listed in multiple places in "
                                f"DUPLICATE_COLUMN_DESCRIPTION_EXEMPTIONS for file_tag "
                                f"[{file_tag}] in state [{state_code}]."
                            )

                        column_to_expected_description[column_name] = description

                raw_file_config = region_config.raw_file_configs[file_tag]
                for column in raw_file_config.all_columns:
                    if column.name not in column_to_expected_description:
                        continue
                    if (
                        column.description
                        == column_to_expected_description[column.name]
                    ):
                        continue
                    raise ValueError(
                        f"Found column [{column.name}] in file [{file_tag}] in "
                        f"state [{state_code.value}] with description "
                        f"[{column.description}] which does not match the description "
                        f"for that column in DUPLICATE_COLUMN_DESCRIPTION_EXEMPTIONS. "
                        f"Either remove the exemption entirely (ideal) or update the "
                        f"exemption."
                    )
