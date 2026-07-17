# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for individual_level_export.py to ensure no PII columns are included."""

import unittest

from recidiviz.public_pathways.individual_level_export import (
    INCLUDED_INDIVIDUAL_LEVEL_COLUMNS,
)


class TestIncludedIndividualLevelColumnsNoPII(unittest.TestCase):
    """Tests that INCLUDED_INDIVIDUAL_LEVEL_COLUMNS does not contain PII identifiers."""

    def test_included_individual_level_columns_no_pii_columns(self) -> None:
        """Test that INCLUDED_INDIVIDUAL_LEVEL_COLUMNS does not contain PII columns.

        Ensures that no columns containing 'name', 'email', 'address', 'ind', or
        'id' are present in the included columns list. Unlike the analogous
        allowed-columns lists for the aggregate Public Pathways views, no id
        column (including person_id) is ever allowed here: this export must
        never give an external consumer a way to key off of any identifier.
        """
        pii_keywords = ["name", "email", "address", "ind"]

        # Adding "kid" to ensure that our id logic does not trip a false positive
        column_names = list(INCLUDED_INDIVIDUAL_LEVEL_COLUMNS) + ["kid"]
        for column in column_names:
            column_lower = column.lower()

            for keyword in pii_keywords:
                self.assertNotIn(
                    keyword,
                    column_lower,
                    f"Column '{column}' contains PII keyword '{keyword}' and should not be in included columns",
                )

            if "id" in column_lower.split("_"):
                self.fail(
                    f"Column '{column}' contains 'id' and should not be in included columns"
                )
