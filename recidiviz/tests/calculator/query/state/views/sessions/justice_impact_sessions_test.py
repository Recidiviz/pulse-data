# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for classes/functions in justice_impact_sessions.py"""

import unittest

from recidiviz.calculator.query.state.views.sessions.justice_impact_sessions import (
    JUSTICE_IMPACT_TYPES,
    JusticeImpact,
    JusticeImpactType,
    get_ji_type_or_weight,
)


class JusticeImpactTypesTest(unittest.TestCase):
    "Test JusticeImpactType dictionary"

    def test_justice_impact_type_matches_key(self) -> None:
        "Check that justice_impact_type = key value"
        for key, value in JUSTICE_IMPACT_TYPES.items():
            self.assertEqual(
                value.justice_impact_type,
                key,
                "JusticeImpact `justice_impact_type` does not match key value.",
            )


class JusticeImpactTest(unittest.TestCase):
    "Test JusticeImpact class"

    # create test justice impact object
    test_ji_type = JusticeImpactType.MINIMUM_CUSTODY
    test_justice_impact = JusticeImpact(
        justice_impact_type=test_ji_type,
        weight=1.0,
    )

    def test_get_weights(self) -> None:
        "Check that weights are returned correctly"

        self.assertEqual(
            self.test_justice_impact.weight,
            1.0,
            "Weight is not returned correctly.",
        )

    def test_get_ji_type(self) -> None:
        "Check that justice impact type is returned correctly"

        self.assertEqual(
            self.test_justice_impact.justice_impact_type.value,
            "MINIMUM_CUSTODY",
            "Justice impact type is not returned correctly.",
        )

    def test_get_ji_type_or_weight(self) -> None:
        "Check that justice impact type or weight is returned correctly"

        self.assertEqual(
            get_ji_type_or_weight(self.test_ji_type, "type"),
            '"MINIMUM_CUSTODY"',
            "Justice impact type is not returned correctly.",
        )
        self.assertEqual(
            get_ji_type_or_weight(self.test_ji_type, "weight"),
            1.0,
            "Justice impact weight is not returned correctly.",
        )
