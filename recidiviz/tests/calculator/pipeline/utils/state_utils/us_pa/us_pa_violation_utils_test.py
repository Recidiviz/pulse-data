# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests the various functions in the us_pa_violation_utils file."""
import unittest
from typing import List

import pytest

from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violation_utils import (
    us_pa_shorthand_for_violation_subtype,
    us_pa_sorted_violation_subtypes_by_severity,
    us_pa_violation_type_from_subtype,
)

# pylint: disable=protected-access
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violations_delegate import (
    _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES,
    _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)

_STATE_CODE = "US_PA"


class TestUsPaSortedViolationSubtypesBySeverity(unittest.TestCase):
    """Tests the us_pa_sorted_violation_subtypes_by_severity function."""

    def test_us_pa_sorted_violation_subtypes_by_severity(self) -> None:
        violation_subtypes = ["LOW_TECH", "LAW", "ABSCONDED"]

        sorted_subtypes = us_pa_sorted_violation_subtypes_by_severity(
            violation_subtypes
        )

        expected_sorted_subtypes = ["LAW", "ABSCONDED", "LOW_TECH"]

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)

    def test_us_pa_sorted_violation_subtypes_by_severity_high_tech(self) -> None:
        violation_subtypes = ["ABSCONDED", "SUBSTANCE_ABUSE", "HIGH_TECH"]

        sorted_subtypes = us_pa_sorted_violation_subtypes_by_severity(
            violation_subtypes
        )

        expected_sorted_subtypes = ["HIGH_TECH", "ABSCONDED", "SUBSTANCE_ABUSE"]

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)

    def test_us_pa_sorted_violation_subtypes_by_severity_substance_abuse(self) -> None:
        violation_subtypes = ["LOW_TECH", "SUBSTANCE_ABUSE", "ELEC_MONITORING"]

        sorted_subtypes = us_pa_sorted_violation_subtypes_by_severity(
            violation_subtypes
        )

        expected_sorted_subtypes = ["SUBSTANCE_ABUSE", "ELEC_MONITORING", "LOW_TECH"]

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)

    def test_us_pa_sorted_violation_subtypes_by_severity_electronic_monitoring(
        self,
    ) -> None:
        violation_subtypes = ["LOW_TECH", "MED_TECH", "ELEC_MONITORING"]

        sorted_subtypes = us_pa_sorted_violation_subtypes_by_severity(
            violation_subtypes
        )

        expected_sorted_subtypes = ["ELEC_MONITORING", "MED_TECH", "LOW_TECH"]

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)

    def test_us_pa_sorted_violation_subtypes_by_severity_empty_list(self) -> None:
        violation_subtypes: List[str] = []

        sorted_subtypes = us_pa_sorted_violation_subtypes_by_severity(
            violation_subtypes
        )

        expected_sorted_subtypes: List[str] = []

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)


class TestUsPaViolationUtilsSubtypeFunctions(unittest.TestCase):
    """Tests multiple functions in us_pa_violation_utils related to violation subtypes."""

    def test_us_pa_violation_type_from_subtype(self) -> None:
        # Assert that all of the StateSupervisionViolationType raw values map to their corresponding violation_type,
        # unless the type is in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES
        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                violation_type_from_subtype = us_pa_violation_type_from_subtype(
                    violation_type.value
                )
                self.assertEqual(violation_type, violation_type_from_subtype)

    def test_us_mo_violation_type_from_subtype_low_tech(self) -> None:
        violation_subtype = "LOW_TECH"

        violation_type_from_subtype = us_pa_violation_type_from_subtype(
            violation_subtype
        )

        self.assertEqual(
            StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype
        )

    def test_us_mo_violation_type_from_subtype_med_tech(self) -> None:
        violation_subtype = "MED_TECH"

        violation_type_from_subtype = us_pa_violation_type_from_subtype(
            violation_subtype
        )

        self.assertEqual(
            StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype
        )

    def test_us_mo_violation_type_from_subtype_high_tech(self) -> None:
        violation_subtype = "HIGH_TECH"

        violation_type_from_subtype = us_pa_violation_type_from_subtype(
            violation_subtype
        )

        self.assertEqual(
            StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype
        )

    def test_us_mo_violation_type_from_subtype_substance_abuse(self) -> None:
        violation_subtype = "SUBSTANCE_ABUSE"

        violation_type_from_subtype = us_pa_violation_type_from_subtype(
            violation_subtype
        )

        self.assertEqual(
            StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype
        )

    def test_us_pa_violation_type_from_subtype_unsupported_escape(self) -> None:
        violation_subtype = "ESCAPED"
        with pytest.raises(ValueError):
            # We don't expect to see ESCAPED violations in US_PA and need to be notified if these appear
            _ = us_pa_violation_type_from_subtype(violation_subtype)

    def test_us_pa_shorthand_for_violation_subtype(self) -> None:
        # Assert that all of the StateSupervisionViolationType values are supported
        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                _ = us_pa_shorthand_for_violation_subtype(violation_type.value)

    # pylint: disable=protected-access
    def test_violationTypeAndSubtypeShorthandMap_isComplete(self) -> None:
        all_types_subtypes = [
            violation_type
            for violation_type, _, _ in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP
        ]

        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                self.assertTrue(violation_type in all_types_subtypes)
