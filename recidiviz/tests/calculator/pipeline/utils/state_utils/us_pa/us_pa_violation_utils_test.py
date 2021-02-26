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
from datetime import date
from typing import List

import pytest

from recidiviz.calculator.pipeline.utils.state_utils.us_pa import us_pa_violation_utils
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violation_utils import \
    us_pa_get_violation_type_subtype_strings_for_violation, us_pa_sorted_violation_subtypes_by_severity, \
    us_pa_violation_type_from_subtype, _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES, us_pa_shorthand_for_violation_subtype
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.persistence.entity.state.entities import StateSupervisionViolation, StateSupervisionViolationTypeEntry

_STATE_CODE = 'US_PA'


class TestUsPaGetViolationTypeSubstringsForViolation(unittest.TestCase):
    """Tests the us_pa_get_violation_type_subtype_strings_for_violation function."""
    def test_us_pa_get_violation_type_subtype_strings_for_violation(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.LAW)
            ]
        )

        # Act
        type_subtype_strings = us_pa_get_violation_type_subtype_strings_for_violation(violation)

        # Assert
        expected_type_subtype_strings = ['LAW']
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_substance(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text='H12'
                )
            ]
        )

        # Act
        type_subtype_strings = us_pa_get_violation_type_subtype_strings_for_violation(violation)

        # Assert
        expected_type_subtype_strings = ['SUBSTANCE_ABUSE']
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_electronic_monitoring(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text='M16'
                )
            ]
        )

        # Act
        type_subtype_strings = us_pa_get_violation_type_subtype_strings_for_violation(violation)

        # Assert
        expected_type_subtype_strings = ['ELEC_MONITORING']
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_low_technical(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text='L05'
                )
            ],
        )

        # Act
        type_subtype_strings = us_pa_get_violation_type_subtype_strings_for_violation(violation)

        # Assert
        expected_type_subtype_strings = ['LOW_TECH']
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_medium_technical(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_PA',
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text='M05'
                ),
            ]
        )

        # Act
        type_subtype_strings = us_pa_get_violation_type_subtype_strings_for_violation(violation)

        # Assert
        expected_type_subtype_strings = ['MED_TECH']
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_high_technical(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_PA',
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text='H05'
                ),
            ]
        )

        # Act
        type_subtype_strings = us_pa_get_violation_type_subtype_strings_for_violation(violation)

        # Assert
        expected_type_subtype_strings = ['HIGH_TECH']
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_unsupported_technical(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_PA',
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    # We expect all TECHNICAL violations to have definable raw text values
                    violation_type_raw_text=None
                ),
            ]
        )

        # Act
        with pytest.raises(ValueError):
            _ = us_pa_get_violation_type_subtype_strings_for_violation(violation)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_unsupported_raw_text(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_PA',
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text='XL'
                ),
            ]
        )

        # Act
        with pytest.raises(ValueError):
            _ = us_pa_get_violation_type_subtype_strings_for_violation(violation)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_no_types(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(state_code=_STATE_CODE)

        # Act
        type_subtype_strings = us_pa_get_violation_type_subtype_strings_for_violation(violation)

        # Assert
        expected_type_subtype_strings: List[str] = []
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)


class TestUsPaSortedViolationSubtypesBySeverity(unittest.TestCase):
    """Tests the us_pa_sorted_violation_subtypes_by_severity function."""
    def test_us_pa_sorted_violation_subtypes_by_severity(self) -> None:
        violation_subtypes = ['LOW_TECH', 'LAW', 'ABSCONDED']

        sorted_subtypes = us_pa_sorted_violation_subtypes_by_severity(violation_subtypes)

        expected_sorted_subtypes = ['LAW', 'ABSCONDED', 'LOW_TECH']

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)

    def test_us_pa_sorted_violation_subtypes_by_severity_high_tech(self) -> None:
        violation_subtypes = ['ABSCONDED', 'SUBSTANCE_ABUSE', 'HIGH_TECH']

        sorted_subtypes = us_pa_sorted_violation_subtypes_by_severity(violation_subtypes)

        expected_sorted_subtypes = ['HIGH_TECH', 'ABSCONDED', 'SUBSTANCE_ABUSE']

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)

    def test_us_pa_sorted_violation_subtypes_by_severity_substance_abuse(self) -> None:
        violation_subtypes = ['LOW_TECH', 'SUBSTANCE_ABUSE', 'ELEC_MONITORING']

        sorted_subtypes = us_pa_sorted_violation_subtypes_by_severity(violation_subtypes)

        expected_sorted_subtypes = ['SUBSTANCE_ABUSE', 'ELEC_MONITORING', 'LOW_TECH']

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)

    def test_us_pa_sorted_violation_subtypes_by_severity_electronic_monitoring(self) -> None:
        violation_subtypes = ['LOW_TECH', 'MED_TECH', 'ELEC_MONITORING']

        sorted_subtypes = us_pa_sorted_violation_subtypes_by_severity(violation_subtypes)

        expected_sorted_subtypes = ['ELEC_MONITORING', 'MED_TECH', 'LOW_TECH']

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)

    def test_us_pa_sorted_violation_subtypes_by_severity_empty_list(self) -> None:
        violation_subtypes: List[str] = []

        sorted_subtypes = us_pa_sorted_violation_subtypes_by_severity(violation_subtypes)

        expected_sorted_subtypes: List[str] = []

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)


class TestUsPaViolationUtilsSubtypeFunctions(unittest.TestCase):
    """Tests multiple functions in us_pa_violation_utils related to violation subtypes."""

    def test_us_pa_violation_type_from_subtype(self) -> None:
        # Assert that all of the StateSupervisionViolationType raw values map to their corresponding violation_type,
        # unless the type is in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES
        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                violation_type_from_subtype = us_pa_violation_type_from_subtype(violation_type.value)
                self.assertEqual(violation_type, violation_type_from_subtype)

    def test_us_mo_violation_type_from_subtype_low_tech(self) -> None:
        violation_subtype = 'LOW_TECH'

        violation_type_from_subtype = us_pa_violation_type_from_subtype(violation_subtype)

        self.assertEqual(StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype)

    def test_us_mo_violation_type_from_subtype_med_tech(self) -> None:
        violation_subtype = 'MED_TECH'

        violation_type_from_subtype = us_pa_violation_type_from_subtype(violation_subtype)

        self.assertEqual(StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype)

    def test_us_mo_violation_type_from_subtype_high_tech(self) -> None:
        violation_subtype = 'HIGH_TECH'

        violation_type_from_subtype = us_pa_violation_type_from_subtype(violation_subtype)

        self.assertEqual(StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype)

    def test_us_mo_violation_type_from_subtype_substance_abuse(self) -> None:
        violation_subtype = 'SUBSTANCE_ABUSE'

        violation_type_from_subtype = us_pa_violation_type_from_subtype(violation_subtype)

        self.assertEqual(StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype)

    def test_us_pa_violation_type_from_subtype_unsupported_escape(self) -> None:
        violation_subtype = 'ESCAPED'
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
            for violation_type, _, _ in us_pa_violation_utils._VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP
        ]

        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in us_pa_violation_utils._UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                self.assertTrue(violation_type in all_types_subtypes)
