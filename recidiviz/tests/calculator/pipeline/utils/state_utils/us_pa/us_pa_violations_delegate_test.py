# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests the us_pa_violation_delegate.py file."""
import unittest
from datetime import date
from typing import List

import pytest

from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violations_delegate import (
    UsPaViolationDelegate,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolation,
    StateSupervisionViolationTypeEntry,
)

_STATE_CODE = StateCode.US_PA.value


class TestUsPaGetViolationTypeSubstringsForViolation(unittest.TestCase):
    """Tests the us_pa_get_violation_type_subtype_strings_for_violation function."""

    def setUp(self) -> None:
        self.delegate = UsPaViolationDelegate()

    def test_us_pa_get_violation_type_subtype_strings_for_violation(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.LAW,
                )
            ],
        )

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings = ["LAW"]
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_substance(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="H12",
                )
            ],
        )

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings = ["SUBSTANCE_ABUSE"]
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_electronic_monitoring(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="M16",
                )
            ],
        )

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings = ["ELEC_MONITORING"]
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_low_technical(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="L05",
                )
            ],
        )

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings = ["LOW_TECH"]
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_medium_technical(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_PA",
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="M05",
                ),
            ],
        )

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings = ["MED_TECH"]
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_high_technical(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_PA",
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="H05",
                ),
            ],
        )

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings = ["HIGH_TECH"]
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_pa_get_violation_type_subtype_strings_for_violation_unsupported_technical(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_PA",
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    # We expect all TECHNICAL violations to have definable raw text values
                    violation_type_raw_text=None,
                ),
            ],
        )

        # Act
        with pytest.raises(ValueError):
            _ = self.delegate.get_violation_type_subtype_strings_for_violation(
                violation
            )

    def test_us_pa_get_violation_type_subtype_strings_for_violation_unsupported_raw_text(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_PA",
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="XL",
                ),
            ],
        )

        # Act
        with pytest.raises(ValueError):
            _ = self.delegate.get_violation_type_subtype_strings_for_violation(
                violation
            )

    def test_us_pa_get_violation_type_subtype_strings_for_violation_no_types(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(state_code=_STATE_CODE)

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings: List[str] = []
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_violation_type_subtypes_with_violation_type_mappings(self) -> None:
        supported_violation_subtypes = (
            self.delegate.violation_type_subtypes_with_violation_type_mappings()
        )
        expected_violation_subtypes = {
            "LAW",
            "HIGH_TECH",
            "ABSCONDED",
            "SUBSTANCE_ABUSE",
            "ELEC_MONITORING",
            "MED_TECH",
            "LOW_TECH",
        }
        self.assertEqual(supported_violation_subtypes, expected_violation_subtypes)
