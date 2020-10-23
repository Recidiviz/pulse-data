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
"""Tests the functions in the violation_utils file."""
import unittest

from recidiviz.calculator.pipeline.utils import violation_utils
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.persistence.entity.state.entities import StateSupervisionViolation, StateSupervisionViolationTypeEntry, \
    StateSupervisionViolatedConditionEntry


class TestIdentifyMostSevereViolationType(unittest.TestCase):
    """Tests code that identifies the most severe violation type."""

    def test_identify_most_severe_violation_type(self) -> None:
        violation = StateSupervisionViolation.new_with_defaults(
            state_code='US_XX',
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.TECHNICAL
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.FELONY
                )
            ])

        most_severe_violation_type, most_severe_violation_type_subtype = \
            violation_utils.identify_most_severe_violation_type_and_subtype([violation])

        self.assertEqual(most_severe_violation_type, StateSupervisionViolationType.FELONY)
        self.assertEqual(most_severe_violation_type_subtype, StateSupervisionViolationType.FELONY.value)

    def test_identify_most_severe_violation_type_test_all_types(self) -> None:
        for violation_type in StateSupervisionViolationType:
            violation = StateSupervisionViolation.new_with_defaults(
                state_code='US_XX',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=violation_type)
                ])
            most_severe_violation_type, most_severe_violation_type_subtype = \
                violation_utils.identify_most_severe_violation_type_and_subtype([violation])

            self.assertEqual(most_severe_violation_type, violation_type)
            self.assertEqual(violation_type.value, most_severe_violation_type_subtype)


class TestGetViolationTypeFrequencyCounter(unittest.TestCase):
    """Tests the get_violation_type_frequency_counter function."""

    def test_get_violation_type_frequency_counter(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code='US_XX',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.ABSCONDED
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.FELONY
                    )
                ],
            )
        ]

        violation_type_frequency_counter = violation_utils.get_violation_type_frequency_counter(violations)

        self.assertEqual([['ABSCONDED', 'FELONY']], violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_no_types(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code='US_XX',
                supervision_violation_types=None,
            )
        ]

        violation_type_frequency_counter = violation_utils.get_violation_type_frequency_counter(violations)

        self.assertIsNone(violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_us_mo(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code='US_MO',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.ABSCONDED
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.FELONY
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='DRG'
                    )
                ]
            )
        ]

        violation_type_frequency_counter = violation_utils.get_violation_type_frequency_counter(violations)

        self.assertEqual([['ABSCONDED', 'FELONY', 'SUBSTANCE_ABUSE']],
                         violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_us_mo_technical_only(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code='US_MO',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='DRG'
                    )
                ]
            )
        ]

        violation_type_frequency_counter = violation_utils.get_violation_type_frequency_counter(violations)

        self.assertEqual([['SUBSTANCE_ABUSE']], violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_us_mo_technical_only_no_conditions(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code='US_MO',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL
                    )
                ]
            )
        ]

        violation_type_frequency_counter = violation_utils.get_violation_type_frequency_counter(violations)

        self.assertEqual([[StateSupervisionViolationType.TECHNICAL.value]],
                         violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_us_mo_multiple_violations(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code='US_MO',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.ABSCONDED
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.FELONY
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='WEA'
                    )
                ]
            ),
            StateSupervisionViolation.new_with_defaults(
                state_code='US_MO',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.MISDEMEANOR
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='DRG'
                    ),
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='EMP'
                    )
                ]
            )
        ]

        violation_type_frequency_counter = violation_utils.get_violation_type_frequency_counter(violations)

        self.assertEqual([['ABSCONDED', 'FELONY', 'WEA'], ['MISDEMEANOR', 'SUBSTANCE_ABUSE', 'EMP']],
                         violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_us_pa(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code='US_PA',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.ABSCONDED
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.FELONY
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                        violation_type_raw_text='L05'
                    ),
                ]
            ),
            StateSupervisionViolation.new_with_defaults(
                state_code='US_PA',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.MISDEMEANOR
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                        violation_type_raw_text='H12'
                    )
                ]
            )
        ]

        violation_type_frequency_counter = violation_utils.get_violation_type_frequency_counter(violations)

        self.assertEqual([['ABSCONDED', 'FELONY', 'LOW_TECH'], ['MISDEMEANOR', 'SUBSTANCE_ABUSE']],
                         violation_type_frequency_counter)
