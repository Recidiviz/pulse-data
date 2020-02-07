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
"""Tests the us_mo_utils.py file."""
import unittest
from collections import OrderedDict

from recidiviz.calculator.pipeline.utils.calculator_utils import identify_violation_subtype
# pylint: disable=protected-access
from recidiviz.calculator.pipeline.utils.us_mo_utils import get_ranked_violation_type_and_subtype_counts, \
    _ORDERED_VIOLATION_TYPES_AND_SUBTYPES, _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_MAP
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.persistence.entity.state.entities import StateSupervisionViolation, \
    StateSupervisionViolatedConditionEntry, StateSupervisionViolationTypeEntry

_STATE_CODE = 'US_MO'


class TestUsMoUtils(unittest.TestCase):
    """Test US_MO specific calculation utils."""

    def test_identifyViolationSubtype_nonTechnical(self):
        # Arrange
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL)
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='DRG')
                ]
            ),
            StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.FELONY)
                ]
            )
        ]

        # Act
        subtype = identify_violation_subtype(StateSupervisionViolationType.FELONY, violations)
        self.assertIsNone(subtype)

    def test_identifyViolationSubtype_technical(self):
        # Arrange
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL)
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='DRG')
                ]
            ),
            StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.FELONY)
                ]
            )
        ]

        # Act
        subtype = identify_violation_subtype(StateSupervisionViolationType.TECHNICAL, violations)

        # Assert
        self.assertEqual('SUBSTANCE_ABUSE', subtype)

    def test_identifyViolationSubtype_technicalNoSubtype(self):
        # Arrange
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL)
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='LAW')
                ]
            ),
            StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.FELONY)
                ]
            )
        ]

        # Act
        subtype = identify_violation_subtype(StateSupervisionViolationType.TECHNICAL, violations)

        # Assert
        self.assertEqual(None, subtype)

    def test_getRankedViolationTypeAndSubtypeCounts(self):
        # Arrange
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL)
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='LAW')
                ]
            ),
            StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL)
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='DRG')
                ]
            ),
            StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.FELONY)
                ]
            ),
            StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.MISDEMEANOR)
                ]
            )
        ]

        # Act
        ordered_counts = get_ranked_violation_type_and_subtype_counts(violations)

        # Assert
        expected_counts = OrderedDict([
            ('fel', 1),
            ('misd', 1),
            ('subs', 1),
            ('tech', 1)
        ])
        self.assertEqual(expected_counts, ordered_counts)

    def test_orderedViolationTypesAndSubtypes_isComplete(self):
        for violation_type in StateSupervisionViolationType:
            self.assertTrue(violation_type in _ORDERED_VIOLATION_TYPES_AND_SUBTYPES)

    def test_violationTypeAndSUbtypeShorthandMap_isComplete(self):
        for violation_type in _ORDERED_VIOLATION_TYPES_AND_SUBTYPES:
            self.assertTrue(violation_type in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_MAP)
