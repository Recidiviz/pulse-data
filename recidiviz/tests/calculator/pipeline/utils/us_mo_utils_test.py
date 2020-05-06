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

import pytest

from recidiviz.calculator.pipeline.utils.calculator_utils import identify_violation_subtype
# pylint: disable=protected-access
from recidiviz.calculator.pipeline.utils.us_mo_utils import get_ranked_violation_type_and_subtype_counts, \
    _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP, _LAW_CITATION_SUBTYPE_STR, \
    _SUBSTANCE_ABUSE_CONDITION_STR, us_mo_filter_violation_responses, normalize_violations_on_responses
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response import StateSupervisionViolationResponseType
from recidiviz.persistence.entity.state.entities import StateSupervisionViolation, \
    StateSupervisionViolatedConditionEntry, StateSupervisionViolationTypeEntry, StateSupervisionViolationResponse

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

    def test_identifyViolationSubtype_technical_drg(self):
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

    def test_identifyViolationSubtype_technical_law_citation(self):
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
                        condition=_SUBSTANCE_ABUSE_CONDITION_STR
                    ),
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition=_LAW_CITATION_SUBTYPE_STR
                    )
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
        self.assertEqual(_LAW_CITATION_SUBTYPE_STR, subtype)

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

    def test_violationTypeAndSubtypeShorthandMap_isComplete(self):
        for violation_type in StateSupervisionViolationType:
            self.assertTrue(violation_type.value in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP.keys())

    def test_handle_citation_violations_us_mo(self):
        # Arrange
        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                state_code='US_MO',
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(condition='LAW'),
                ]
            )

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.CITATION,
                supervision_violation=supervision_violation
            )

        # Act
        _ = normalize_violations_on_responses(supervision_violation_response)

        # Assert
        self.assertEqual(supervision_violation.supervision_violation_types, [
            StateSupervisionViolationTypeEntry(
                state_code='US_MO',
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text=None
            )
        ])
        self.assertEqual(supervision_violation.supervision_violated_conditions, [
            StateSupervisionViolatedConditionEntry.new_with_defaults(condition=_LAW_CITATION_SUBTYPE_STR),
        ])

    def test_handle_citation_violations_us_mo_no_conditions(self):
        # Arrange
        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                state_code='US_MO'
            )

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.CITATION,
                supervision_violation=supervision_violation
            )

        # Act
        _ = normalize_violations_on_responses(supervision_violation_response)

        # Assert
        self.assertEqual([
            StateSupervisionViolationTypeEntry(
                state_code='US_MO',
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text=None
            )
        ], supervision_violation.supervision_violation_types)
        self.assertEqual([], supervision_violation.supervision_violated_conditions)

    def test_handle_citation_violations_violation_report_us_mo(self):
        # Arrange
        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                state_code='US_MO',
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(condition='LAW'),
                ]
            )

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                supervision_violation=supervision_violation
            )

        # Act
        _ = normalize_violations_on_responses(supervision_violation_response)

        # Assert
        self.assertEqual(supervision_violation.supervision_violation_types, [])
        self.assertEqual(supervision_violation.supervision_violated_conditions, [
            StateSupervisionViolatedConditionEntry.new_with_defaults(condition='LAW'),
        ])

    def test_handle_citation_violations_not_us_mo(self):
        # Arrange
        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                state_code='US_NOT_MO',
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(condition='LAW'),
                ]
            )

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_NOT_MO',
                response_type=StateSupervisionViolationResponseType.CITATION,
                supervision_violation=supervision_violation
            )

        # Act and Assert
        with pytest.raises(ValueError):
            # This function should only be called for responses from US_MO
            _ = normalize_violations_on_responses(supervision_violation_response)

    def test_filter_violation_responses_INI(self):
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.CITATION,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='INI'  # Should be included
            )
        ]

        filtered_responses = us_mo_filter_violation_responses(supervision_violation_responses)

        self.assertEqual(supervision_violation_responses, filtered_responses)

    def test_filter_violation_responses_ITR(self):
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.CITATION,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='ITR'  # Should be included
            )
        ]

        filtered_responses = us_mo_filter_violation_responses(supervision_violation_responses)

        self.assertEqual(supervision_violation_responses, filtered_responses)

    def test_filter_violation_responses_do_not_include(self):
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.CITATION,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='SUP'  # Should not be included
            )
        ]

        filtered_responses = us_mo_filter_violation_responses(supervision_violation_responses)

        expected_output = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.CITATION,
            )
        ]

        self.assertEqual(expected_output, filtered_responses)

    def test_filter_violation_responses_none_valid(self):
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='SUP'  # Should not be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='HOF'  # Should not be included
            )
        ]

        filtered_responses = us_mo_filter_violation_responses(supervision_violation_responses)

        expected_output = []

        self.assertEqual(expected_output, filtered_responses)

    def test_filter_violation_responses_unexpected_subtype(self):
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='XXX'  # Not one we expect to see
            )
        ]

        with pytest.raises(ValueError):
            _ = us_mo_filter_violation_responses(supervision_violation_responses)
