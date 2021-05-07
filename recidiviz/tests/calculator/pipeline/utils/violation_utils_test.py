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
import datetime
import unittest

from recidiviz.calculator.pipeline.utils import violation_utils
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseRevocationType,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolation,
    StateSupervisionViolationTypeEntry,
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationResponse,
)

_DEFAULT_SSVR_ID = 999


class TestIdentifyMostSevereViolationType(unittest.TestCase):
    """Tests code that identifies the most severe violation type."""

    def test_identify_most_severe_violation_type(self) -> None:
        violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_XX",
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        (
            most_severe_violation_type,
            most_severe_violation_type_subtype,
        ) = violation_utils.identify_most_severe_violation_type_and_subtype([violation])

        self.assertEqual(
            most_severe_violation_type, StateSupervisionViolationType.FELONY
        )
        self.assertEqual(
            most_severe_violation_type_subtype,
            StateSupervisionViolationType.FELONY.value,
        )

    def test_identify_most_severe_violation_type_test_all_types(self) -> None:
        for violation_type in StateSupervisionViolationType:
            violation = StateSupervisionViolation.new_with_defaults(
                state_code="US_XX",
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX", violation_type=violation_type
                    )
                ],
            )
            (
                most_severe_violation_type,
                most_severe_violation_type_subtype,
            ) = violation_utils.identify_most_severe_violation_type_and_subtype(
                [violation]
            )

            self.assertEqual(most_severe_violation_type, violation_type)
            self.assertEqual(violation_type.value, most_severe_violation_type_subtype)


class TestGetViolationTypeFrequencyCounter(unittest.TestCase):
    """Tests the get_violation_type_frequency_counter function."""

    def test_get_violation_type_frequency_counter(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code="US_XX",
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.ABSCONDED,
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.FELONY,
                    ),
                ],
            )
        ]

        violation_type_frequency_counter = (
            violation_utils.get_violation_type_frequency_counter(violations)
        )

        self.assertEqual([["ABSCONDED", "FELONY"]], violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_no_types(self) -> None:
        violations = [StateSupervisionViolation.new_with_defaults(state_code="US_XX")]

        violation_type_frequency_counter = (
            violation_utils.get_violation_type_frequency_counter(violations)
        )

        self.assertIsNone(violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_us_mo(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code="US_MO",
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.ABSCONDED,
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.FELONY,
                    ),
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        state_code="US_XX", condition="DRG"
                    )
                ],
            )
        ]

        violation_type_frequency_counter = (
            violation_utils.get_violation_type_frequency_counter(violations)
        )

        self.assertEqual(
            [["ABSCONDED", "FELONY", "SUBSTANCE_ABUSE"]],
            violation_type_frequency_counter,
        )

    def test_get_violation_type_frequency_counter_us_mo_technical_only(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code="US_MO",
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        state_code="US_XX", condition="DRG"
                    )
                ],
            )
        ]

        violation_type_frequency_counter = (
            violation_utils.get_violation_type_frequency_counter(violations)
        )

        self.assertEqual([["SUBSTANCE_ABUSE"]], violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_us_mo_technical_only_no_conditions(
        self,
    ) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code="US_MO",
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                    )
                ],
            )
        ]

        violation_type_frequency_counter = (
            violation_utils.get_violation_type_frequency_counter(violations)
        )

        self.assertEqual(
            [[StateSupervisionViolationType.TECHNICAL.value]],
            violation_type_frequency_counter,
        )

    def test_get_violation_type_frequency_counter_us_mo_multiple_violations(
        self,
    ) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code="US_MO",
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.ABSCONDED,
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.FELONY,
                    ),
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        state_code="US_XX", condition="WEA"
                    )
                ],
            ),
            StateSupervisionViolation.new_with_defaults(
                state_code="US_MO",
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.MISDEMEANOR,
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                    ),
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        state_code="US_XX", condition="DRG"
                    ),
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        state_code="US_XX", condition="EMP"
                    ),
                ],
            ),
        ]

        violation_type_frequency_counter = (
            violation_utils.get_violation_type_frequency_counter(violations)
        )

        self.assertEqual(
            [["ABSCONDED", "FELONY", "WEA"], ["MISDEMEANOR", "SUBSTANCE_ABUSE", "EMP"]],
            violation_type_frequency_counter,
        )

    def test_get_violation_type_frequency_counter_us_pa(self) -> None:
        violations = [
            StateSupervisionViolation.new_with_defaults(
                state_code="US_PA",
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.ABSCONDED,
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.FELONY,
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                        violation_type_raw_text="L05",
                    ),
                ],
            ),
            StateSupervisionViolation.new_with_defaults(
                state_code="US_PA",
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.MISDEMEANOR,
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                        violation_type_raw_text="H12",
                    ),
                ],
            ),
        ]

        violation_type_frequency_counter = (
            violation_utils.get_violation_type_frequency_counter(violations)
        )

        self.assertEqual(
            [["ABSCONDED", "FELONY", "LOW_TECH"], ["MISDEMEANOR", "SUBSTANCE_ABUSE"]],
            violation_type_frequency_counter,
        )


class TestGetViolationAndResponseHistory(unittest.TestCase):
    """Tests the get_violation_and_response_history function."""

    def test_get_violation_and_response_history(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_XX",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_XX",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date, [supervision_violation_response]
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1felony",
            violation_type_frequency_counter=[["TECHNICAL", "FELONY", "ABSCONDED"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_outside_lookback(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_XX",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        # This is outside of the lookback window
        supervision_violation_response_before_look_back = (
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code="US_XX",
                response_date=datetime.date(2018, 7, 25),
            )
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_XX",
            response_date=datetime.date(2019, 1, 20),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        violation_responses = [
            supervision_violation_response_before_look_back,
            supervision_violation_response,
        ]

        end_date = datetime.date(2019, 9, 5)

        violation_history = violation_utils.get_violation_and_response_history(
            end_date,
            violation_responses,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1felony",
            violation_type_frequency_counter=[["TECHNICAL", "FELONY", "ABSCONDED"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_with_us_mo_subtype(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_MO",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
            ],
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition="DRG"
                ),
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition="OTHER"
                ),
            ],
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            state_code="US_MO",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date, [supervision_violation_response]
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype="SUBSTANCE_ABUSE",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1subs",
            violation_type_frequency_counter=[["SUBSTANCE_ABUSE", "OTHER"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_with_us_pa_subtype_high_technical(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                    violation_type_raw_text="H09",
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="H08",  # High Technical
                ),
            ],
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_PA",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date, [supervision_violation_response]
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype="HIGH_TECH",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1high_tech",
            violation_type_frequency_counter=[["ABSCONDED", "HIGH_TECH"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_with_us_pa_subtype_substance_use(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="L08",  # Substance Use
                ),
            ],
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_PA",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date, [supervision_violation_response]
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype="SUBSTANCE_ABUSE",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1subs",
            violation_type_frequency_counter=[["SUBSTANCE_ABUSE"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_with_us_pa_subtype_electronic_monitoring(
        self,
    ):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="M16",  # Electronic Monitoring
                ),
            ],
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_PA",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date, [supervision_violation_response]
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype="ELEC_MONITORING",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1em",
            violation_type_frequency_counter=[["ELEC_MONITORING"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_with_us_pa_subtype_multiple_types(self):
        supervision_violation_1 = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=12345,
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="M05",  # MED_TECH
                ),
            ],
        )

        supervision_violation_2 = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123456,
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="M05",  # MED_TECH
                ),
            ],
        )

        supervision_violation_3 = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=1234567,
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="M14",  # SUBSTANCE_ABUSE
                ),
            ],
        )

        supervision_violation_response_1 = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_PA",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=supervision_violation_1,
        )

        supervision_violation_response_2 = (
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=1234567,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code="US_PA",
                response_date=datetime.date(2009, 1, 6),
                supervision_violation=supervision_violation_2,
            )
        )

        supervision_violation_response_3 = (
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=1234567,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code="US_PA",
                response_date=datetime.date(2009, 1, 5),
                supervision_violation=supervision_violation_3,
            )
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [
                supervision_violation_response_3,
                supervision_violation_response_2,
                supervision_violation_response_1,
            ],
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype="SUBSTANCE_ABUSE",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=3,
            violation_history_description="1subs;2med_tech",
            violation_type_frequency_counter=[
                ["SUBSTANCE_ABUSE"],
                ["MED_TECH"],
                ["MED_TECH"],
            ],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_no_violations(self):
        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(2009, 1, 7),
                supervision_violation_response_decisions=[
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        state_code="US_XX",
                        decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    )
                ],
            )
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date, [supervision_violation_response]
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=None,
            most_severe_violation_type_subtype=None,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
            response_count=1,
            violation_history_description=None,
            violation_type_frequency_counter=None,
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_no_responses(self):
        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date, []
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=None,
            most_severe_violation_type_subtype=None,
            most_severe_response_decision=None,
            response_count=0,
            violation_history_description=None,
            violation_type_frequency_counter=None,
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_citation_date(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_XX",
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [supervision_violation_response],
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
            most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1misdemeanor",
            violation_type_frequency_counter=[["ABSCONDED", "MISDEMEANOR"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_us_mo_handle_law_technicals(self):
        """Tests that a US_MO violation report with a TECHNICAL type and a LAW condition is not treated like a
        citation with a LAW condition."""
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_MO",
            violation_date=datetime.date(2009, 1, 7),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
            ],
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition="LAW"
                ),
            ],
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_MO",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="ITR",
            response_date=datetime.date(2009, 1, 7),
            is_draft=False,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [supervision_violation_response],
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype=StateSupervisionViolationType.TECHNICAL.value,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1tech",
            violation_type_frequency_counter=[["LAW", "TECHNICAL"]],
        )

        self.assertEqual(expected_output, violation_history)
