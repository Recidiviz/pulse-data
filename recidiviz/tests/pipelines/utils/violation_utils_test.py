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
# pylint: disable=protected-access
"""Tests the functions in the violation_utils file."""
import datetime
import unittest

from recidiviz.common.constants.state.state_supervision_violated_condition import (
    StateSupervisionViolatedConditionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolatedConditionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
)
from recidiviz.pipelines.metrics.utils import violation_utils
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_violations_delegate import (
    UsXxViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_violations_delegate import (
    UsMoViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_violations_delegate import (
    UsPaViolationDelegate,
)

_DEFAULT_SSVR_ID = 999


class TestIdentifyMostSevereViolationType(unittest.TestCase):
    """Tests code that identifies the most severe violation type."""

    def test_identify_most_severe_violation_simple(self) -> None:
        violation = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            external_id="sv1",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        most_severe_violation = violation_utils._identify_most_severe_violation(
            [violation], UsXxViolationDelegate()
        )

        self.assertEqual(most_severe_violation, violation)

    def test_identify_most_severe_violation_multiple(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
            ],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_XX",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.LAW,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        violation_3 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123457,
            external_id="sv3",
            state_code="US_XX",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        most_severe_violation = violation_utils._identify_most_severe_violation(
            [violation_1, violation_2, violation_3], UsXxViolationDelegate()
        )

        self.assertEqual(most_severe_violation, violation_3)

    def test_identify_most_severe_violation_first(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_XX",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.ESCAPED,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.LAW,
                ),
            ],
        )

        violation_3 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123457,
            external_id="sv3",
            state_code="US_XX",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        most_severe_violation = violation_utils._identify_most_severe_violation(
            [violation_1, violation_2, violation_3], UsXxViolationDelegate()
        )

        self.assertEqual(most_severe_violation, violation_1)

    def test_identify_most_severe_violation_most_recent(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=datetime.date(2010, 1, 1),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_XX",
            violation_date=datetime.date(2012, 6, 6),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.LAW,
                ),
            ],
        )

        violation_3 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123457,
            external_id="sv3",
            state_code="US_XX",
            violation_date=datetime.date(2012, 5, 20),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        most_severe_violation = violation_utils._identify_most_severe_violation(
            [violation_1, violation_2, violation_3], UsXxViolationDelegate()
        )

        self.assertEqual(most_severe_violation, violation_2)

    def test_identify_most_severe_violation_most_recent_ensure_sorted(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_XX",
            violation_date=datetime.date(2012, 6, 6),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.LAW,
                ),
            ],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123457,
            external_id="sv3",
            state_code="US_XX",
            violation_date=datetime.date(2012, 5, 20),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        violation_3 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=datetime.date(2010, 1, 1),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        most_severe_violation = violation_utils._identify_most_severe_violation(
            [violation_1, violation_2, violation_3], UsXxViolationDelegate()
        )

        self.assertEqual(most_severe_violation, violation_1)

    def test_identify_most_severe_violation_most_recent_none(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_XX",
            violation_date=None,
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.LAW,
                ),
            ],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123457,
            external_id="sv3",
            state_code="US_XX",
            violation_date=datetime.date(2012, 5, 20),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        violation_3 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=datetime.date(2010, 1, 1),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        most_severe_violation = violation_utils._identify_most_severe_violation(
            [violation_1, violation_2, violation_3], UsXxViolationDelegate()
        )

        self.assertEqual(most_severe_violation, violation_3)

    def test_identify_most_severe_violation_empty(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=datetime.date(2010, 1, 1),
            supervision_violation_types=[],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_XX",
            violation_date=datetime.date(2012, 6, 6),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.LAW,
                ),
            ],
        )

        violation_3 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123457,
            external_id="sv3",
            state_code="US_XX",
            violation_date=datetime.date(2012, 5, 20),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        most_severe_violation = violation_utils._identify_most_severe_violation(
            [violation_1, violation_2, violation_3], UsXxViolationDelegate()
        )

        self.assertEqual(most_severe_violation, violation_2)

    def test_identify_most_severe_violation_all_empty(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=datetime.date(2010, 1, 1),
            supervision_violation_types=[],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_XX",
            violation_date=datetime.date(2012, 6, 6),
            supervision_violation_types=[],
        )

        violation_3 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123457,
            external_id="sv3",
            state_code="US_XX",
            violation_date=datetime.date(2012, 5, 20),
            supervision_violation_types=[],
        )

        most_severe_violation = violation_utils._identify_most_severe_violation(
            [violation_1, violation_2, violation_3], UsXxViolationDelegate()
        )
        self.assertIsNone(most_severe_violation)

    def test_identify_most_severe_violation_single_empty(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=datetime.date(2010, 1, 1),
            supervision_violation_types=[],
        )

        most_severe_violation = violation_utils._identify_most_severe_violation(
            [violation_1], UsXxViolationDelegate()
        )

        self.assertIsNone(most_severe_violation)

    def test_identify_most_severe_violation_fallback(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_XX",
            violation_date=None,
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.LAW,
                ),
            ],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123457,
            external_id="sv3",
            state_code="US_XX",
            violation_date=None,
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_3 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=None,
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        most_severe_violation = violation_utils._identify_most_severe_violation(
            [violation_1, violation_2, violation_3], UsXxViolationDelegate()
        )

        self.assertEqual(most_severe_violation, violation_2)


class TestViolationHistoryIdArray(unittest.TestCase):
    """Tests code that identifies the list of violation ids in the violation history."""

    def test_identify_violation_history_id_array_simple(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            external_id="sv1",
            supervision_violation_id=1000,
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            external_id="sv2",
            supervision_violation_id=2000,
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
            ],
        )

        violation_id_array = violation_utils._get_violation_history_ids_array_str(
            [violation_1, violation_2]
        )
        expected_violation_id_array = "1000,2000"
        self.assertEqual(expected_violation_id_array, violation_id_array)

    def test_identify_violation_history_id_array_single(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            external_id="sv1",
            supervision_violation_id=1000,
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_id_array = violation_utils._get_violation_history_ids_array_str(
            [violation_1]
        )
        expected_violation_id_array = "1000"
        self.assertEqual(expected_violation_id_array, violation_id_array)

    def test_identify_violation_history_id_array_empty(self) -> None:
        violation_id_array = violation_utils._get_violation_history_ids_array_str([])
        expected_violation_id_array = None
        self.assertEqual(expected_violation_id_array, violation_id_array)

    def test_identify_violation_history_id_array_some_none(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            external_id="sv1",
            supervision_violation_id=4321,
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            external_id="sv2",
            supervision_violation_id=1234,
            supervision_violation_types=[],
        )

        violation_id_array = violation_utils._get_violation_history_ids_array_str(
            [violation_1, violation_2]
        )
        expected_violation_id_array = "1234,4321"
        self.assertEqual(expected_violation_id_array, violation_id_array)

    def test_identify_violation_history_id_array_multiple(self) -> None:
        violation_1 = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=3000,
            external_id="sv1",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_2 = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=2000,
            external_id="sv2",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
            ],
        )

        violation_3 = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=1000,
            external_id="sv3",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        violation_id_array = violation_utils._get_violation_history_ids_array_str(
            [violation_1, violation_2, violation_3]
        )
        expected_violation_id_array = "1000,2000,3000"
        self.assertEqual(expected_violation_id_array, violation_id_array)


class TestGetViolationTypeFrequencyCounter(unittest.TestCase):
    """Tests the _get_violation_type_frequency_counter function."""

    def test_get_violation_type_frequency_counter(self) -> None:
        violations = [
            NormalizedStateSupervisionViolation(
                state_code="US_XX",
                external_id="sv1",
                supervision_violation_types=[
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.ABSCONDED,
                    ),
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.FELONY,
                    ),
                ],
            )
        ]

        violation_type_frequency_counter = (
            violation_utils._get_violation_type_frequency_counter(
                violations, UsXxViolationDelegate()
            )
        )

        self.assertEqual([["ABSCONDED", "FELONY"]], violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_no_types(self) -> None:
        violations = [
            NormalizedStateSupervisionViolation(
                state_code="US_XX",
                external_id="sv1",
            )
        ]

        violation_type_frequency_counter = (
            violation_utils._get_violation_type_frequency_counter(
                violations, UsXxViolationDelegate()
            )
        )

        self.assertIsNone(violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_us_mo(self) -> None:
        violations = [
            NormalizedStateSupervisionViolation(
                state_code="US_MO",
                external_id="sv1",
                supervision_violation_types=[
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.ABSCONDED,
                    ),
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.FELONY,
                    ),
                ],
                supervision_violated_conditions=[
                    NormalizedStateSupervisionViolatedConditionEntry(
                        state_code="US_XX",
                        condition=StateSupervisionViolatedConditionType.INTERNAL_UNKNOWN,
                        condition_raw_text="DRG",
                    )
                ],
            )
        ]

        violation_type_frequency_counter = (
            violation_utils._get_violation_type_frequency_counter(
                violations, UsMoViolationDelegate()
            )
        )

        self.assertEqual(
            [["ABSCONDED", "FELONY", "SUBSTANCE_ABUSE"]],
            violation_type_frequency_counter,
        )

    def test_get_violation_type_frequency_counter_us_mo_technical_only(self) -> None:
        violations = [
            NormalizedStateSupervisionViolation(
                state_code="US_MO",
                external_id="sv1",
                supervision_violation_types=[
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                    )
                ],
                supervision_violated_conditions=[
                    NormalizedStateSupervisionViolatedConditionEntry(
                        state_code="US_XX",
                        condition=StateSupervisionViolatedConditionType.INTERNAL_UNKNOWN,
                        condition_raw_text="DRG",
                    )
                ],
            )
        ]

        violation_type_frequency_counter = (
            violation_utils._get_violation_type_frequency_counter(
                violations, UsMoViolationDelegate()
            )
        )

        self.assertEqual([["SUBSTANCE_ABUSE"]], violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_us_mo_technical_only_no_conditions(
        self,
    ) -> None:
        violations = [
            NormalizedStateSupervisionViolation(
                state_code="US_MO",
                external_id="sv1",
                supervision_violation_types=[
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                    )
                ],
            )
        ]

        violation_type_frequency_counter = (
            violation_utils._get_violation_type_frequency_counter(
                violations, UsMoViolationDelegate()
            )
        )

        self.assertEqual(
            [[StateSupervisionViolationType.TECHNICAL.value]],
            violation_type_frequency_counter,
        )

    def test_get_violation_type_frequency_counter_us_mo_multiple_violations(
        self,
    ) -> None:
        violations = [
            NormalizedStateSupervisionViolation(
                state_code="US_MO",
                external_id="sv1",
                supervision_violation_types=[
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.ABSCONDED,
                    ),
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.FELONY,
                    ),
                ],
                supervision_violated_conditions=[
                    NormalizedStateSupervisionViolatedConditionEntry(
                        state_code="US_XX",
                        condition=StateSupervisionViolatedConditionType.INTERNAL_UNKNOWN,
                        condition_raw_text="WEA",
                    )
                ],
            ),
            NormalizedStateSupervisionViolation(
                state_code="US_MO",
                external_id="sv2",
                supervision_violation_types=[
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.MISDEMEANOR,
                    ),
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                    ),
                ],
                supervision_violated_conditions=[
                    NormalizedStateSupervisionViolatedConditionEntry(
                        state_code="US_XX",
                        condition=StateSupervisionViolatedConditionType.INTERNAL_UNKNOWN,
                        condition_raw_text="DRG",
                    ),
                    NormalizedStateSupervisionViolatedConditionEntry(
                        state_code="US_XX",
                        condition=StateSupervisionViolatedConditionType.INTERNAL_UNKNOWN,
                        condition_raw_text="EMP",
                    ),
                ],
            ),
        ]

        violation_type_frequency_counter = (
            violation_utils._get_violation_type_frequency_counter(
                violations, UsMoViolationDelegate()
            )
        )

        self.assertEqual(
            [["ABSCONDED", "FELONY", "WEA"], ["MISDEMEANOR", "SUBSTANCE_ABUSE", "EMP"]],
            violation_type_frequency_counter,
        )

    def test_get_violation_type_frequency_counter_us_pa(self) -> None:
        violations = [
            NormalizedStateSupervisionViolation(
                state_code="US_PA",
                external_id="sv1",
                supervision_violation_types=[
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.ABSCONDED,
                    ),
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.FELONY,
                    ),
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                        violation_type_raw_text="L05",
                    ),
                ],
            ),
            NormalizedStateSupervisionViolation(
                state_code="US_PA",
                external_id="sv2",
                supervision_violation_types=[
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.MISDEMEANOR,
                    ),
                    NormalizedStateSupervisionViolationTypeEntry(
                        state_code="US_XX",
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                        violation_type_raw_text="H12",
                    ),
                ],
            ),
        ]

        violation_type_frequency_counter = (
            violation_utils._get_violation_type_frequency_counter(
                violations, UsPaViolationDelegate()
            )
        )

        self.assertEqual(
            [["ABSCONDED", "FELONY", "LOW_TECH"], ["MISDEMEANOR", "SUBSTANCE_ABUSE"]],
            violation_type_frequency_counter,
        )


class TestGetViolationAndResponseHistory(unittest.TestCase):
    """Tests the get_violation_and_response_history function."""

    def test_get_violation_and_response_history(self) -> None:
        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_XX",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [supervision_violation_response],
            UsXxViolationDelegate(),
            incarceration_period=None,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_violation_id=123455,
            violation_history_id_array="123455",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1felony",
            violation_type_frequency_counter=[["TECHNICAL", "FELONY", "ABSCONDED"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_outside_lookback(self) -> None:
        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        # This is outside of the lookback window
        supervision_violation_response_before_look_back = (
            NormalizedStateSupervisionViolationResponse(
                sequence_num=0,
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                external_id="svr1",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code="US_XX",
                response_date=datetime.date(2018, 7, 25),
            )
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr2",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_XX",
            response_date=datetime.date(2019, 1, 20),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
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
            UsXxViolationDelegate(),
            incarceration_period=None,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_violation_id=123455,
            violation_history_id_array="123455",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1felony",
            violation_type_frequency_counter=[["TECHNICAL", "FELONY", "ABSCONDED"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_with_us_mo_subtype(self) -> None:
        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_MO",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
            ],
            supervision_violated_conditions=[
                NormalizedStateSupervisionViolatedConditionEntry(
                    state_code="US_MO",
                    condition=StateSupervisionViolatedConditionType.SUBSTANCE,
                    condition_raw_text="DRG",
                ),
                NormalizedStateSupervisionViolatedConditionEntry(
                    state_code="US_MO",
                    condition=StateSupervisionViolatedConditionType.INTERNAL_UNKNOWN,
                    condition_raw_text="OTHER",
                ),
            ],
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            state_code="US_MO",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [supervision_violation_response],
            UsMoViolationDelegate(),
            incarceration_period=None,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype="SUBSTANCE_ABUSE",
            most_severe_violation_id=123455,
            violation_history_id_array="123455",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1subs",
            violation_type_frequency_counter=[["SUBSTANCE_ABUSE", "OTHER"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_with_us_pa_subtype_high_technical(
        self,
    ) -> None:
        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                    violation_type_raw_text="H09",
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="H08",  # High Technical
                ),
            ],
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_PA",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [supervision_violation_response],
            UsPaViolationDelegate(),
            incarceration_period=None,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype="HIGH_TECH",
            most_severe_violation_id=123455,
            violation_history_id_array="123455",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1high_tech",
            violation_type_frequency_counter=[["ABSCONDED", "HIGH_TECH"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_with_us_pa_subtype_substance_use(
        self,
    ) -> None:
        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="L08",  # Substance Use
                ),
            ],
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_PA",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [supervision_violation_response],
            UsPaViolationDelegate(),
            incarceration_period=None,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype="SUBSTANCE_ABUSE",
            most_severe_violation_id=123455,
            violation_history_id_array="123455",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1subs",
            violation_type_frequency_counter=[["SUBSTANCE_ABUSE"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_with_us_pa_subtype_electronic_monitoring(
        self,
    ) -> None:
        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="M16",  # Electronic Monitoring
                ),
            ],
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_PA",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [supervision_violation_response],
            UsPaViolationDelegate(),
            incarceration_period=None,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype="ELEC_MONITORING",
            most_severe_violation_id=123455,
            violation_history_id_array="123455",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1em",
            violation_type_frequency_counter=[["ELEC_MONITORING"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_with_us_pa_subtype_multiple_types(
        self,
    ) -> None:
        supervision_violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=12345,
            external_id="sv1",
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="M05",  # MED_TECH
                ),
            ],
        )

        supervision_violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="M05",  # MED_TECH
                ),
            ],
        )

        supervision_violation_3 = NormalizedStateSupervisionViolation(
            supervision_violation_id=1234567,
            external_id="sv3",
            state_code="US_PA",
            violation_date=datetime.date(2009, 1, 3),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_PA",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="M14",  # SUBSTANCE_ABUSE
                ),
            ],
        )

        supervision_violation_response_1 = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_PA",
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_PA",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation_1,
        )

        supervision_violation_response_2 = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=1234567,
            external_id="svr2",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_PA",
            response_date=datetime.date(2009, 1, 6),
            supervision_violation=supervision_violation_2,
        )

        supervision_violation_response_3 = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=1234567,
            external_id="svr3",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_PA",
            response_date=datetime.date(2009, 1, 5),
            supervision_violation=supervision_violation_3,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [
                supervision_violation_response_3,
                supervision_violation_response_2,
                supervision_violation_response_1,
            ],
            UsPaViolationDelegate(),
            incarceration_period=None,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype="SUBSTANCE_ABUSE",
            most_severe_violation_id=1234567,
            violation_history_id_array="12345,123456,1234567",
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

    def test_get_violation_and_response_history_no_violations(self) -> None:
        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code="US_XX",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                )
            ],
        )

        revocation_date = datetime.date(2009, 2, 13)

        with self.assertRaises(TypeError):
            _ = violation_utils.get_violation_and_response_history(
                revocation_date,
                [supervision_violation_response],
                UsXxViolationDelegate(),
                incarceration_period=None,
            )

    def test_get_violation_and_response_history_no_responses(self) -> None:
        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [],
            UsXxViolationDelegate(),
            incarceration_period=None,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=None,
            most_severe_violation_type_subtype=None,
            most_severe_violation_id=None,
            violation_history_id_array=None,
            most_severe_response_decision=None,
            response_count=0,
            violation_history_description=None,
            violation_type_frequency_counter=None,
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_citation_date(self) -> None:
        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code="US_XX",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=datetime.date(2009, 1, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [supervision_violation_response],
            UsXxViolationDelegate(),
            incarceration_period=None,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
            most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
            most_severe_violation_id=123455,
            violation_history_id_array="123455",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1misdemeanor",
            violation_type_frequency_counter=[["ABSCONDED", "MISDEMEANOR"]],
        )

        self.assertEqual(expected_output, violation_history)

    def test_get_violation_and_response_history_us_mo_handle_law_technicals(
        self,
    ) -> None:
        """Tests that a US_MO violation report with a TECHNICAL type and a LAW condition is not treated like a
        citation with a LAW condition."""
        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_MO",
            violation_date=datetime.date(2009, 1, 7),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
            ],
            supervision_violated_conditions=[
                NormalizedStateSupervisionViolatedConditionEntry(
                    state_code="US_MO",
                    condition=StateSupervisionViolatedConditionType.INTERNAL_UNKNOWN,
                    condition_raw_text="LAW",
                ),
            ],
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code="US_MO",
            external_id="svr1",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="ITR",
            response_date=datetime.date(2009, 1, 7),
            is_draft=False,
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        revocation_date = datetime.date(2009, 2, 13)

        violation_history = violation_utils.get_violation_and_response_history(
            revocation_date,
            [supervision_violation_response],
            UsMoViolationDelegate(),
            incarceration_period=None,
        )

        expected_output = violation_utils.ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype=StateSupervisionViolationType.TECHNICAL.value,
            most_severe_violation_id=123455,
            violation_history_id_array="123455",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1tech",
            violation_type_frequency_counter=[["LAW", "TECHNICAL"]],
        )

        self.assertEqual(expected_output, violation_history)
