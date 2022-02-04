# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Unit tests for state_specific_supervision_delegate default functions"""
import unittest
from datetime import date
from typing import Optional

from parameterized import parameterized

from recidiviz.calculator.pipeline.metrics.supervision.events import (
    SupervisionPopulationEvent,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationSentence,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)


class TestStateSpecificSupervisionDelegate(unittest.TestCase):
    """Unit tests for state_specific_supervision_delegate default function implementations."""

    def setUp(self) -> None:
        self.supervision_delegate = UsXxSupervisionDelegate()

    def test_supervision_location_from_supervision_site(self) -> None:
        (
            level_1,
            level_2,
        ) = self.supervision_delegate.supervision_location_from_supervision_site("1")
        self.assertEqual(level_1, "1")
        self.assertEqual(level_2, None)

    def test_supervision_period_is_out_of_state_with_identifier(self) -> None:
        self.assertFalse(
            self.supervision_delegate.is_supervision_location_out_of_state(
                self.create_population_event(
                    "US_XX", "INTERSTATE PROBATION - remainder of identifier"
                )
            )
        )

    def test_supervision_period_is_out_of_state_with_incorrect_identifier(
        self,
    ) -> None:
        self.assertFalse(
            self.supervision_delegate.is_supervision_location_out_of_state(
                self.create_population_event(
                    "US_XX", "Incorrect - remainder of identifier"
                )
            )
        )

    @staticmethod
    def create_population_event(
        state_code: str, supervising_district_external_id: Optional[str]
    ) -> SupervisionPopulationEvent:
        return SupervisionPopulationEvent(
            state_code=state_code,
            year=2010,
            month=1,
            event_date=date(2010, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id=supervising_district_external_id,
            projected_end_date=None,
        )

    @parameterized.expand(
        [
            ("low", 19, "0-23"),
            ("med", 27, "24-29"),
            ("high", 30, "30-38"),
            ("max", 39, "39+"),
        ]
    )
    def test_lsir_score_bucket(self, _name: str, score: int, bucket: str) -> None:
        self.assertEqual(
            self.supervision_delegate.set_lsir_assessment_score_bucket(
                assessment_score=score,
                assessment_level=None,
            ),
            bucket,
        )

    def test_get_projected_completion_date_supervision_period_not_in_sentence(
        self,
    ) -> None:
        """The supervision periods extends past the supervision sentence projected
        completion date, but is not associated with the supervision sentence. However,
        it should still be considered past the completion date,"""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            # Termination date is after sentence's projected completion date
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
        )

        self.assertEqual(
            self.supervision_delegate.get_projected_completion_date(
                supervision_period=supervision_period,
                supervision_sentences=[supervision_sentence],
                incarceration_sentences=[],
            ),
            supervision_sentence.projected_completion_date,
        )

    def test_get_projected_completion_date_no_supervision_sentence(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        self.assertIsNone(
            self.supervision_delegate.get_projected_completion_date(
                supervision_period=supervision_period,
                supervision_sentences=[],
                incarceration_sentences=[],
            )
        )

    def test_get_projected_completion_date_supervision_period_in_supervision_sentence(
        self,
    ) -> None:
        """The supervision periods extends past the supervision sentence projected
        completion date, and is associated with the supervision sentence."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            # Termination date is after sentence's projected completion date
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
        )

        self.assertEqual(
            self.supervision_delegate.get_projected_completion_date(
                supervision_period=supervision_period,
                supervision_sentences=[supervision_sentence],
                incarceration_sentences=[],
            ),
            supervision_sentence.projected_completion_date,
        )

    def test_get_projected_completion_date_supervision_period_in_incarceration_sentence(
        self,
    ) -> None:
        """The supervision periods extends past the supervision sentence projected
        completion date, and is associated with the incarceration sentence."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            # Termination date is after sentence's projected completion date
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            start_date=date(2018, 5, 1),
            projected_max_release_date=date(2018, 5, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(
            self.supervision_delegate.get_projected_completion_date(
                supervision_period=supervision_period,
                incarceration_sentences=[incarceration_sentence],
                supervision_sentences=[],
            ),
            incarceration_sentence.projected_max_release_date,
        )

    def test_get_projected_completion_date_supervision_period_in_incarceration_and_supervision_sentence(
        self,
    ) -> None:
        """The supervision period is within the bounds of both a supervision and
        incarceration sentence, but it extends past the supervision sentence projected
        completion date, and does not extend past the incarceration sentence's projected
        release date."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            # Termination date is after sentence's projected completion date
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
        )

        # The supervision period is within the bounds of the incarceration sentence, and because its projected max
        # release date is after the supervision sentence's, it will be used to determine whether the event date is past
        # the projected end date.
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            start_date=date(2018, 5, 1),
            projected_max_release_date=date(2018, 5, 20),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(
            self.supervision_delegate.get_projected_completion_date(
                supervision_period=supervision_period,
                incarceration_sentences=[incarceration_sentence],
                supervision_sentences=[supervision_sentence],
            ),
            incarceration_sentence.projected_max_release_date,
        )
