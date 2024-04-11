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

from parameterized import parameterized

from recidiviz.calculator.query.state.views.reference.supervision_location_ids_to_names import (
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
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
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationSentence,
    NormalizedStateSupervisionSentence,
)
from recidiviz.pipelines.utils.state_utils import state_calculation_config_manager
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)


class TestStateSpecificSupervisionDelegate(unittest.TestCase):
    """Unit tests for state_specific_supervision_delegate default function implementations."""

    def setUp(self) -> None:
        self.default_supervision_delegate = UsXxSupervisionDelegate()

    def test_no_new_supervision_location_from_supervision_site_override(self) -> None:
        allowed_override_states = [StateCode.US_PA]
        for state_code in StateCode:
            if state_code in allowed_override_states:
                continue
            try:
                # pylint: disable=protected-access
                delegate = state_calculation_config_manager._get_state_specific_supervision_delegate(
                    state_code.value
                )
                if (
                    delegate.supervision_location_from_supervision_site
                    is not StateSpecificSupervisionDelegate.supervision_location_from_supervision_site
                ):
                    raise ValueError(
                        f"Found override of supervision_location_from_supervision_site() "
                        f"on the StateSpecificSupervisionDelegate for state "
                        f"[{state_code.value}]. States should make every possible "
                        f"effort to avoid state-specific logic here. For most states, "
                        f"the level_2_supervision_location_external_id is derived in "
                        f"the most_recent_dataflow_metrics_* views by joining with the "
                        f"{SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME} view."
                    )
            except ValueError:
                # No delegate for this state
                continue

    def test_supervision_location_from_supervision_site(self) -> None:
        (
            level_1,
            level_2,
        ) = self.default_supervision_delegate.supervision_location_from_supervision_site(
            "1"
        )
        self.assertEqual(level_1, "1")
        self.assertEqual(level_2, None)

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
            self.default_supervision_delegate.set_lsir_assessment_score_bucket(
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

        supervision_sentence = NormalizedStateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            effective_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
        )

        self.assertEqual(
            self.default_supervision_delegate.get_projected_completion_date(
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
            self.default_supervision_delegate.get_projected_completion_date(
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

        supervision_sentence = NormalizedStateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            effective_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
        )

        self.assertEqual(
            self.default_supervision_delegate.get_projected_completion_date(
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

        incarceration_sentence = NormalizedStateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            effective_date=date(2018, 5, 1),
            projected_max_release_date=date(2018, 5, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(
            self.default_supervision_delegate.get_projected_completion_date(
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

        supervision_sentence = NormalizedStateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            effective_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
        )

        # The supervision period is within the bounds of the incarceration sentence, and because its projected max
        # release date is after the supervision sentence's, it will be used to determine whether the event date is past
        # the projected end date.
        incarceration_sentence = NormalizedStateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            effective_date=date(2018, 5, 1),
            projected_max_release_date=date(2018, 5, 20),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(
            self.default_supervision_delegate.get_projected_completion_date(
                supervision_period=supervision_period,
                incarceration_sentences=[incarceration_sentence],
                supervision_sentences=[supervision_sentence],
            ),
            incarceration_sentence.projected_max_release_date,
        )
