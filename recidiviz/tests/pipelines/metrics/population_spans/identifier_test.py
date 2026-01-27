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
"""Tests for population_spans/identifier.py."""
import unittest
from datetime import date
from typing import Dict, List, Optional, Sequence, Set, Type, Union
from unittest.mock import patch

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import StateEthnicity
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStatePerson,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.metrics.population_spans import identifier
from recidiviz.pipelines.metrics.population_spans import (
    identifier as population_spans_identifier,
)
from recidiviz.pipelines.metrics.population_spans.spans import (
    IncarcerationPopulationSpan,
    SupervisionPopulationSpan,
)
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.pipelines.utils.identifier_models import IdentifierResult, Span
from recidiviz.pipelines.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.tests.pipelines.fake_state_calculation_config_manager import (
    start_pipeline_delegate_getter_patchers,
)

_DEFAULT_IP_ID = 123

_DEFAULT_SP_ID = 234


class TestFindPopulationSpans(unittest.TestCase):
    """Tests the identify function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            population_spans_identifier
        )
        self.identifier = identifier.PopulationSpanIdentifier(StateCode.US_XX)
        self.person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=99000123,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def _run_find_population_spans(
        self,
        incarceration_periods: Optional[
            List[NormalizedStateIncarcerationPeriod]
        ] = None,
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]] = None,
        included_result_classes: Optional[Set[Type[IdentifierResult]]] = None,
    ) -> List[Span]:
        """Helper for testing the identify function on the identifier."""
        all_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            NormalizedStateIncarcerationPeriod.__name__: incarceration_periods or [],
            NormalizedStateSupervisionPeriod.__name__: supervision_periods or [],
        }
        return self.identifier.identify(
            self.person,
            all_kwargs,
            included_result_classes=included_result_classes
            or {IncarcerationPopulationSpan, SupervisionPopulationSpan},
        )

    def test_find_incarceration_spans(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        spans = self._run_find_population_spans(
            incarceration_periods=[incarceration_period],
        )

        expected_spans = [
            expected_incarceration_span(
                incarceration_period,
            )
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_incarceration_spans_multiple(self) -> None:
        incarceration_period_1 = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2009, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NA",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2009, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            sequence_num=0,
        )

        incarceration_period_2 = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON 10",
            admission_date=date(2009, 12, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2010, 2, 4),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            sequence_num=1,
        )

        spans = self._run_find_population_spans(
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
        )

        expected_spans = [
            expected_incarceration_span(
                incarceration_period_1,
            ),
            expected_incarceration_span(
                incarceration_period_2,
            ),
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_incarceration_spans_out_of_state(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2009, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NA",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2009, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            sequence_num=0,
        )

        with patch.object(
            StateSpecificIncarcerationDelegate,
            "is_period_included_in_state_population",
            return_value=False,
        ):
            spans = self._run_find_population_spans(
                incarceration_periods=[incarceration_period],
            )

        expected_spans = [
            expected_incarceration_span(
                incarceration_period,
                included_in_state_population=False,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_supervision_spans(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2009, 11, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2015, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )

        spans = self._run_find_population_spans(
            supervision_periods=[supervision_period]
        )

        expected_spans = [
            expected_supervision_span(
                supervision_period,
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
            )
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_supervision_spans_multiple(self) -> None:
        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2009, 11, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2015, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )
        supervision_period_2 = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            external_id="sp2",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2015, 2, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_date=date(2020, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=1,
        )

        spans = self._run_find_population_spans(
            supervision_periods=[supervision_period_1, supervision_period_2]
        )

        expected_spans = [
            expected_supervision_span(
                supervision_period_1,
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
            ),
            expected_supervision_span(
                supervision_period_2,
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
            ),
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_no_spans_none(self) -> None:
        population_spans = self._run_find_population_spans([])

        self.assertEqual([], population_spans)

    def test_find_both_types_of_spans(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2009, 11, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2015, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )

        spans = self._run_find_population_spans(
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
        )

        expected_spans = [
            expected_incarceration_span(
                incarceration_period,
            ),
            expected_supervision_span(
                supervision_period,
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
            ),
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_filtered(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2009, 11, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2015, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )

        spans = self._run_find_population_spans(
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
            included_result_classes={IncarcerationPopulationSpan},
        )

        expected_spans = [
            expected_incarceration_span(
                incarceration_period,
            ),
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_with_overlaps(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2008, 12, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2015, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )

        spans = self._run_find_population_spans(
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
        )
        print(spans)

        expected_spans = [
            expected_incarceration_span(
                incarceration_period,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2008, 12, 20),
                end_date_exclusive=date(2009, 1, 4),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=False,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2009, 1, 4),
                end_date_exclusive=date(2015, 2, 3),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=True,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_with_overlap_open_incarceration_and_supervision(
        self,
    ) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=None,
            release_reason=None,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2008, 8, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=None,
            termination_reason=None,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )

        spans = self._run_find_population_spans(
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
        )

        expected_spans = [
            expected_incarceration_span(
                incarceration_period,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2008, 8, 20),
                end_date_exclusive=date(2008, 11, 20),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=True,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2008, 11, 20),
                end_date_exclusive=None,
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=False,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_overlapping_consecutive_incarceration_periods(
        self,
    ) -> None:
        incarceration_period_1 = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2010, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2011, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            sequence_num=0,
        )

        incarceration_period_2 = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2011, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2011, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            sequence_num=1,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2010, 12, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2012, 1, 1),
            termination_reason=None,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )

        spans = self._run_find_population_spans(
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            supervision_periods=[supervision_period],
        )

        expected_spans = [
            expected_incarceration_span(
                incarceration_period_1,
            ),
            expected_incarceration_span(incarceration_period_2),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2010, 12, 1),
                end_date_exclusive=date(2011, 3, 1),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=False,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2011, 3, 1),
                end_date_exclusive=date(2012, 1, 1),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=True,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_multiple_incarceration_periods_during_supervision(
        self,
    ) -> None:
        incarceration_period_1 = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2011, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2011, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            sequence_num=0,
        )

        incarceration_period_2 = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2011, 5, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2011, 7, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            sequence_num=1,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2010, 12, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2012, 1, 1),
            termination_reason=None,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )

        spans = self._run_find_population_spans(
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            supervision_periods=[supervision_period],
        )

        expected_spans = [
            expected_incarceration_span(
                incarceration_period_1,
            ),
            expected_incarceration_span(incarceration_period_2),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2010, 12, 1),
                end_date_exclusive=date(2011, 1, 1),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=True,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2011, 1, 1),
                end_date_exclusive=date(2011, 3, 1),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=False,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2011, 3, 1),
                end_date_exclusive=date(2011, 5, 1),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=True,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2011, 5, 1),
                end_date_exclusive=date(2011, 7, 1),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=False,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2011, 7, 1),
                end_date_exclusive=date(2012, 1, 1),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=True,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_multiple_supervision_periods_during_incarceration(
        self,
    ) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2010, 12, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2012, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            sequence_num=0,
        )
        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 3, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )
        supervision_period_2 = NormalizedStateSupervisionPeriod(
            supervision_period_id=2222,
            external_id="sp2",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 5, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=1,
        )

        spans = self._run_find_population_spans(
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period_1, supervision_period_2],
        )

        expected_spans = [
            expected_incarceration_span(
                incarceration_period,
            ),
            expected_supervision_span(
                supervision_period_1,
                start_date_inclusive=date(2011, 1, 1),
                end_date_exclusive=date(2011, 3, 1),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=False,
            ),
            expected_supervision_span(
                supervision_period_2,
                start_date_inclusive=date(2011, 5, 1),
                end_date_exclusive=date(2011, 7, 1),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                included_in_state_population=False,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_all_supervision_spans_for_dual_periods_to_be_expanded(self) -> None:
        dual_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 3, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )

        spans = self._run_find_population_spans(
            incarceration_periods=[],
            supervision_periods=[dual_supervision_period],
        )

        expected_spans = [
            expected_supervision_span(
                dual_supervision_period,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
            ),
            expected_supervision_span(
                dual_supervision_period,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
            ),
            expected_supervision_span(
                dual_supervision_period,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
            ),
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_all_supervision_spans_for_dual_periods_to_be_converted(self) -> None:
        parole_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 3, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )
        parole_period_2 = NormalizedStateSupervisionPeriod(
            supervision_period_id=2222,
            external_id="sp2",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 3, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 5, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=2,
        )
        probation_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=3333,
            external_id="sp3",
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 2, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 4, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=1,
        )
        dual_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=4444,
            external_id="sp4",
            supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 5, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=3,
        )
        probation_period_2 = NormalizedStateSupervisionPeriod(
            supervision_period_id=5555,
            external_id="sp5",
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 6, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 8, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=4,
        )

        with patch.object(
            StateSpecificSupervisionDelegate,
            "supervision_types_mutually_exclusive",
            return_value=True,
        ):
            spans = self._run_find_population_spans(
                incarceration_periods=[],
                supervision_periods=[
                    parole_period_1,
                    parole_period_2,
                    probation_period_1,
                    dual_period_1,
                    probation_period_2,
                ],
            )

        expected_spans = [
            expected_supervision_span(
                parole_period_1,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 1, 1),
                end_date_exclusive=date(2011, 2, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            expected_supervision_span(
                parole_period_1,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 2, 1),
                end_date_exclusive=date(2011, 3, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                probation_period_1,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 2, 1),
                end_date_exclusive=date(2011, 3, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                probation_period_1,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 3, 1),
                end_date_exclusive=date(2011, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                parole_period_2,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 3, 1),
                end_date_exclusive=date(2011, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                parole_period_2,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 4, 1),
                end_date_exclusive=date(2011, 5, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            expected_supervision_span(
                dual_period_1,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 5, 1),
                end_date_exclusive=date(2011, 6, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                dual_period_1,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 6, 1),
                end_date_exclusive=date(2011, 7, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                probation_period_2,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 6, 1),
                end_date_exclusive=date(2011, 7, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                probation_period_2,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                level_2_supervision_location_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 7, 1),
                end_date_exclusive=date(2011, 8, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
        ]

        self.assertEqual(expected_spans, spans)


def expected_incarceration_span(
    incarceration_period: NormalizedStateIncarcerationPeriod,
    included_in_state_population: bool = True,
    custodial_authority: Optional[StateCustodialAuthority] = None,
) -> IncarcerationPopulationSpan:
    if incarceration_period.admission_date is None:
        raise ValueError(
            f"Can't create expected span for period [{incarceration_period.incarceration_period_id}] with missing admission date."
        )
    return IncarcerationPopulationSpan(
        state_code=incarceration_period.state_code,
        incarceration_type=incarceration_period.incarceration_type,
        facility=incarceration_period.facility,
        start_date_inclusive=incarceration_period.admission_date,
        end_date_exclusive=incarceration_period.release_date,
        purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration
        or StateSpecializedPurposeForIncarceration.GENERAL,
        included_in_state_population=included_in_state_population,
        custodial_authority=custodial_authority,
    )


def expected_supervision_span(
    supervision_period: NormalizedStateSupervisionPeriod,
    start_date_inclusive: Optional[date] = None,
    end_date_exclusive: Optional[date] = None,
    case_type: Optional[StateSupervisionCaseType] = None,
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = None,
    level_1_supervision_location_external_id: Optional[str] = None,
    level_2_supervision_location_external_id: Optional[str] = None,
    included_in_state_population: bool = True,
) -> SupervisionPopulationSpan:
    if supervision_period.start_date is None:
        raise ValueError(
            f"Can't create expected span for period [{supervision_period.supervision_period_id}] with missing start date."
        )
    return SupervisionPopulationSpan(
        state_code=supervision_period.state_code,
        start_date_inclusive=start_date_inclusive or supervision_period.start_date,
        end_date_exclusive=end_date_exclusive or supervision_period.termination_date,
        case_type=case_type,
        custodial_authority=supervision_period.custodial_authority,
        supervising_officer_staff_id=supervision_period.supervising_officer_staff_id,
        level_1_supervision_location_external_id=level_1_supervision_location_external_id,
        level_2_supervision_location_external_id=level_2_supervision_location_external_id,
        supervision_type=supervision_type or supervision_period.supervision_type,
        supervision_level=supervision_period.supervision_level,
        supervision_level_raw_text=supervision_period.supervision_level_raw_text,
        included_in_state_population=included_in_state_population,
    )
