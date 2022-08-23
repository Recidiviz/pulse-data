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
from typing import Any, Dict, List, Optional, Sequence, Union

from recidiviz.calculator.pipeline.metrics.population_spans import identifier
from recidiviz.calculator.pipeline.metrics.population_spans.spans import (
    IncarcerationPopulationSpan,
    SupervisionPopulationSpan,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.calculator.pipeline.utils.execution_utils import TableRow
from recidiviz.calculator.pipeline.utils.identifier_models import Span
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_delegate import (
    UsXxIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StatePerson,
)

_DEFAULT_IP_ID = 123

_DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION = [
    {"incarceration_period_id": _DEFAULT_IP_ID, "judicial_district_code": "NW"}
]

_DEFAULT_SP_ID = 234

_DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION = [
    {"supervision_period_id": _DEFAULT_SP_ID, "judicial_district_code": "NW"}
]

_DEFAULT_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION = [
    {"supervision_period_id": _DEFAULT_SP_ID, "agent_external_id": "TODDB"}
]


class TestNotInStateIncarcerationDelegate(StateSpecificIncarcerationDelegate):
    def is_period_included_in_state_population(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> bool:
        return False


class TestFindPopulationSpans(unittest.TestCase):
    """Tests the identify function."""

    def setUp(self) -> None:
        self.identifier = identifier.PopulationSpanIdentifier()
        self.person = StatePerson.new_with_defaults(
            state_code="US_XX", person_id=99000123
        )

    def _run_find_population_spans(
        self,
        incarceration_periods: Optional[
            List[NormalizedStateIncarcerationPeriod]
        ] = None,
        incarceration_delegate: Optional[StateSpecificIncarcerationDelegate] = None,
        incarceration_period_judicial_district_association: Optional[
            List[Dict[str, Any]]
        ] = None,
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]] = None,
        supervision_delegate: Optional[StateSpecificSupervisionDelegate] = None,
        supervision_period_judicial_district_association: Optional[
            List[Dict[str, Any]]
        ] = None,
        supervision_period_to_agent_association: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Span]:
        """Helper for testing the identify function on the identifier."""
        entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            NormalizedStateIncarcerationPeriod.base_class_name(): incarceration_periods
            or [],
            "incarceration_period_judicial_district_association": incarceration_period_judicial_district_association
            or _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION,
            NormalizedStateSupervisionPeriod.base_class_name(): supervision_periods
            or [],
            "supervision_period_judicial_district_association": supervision_period_judicial_district_association
            or _DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION,
            "supervision_period_to_agent_association": supervision_period_to_agent_association
            or _DEFAULT_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION,
        }
        required_delegates: Dict[str, StateSpecificDelegate] = {
            "StateSpecificIncarcerationDelegate": incarceration_delegate
            or UsXxIncarcerationDelegate(),
            "StateSpecificSupervisionDelegate": supervision_delegate
            or UsXxSupervisionDelegate(),
        }

        all_kwargs: Dict[
            str, Union[Sequence[Entity], List[TableRow], StateSpecificDelegate]
        ] = {**required_delegates, **entity_kwargs}
        return self.identifier.identify(self.person, all_kwargs)

    def test_find_incarceration_spans(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
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
                judicial_district_code="NW",
            )
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_incarceration_spans_multiple(self) -> None:
        incarceration_period_1 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
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

        incarceration_period_2 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
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
                judicial_district_code="NW",
            ),
            expected_incarceration_span(
                incarceration_period_2,
            ),
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_incarceration_spans_out_of_state(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
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

        spans = self._run_find_population_spans(
            incarceration_periods=[incarceration_period],
            incarceration_delegate=TestNotInStateIncarcerationDelegate(),
        )

        expected_spans = [
            expected_incarceration_span(
                incarceration_period,
                included_in_state_population=False,
            ),
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_supervision_spans(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2009, 11, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2015, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
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
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
            )
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_supervision_spans_multiple(self) -> None:
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2009, 11, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2015, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )
        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2015, 2, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_date=date(2020, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
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
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
            ),
            expected_supervision_span(
                supervision_period_2,
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
            ),
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_no_spans_none(self) -> None:
        population_spans = self._run_find_population_spans([])

        self.assertEqual([], population_spans)

    def test_find_both_types_of_spans(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
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

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2009, 11, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2015, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
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
                judicial_district_code="NW",
            ),
            expected_supervision_span(
                supervision_period,
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
            ),
        ]

        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_with_overlaps(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
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

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2008, 12, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2015, 2, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
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
                judicial_district_code="NW",
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2008, 12, 20),
                end_date_exclusive=date(2009, 1, 4),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=False,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2009, 1, 4),
                end_date_exclusive=date(2015, 2, 3),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=True,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_with_overlap_open_incarceration_and_supervision(
        self,
    ) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
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

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2008, 8, 20),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=None,
            termination_reason=None,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
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
                judicial_district_code="NW",
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2008, 8, 20),
                end_date_exclusive=date(2008, 11, 20),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=True,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2008, 11, 20),
                end_date_exclusive=None,
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=False,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_overlapping_consecutive_incarceration_periods(
        self,
    ) -> None:
        incarceration_period_1 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
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

        incarceration_period_2 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
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

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2010, 12, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2012, 1, 1),
            termination_reason=None,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
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
                judicial_district_code="NW",
            ),
            expected_incarceration_span(incarceration_period_2),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2010, 12, 1),
                end_date_exclusive=date(2011, 3, 1),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=False,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2011, 3, 1),
                end_date_exclusive=date(2012, 1, 1),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=True,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_multiple_incarceration_periods_during_supervision(
        self,
    ) -> None:
        incarceration_period_1 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
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

        incarceration_period_2 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
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

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2010, 12, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2012, 1, 1),
            termination_reason=None,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
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
                judicial_district_code="NW",
            ),
            expected_incarceration_span(incarceration_period_2),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2010, 12, 1),
                end_date_exclusive=date(2011, 1, 1),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=True,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2011, 1, 1),
                end_date_exclusive=date(2011, 3, 1),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=False,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2011, 3, 1),
                end_date_exclusive=date(2011, 5, 1),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=True,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2011, 5, 1),
                end_date_exclusive=date(2011, 7, 1),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=False,
            ),
            expected_supervision_span(
                supervision_period,
                start_date_inclusive=date(2011, 7, 1),
                end_date_exclusive=date(2012, 1, 1),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=True,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_both_types_of_spans_multiple_supervision_periods_during_incarceration(
        self,
    ) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 3, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )
        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=2222,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 5, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
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
                judicial_district_code="NW",
            ),
            expected_supervision_span(
                supervision_period_1,
                start_date_inclusive=date(2011, 1, 1),
                end_date_exclusive=date(2011, 3, 1),
                judicial_district_code="NW",
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=False,
            ),
            expected_supervision_span(
                supervision_period_2,
                start_date_inclusive=date(2011, 5, 1),
                end_date_exclusive=date(2011, 7, 1),
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                included_in_state_population=False,
            ),
        ]
        self.assertEqual(expected_spans, spans)

    def test_find_all_supervision_spans_for_dual_periods_to_be_expanded(self) -> None:
        dual_supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 3, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
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
                judicial_district_code="NW",
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
            ),
            expected_supervision_span(
                dual_supervision_period,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                judicial_district_code="NW",
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
            ),
            expected_supervision_span(
                dual_supervision_period,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                judicial_district_code="NW",
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
            ),
        ]

        self.assertEqual(expected_spans, spans)

    class TestsXxSupervisionDelegate(UsXxSupervisionDelegate):
        def supervision_types_mutually_exclusive(self) -> bool:
            return True

    def test_find_all_supervision_spans_for_dual_periods_to_be_converted(self) -> None:
        parole_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 3, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )
        parole_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=2222,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 3, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 5, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=2,
        )
        probation_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=3333,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 2, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 4, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=1,
        )
        dual_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=4444,
            supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 5, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=3,
        )
        probation_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=5555,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            state_code="US_XX",
            supervision_site="SUPERVISION SITE 3",
            start_date=date(2011, 6, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_date=date(2011, 8, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=4,
        )

        spans = self._run_find_population_spans(
            supervision_delegate=self.TestsXxSupervisionDelegate(),
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
                judicial_district_code="NW",
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 1, 1),
                end_date_exclusive=date(2011, 2, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            expected_supervision_span(
                parole_period_1,
                judicial_district_code="NW",
                supervising_officer_external_id="TODDB",
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 2, 1),
                end_date_exclusive=date(2011, 3, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                probation_period_1,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 2, 1),
                end_date_exclusive=date(2011, 3, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                probation_period_1,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 3, 1),
                end_date_exclusive=date(2011, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                parole_period_2,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 3, 1),
                end_date_exclusive=date(2011, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                parole_period_2,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 4, 1),
                end_date_exclusive=date(2011, 5, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            expected_supervision_span(
                dual_period_1,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 5, 1),
                end_date_exclusive=date(2011, 6, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                dual_period_1,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 6, 1),
                end_date_exclusive=date(2011, 7, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                probation_period_2,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 6, 1),
                end_date_exclusive=date(2011, 7, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            ),
            expected_supervision_span(
                probation_period_2,
                level_1_supervision_location_external_id="SUPERVISION SITE 3",
                case_type=StateSupervisionCaseType.GENERAL,
                start_date_inclusive=date(2011, 7, 1),
                end_date_exclusive=date(2011, 8, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
        ]

        self.assertEqual(expected_spans, spans)


def expected_incarceration_span(
    incarceration_period: NormalizedStateIncarcerationPeriod,
    judicial_district_code: Optional[str] = None,
    included_in_state_population: bool = True,
    custodial_authority: Optional[StateCustodialAuthority] = None,
) -> IncarcerationPopulationSpan:
    if incarceration_period.admission_date is None:
        raise ValueError(
            f"Can't create expected span for period [{incarceration_period.incarceration_period_id}] with missing admission date."
        )
    return IncarcerationPopulationSpan(
        state_code=incarceration_period.state_code,
        facility=incarceration_period.facility,
        start_date_inclusive=incarceration_period.admission_date,
        end_date_exclusive=incarceration_period.release_date,
        judicial_district_code=judicial_district_code,
        purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration
        or StateSpecializedPurposeForIncarceration.GENERAL,
        included_in_state_population=included_in_state_population,
        custodial_authority=custodial_authority,
    )


def expected_supervision_span(
    supervision_period: NormalizedStateSupervisionPeriod,
    start_date_inclusive: Optional[date] = None,
    end_date_exclusive: Optional[date] = None,
    judicial_district_code: Optional[str] = None,
    case_type: Optional[StateSupervisionCaseType] = None,
    supervising_officer_external_id: Optional[str] = None,
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
        judicial_district_code=judicial_district_code,
        case_type=case_type,
        custodial_authority=supervision_period.custodial_authority,
        supervising_officer_external_id=supervising_officer_external_id,
        supervising_district_external_id=level_1_supervision_location_external_id,
        level_1_supervision_location_external_id=level_1_supervision_location_external_id,
        level_2_supervision_location_external_id=level_2_supervision_location_external_id,
        supervision_type=supervision_type or supervision_period.supervision_type,
        supervision_level=supervision_period.supervision_level,
        supervision_level_raw_text=supervision_period.supervision_level_raw_text,
        included_in_state_population=included_in_state_population,
    )
