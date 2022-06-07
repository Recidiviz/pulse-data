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
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
)
from recidiviz.calculator.pipeline.utils.execution_utils import TableRow
from recidiviz.calculator.pipeline.utils.identifier_models import Span
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_delegate import (
    UsXxIncarcerationDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StatePerson,
)

_DEFAULT_IP_ID = 123

_DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION = [
    {"incarceration_period_id": _DEFAULT_IP_ID, "judicial_district_code": "NW"}
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
    ) -> List[Span]:
        """Helper for testing the identify function on the identifier."""
        entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            NormalizedStateIncarcerationPeriod.base_class_name(): incarceration_periods
            or [],
            "incarceration_period_judicial_district_association": incarceration_period_judicial_district_association
            or _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION,
        }
        required_delegates: Dict[str, StateSpecificDelegate] = {
            "StateSpecificIncarcerationDelegate": incarceration_delegate
            or UsXxIncarcerationDelegate(),
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

    def test_find_incarceration_spans_none(self) -> None:
        population_spans = self._run_find_population_spans([])

        self.assertEqual([], population_spans)

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
