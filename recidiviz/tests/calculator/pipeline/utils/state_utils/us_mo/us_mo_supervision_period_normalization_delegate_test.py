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
"""Tests for the us_mo_supervision_period_normalization_delegate.py file"""
# pylint: disable=protected-access
import datetime
import unittest
from datetime import date
from typing import List, Optional

import attr
import mock

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateSupervisionSentence,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    clear_entity_id_index_cache,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    SupervisionTypeSpan,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_period_normalization_delegate import (
    UsMoSupervisionNormalizationDelegate,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
)


class TestUsMoSupervisionPeriodNormalizationDelegate(unittest.TestCase):
    """Unit tests for UsMoSupervisionPeriodNormalizationDelegate"""

    def setUp(self) -> None:
        self.person_id = 290000700089

        clear_entity_id_index_cache()
        self.unique_id_patcher = mock.patch(
            "recidiviz.calculator.pipeline.normalization.utils."
            "normalized_entities_utils._fixed_length_object_id_for_entity"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 12345

        self.validation_date = datetime.date(year=2019, month=10, day=31)

    @classmethod
    def _get_overlapping_supervision_type_span_index(
        cls,
        supervision_type_spans: List[SupervisionTypeSpan],
        supervision_type_day: datetime.date,
    ) -> Optional[int]:
        """Returns the index of the span in this sentence's supervision_type_spans list that overlaps in time with the
        provided date, or None if there are no overlapping spans."""
        filtered_spans = [
            (i, span)
            for i, span in enumerate(supervision_type_spans)
            if span.start_date <= supervision_type_day
            and (span.end_date is None or supervision_type_day < span.end_date)
        ]

        if not filtered_spans:
            return None

        if len(filtered_spans) > 1:
            raise ValueError("Should have non-overlapping supervision type spans")

        return filtered_spans[0][0]

    @classmethod
    def get_sentence_supervision_type_on_day(
        cls,
        supervision_type_spans: List[SupervisionTypeSpan],
        supervision_type_day: datetime.date,
    ) -> Optional[StateSupervisionSentenceSupervisionType]:
        """Calculates the supervision type to be associated with this sentence on a given day, or None if the sentence
        has been completed/terminated, if the person is incarcerated on this date, or if there are no statuses for this
        person on/before a given date.
        """

        overlapping_span_index = cls._get_overlapping_supervision_type_span_index(
            supervision_type_spans, supervision_type_day
        )

        if overlapping_span_index is None:
            return None

        if supervision_type_spans[overlapping_span_index].supervision_type is None:
            return None

        while overlapping_span_index >= 0:
            span = supervision_type_spans[overlapping_span_index]
            if (
                span.supervision_type is not None
                and span.supervision_type
                != StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN
            ):
                return span.supervision_type

            # If the most recent status status is INTERNAL_UNKNOWN, we look back at previous statuses until we can
            # find a status that is not INTERNAL_UNKNOWN
            overlapping_span_index -= 1

        return supervision_type_spans[overlapping_span_index].supervision_type

    def test_split_periods_based_on_sentences(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            external_id="sp1",
            state_code="US_MO",
            start_date=date(2020, 9, 1),
            termination_date=date(2020, 10, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.PARDONED,
            supervision_type=None,
            case_type_entries=[
                StateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=9,
                    state_code="US_MO",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
        )

        supervision_sentence = NormalizedStateSupervisionSentence.new_with_defaults(
            state_code="US_MO",
            supervision_sentence_id=111,
            effective_date=date(2020, 9, 1),
            completion_date=date(2020, 10, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
        )

        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-1",
                    "status_code": "40O1010",
                    "status_date": "20200901",
                    "status_description": "Parole Release",
                },
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-2",
                    "status_code": "99O2010",
                    "status_date": "20200915",
                    "status_description": "Parole Discharge",
                },
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-3",
                    "status_code": "15I1000",
                    "status_date": "20200915",
                    "status_description": "New Court Probation",
                },
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-4",
                    "status_code": "99O1000",
                    "status_date": "20201001",
                    "status_description": "Court Probation Discharge",
                },
            ]
        )

        expected_periods = [
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 1),
                termination_date=date(2020, 9, 15),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
                admission_reason_raw_text="40O1010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="15I1000,99O2010",
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=29000070008912345,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry(
                        supervision_case_type_entry_id=29000070008912345,
                        state_code="US_MO",
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                    )
                ],
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 15),
                termination_date=date(2020, 10, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                admission_reason_raw_text="15I1000,99O2010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="99O1000",
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=29000070008912346,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry(
                        supervision_case_type_entry_id=29000070008912346,
                        state_code="US_MO",
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                    )
                ],
            ),
        ]

        for sp in expected_periods:
            for cte in sp.case_type_entries:
                cte.supervision_period = sp

        results = delegate.split_periods_based_on_sentences(
            person_id=self.person_id,
            supervision_periods=[supervision_period],
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
        )
        self.assertEqual(expected_periods, results)

    def test_split_periods_based_on_sentences_no_periods(self) -> None:
        supervision_sentence = NormalizedStateSupervisionSentence.new_with_defaults(
            state_code="US_MO",
            supervision_sentence_id=111,
            effective_date=date(2020, 9, 1),
            completion_date=date(2020, 10, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
        )
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-1",
                    "status_code": "40O1010",
                    "status_date": "20200901",
                    "status_description": "Parole Release",
                },
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-2",
                    "status_code": "99O2010",
                    "status_date": "20200915",
                    "status_description": "Parole Discharge",
                },
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-3",
                    "status_code": "15I1000",
                    "status_date": "20200915",
                    "status_description": "New Court Probation",
                },
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-4",
                    "status_code": "99O1000",
                    "status_date": "20201001",
                    "status_description": "Court Probation Discharge",
                },
            ],
        )

        expected_periods = [
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 1),
                termination_date=date(2020, 9, 15),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
                admission_reason_raw_text="40O1010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="15I1000,99O2010",
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=29000070008912345,
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 15),
                termination_date=date(2020, 10, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                admission_reason_raw_text="15I1000,99O2010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="99O1000",
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=29000070008912346,
            ),
        ]

        results = delegate.split_periods_based_on_sentences(
            person_id=self.person_id,
            supervision_periods=[],
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
        )

        self.assertEqual(expected_periods, results)

    def test_split_periods_based_on_sentences_no_end_date(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            external_id="sp1",
            state_code="US_MO",
            start_date=date(2020, 9, 1),
            termination_date=None,
            termination_reason=StateSupervisionPeriodTerminationReason.PARDONED,
            supervision_type=None,
        )

        supervision_sentence = NormalizedStateSupervisionSentence.new_with_defaults(
            state_code="US_MO",
            supervision_sentence_id=111,
            effective_date=date(2020, 9, 1),
            completion_date=date(2020, 10, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
        )
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-1",
                    "status_code": "40O1010",
                    "status_date": "20200901",
                    "status_description": "Parole Release",
                },
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-2",
                    "status_code": "99O2010",
                    "status_date": "20200915",
                    "status_description": "Parole Discharge",
                },
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-3",
                    "status_code": "15I1000",
                    "status_date": "20200915",
                    "status_description": "New Court Probation",
                },
            ],
        )

        expected_periods = [
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 1),
                termination_date=date(2020, 9, 15),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
                admission_reason_raw_text="40O1010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="15I1000,99O2010",
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=29000070008912345,
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 15),
                termination_date=None,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                admission_reason_raw_text="15I1000,99O2010",
                termination_reason=None,
                termination_reason_raw_text=None,
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=29000070008912346,
            ),
        ]

        results = delegate.split_periods_based_on_sentences(
            person_id=self.person_id,
            supervision_periods=[supervision_period],
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
        )

        self.assertEqual(expected_periods, results)

    def test_split_periods_based_on_sentences_absconsion(self) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            external_id="sp1",
            state_code="US_MO",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            admission_reason_raw_text="15I1000",
            start_date=date(2020, 9, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            termination_reason_raw_text="65L9100",
            termination_date=date(2020, 9, 15),
            supervision_type=None,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=2,
            external_id="sp1",
            state_code="US_MO",
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            admission_reason_raw_text="65L9100",
            start_date=date(2020, 9, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.SUSPENSION,
            termination_reason_raw_text="65O2015",
            termination_date=date(2020, 10, 1),
            supervision_type=None,
        )

        supervision_sentence = NormalizedStateSupervisionSentence.new_with_defaults(
            state_code="US_MO",
            supervision_sentence_id=111,
            effective_date=date(2020, 9, 1),
            completion_date=date(2020, 10, 1),
            external_id="ss1",
            status=StateSentenceStatus.SUSPENDED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
        )
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-1",
                    "status_code": "15I1000",
                    "status_date": "20200901",
                    "status_description": "New Court Probation",
                },
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-2",
                    "status_code": "65L9100",
                    "status_date": "20200915",
                    "status_description": "Offender Declared Absconder",
                },
                {
                    "sentence_external_id": supervision_sentence.external_id,
                    "sentence_status_external_id": f"{supervision_sentence.external_id}-3",
                    "status_code": "65O2015",
                    "status_date": "20201001",
                    "status_description": "Court Probation Suspension",
                },
            ],
        )

        expected_periods = [
            attr.evolve(
                supervision_period_1,
                external_id=None,
                supervision_period_id=29000070008912345,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
            attr.evolve(
                supervision_period_2,
                external_id=None,
                supervision_period_id=29000070008912346,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
        ]

        results = delegate.split_periods_based_on_sentences(
            person_id=self.person_id,
            supervision_periods=[supervision_period_1, supervision_period_2],
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
        )

        self.assertEqual(expected_periods, results)

    def test_supervision_type_new_probation(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1345495-20190808-1",
                    "sentence_status_external_id": "1345495-20190808-1-1",
                    "status_code": "15I1000",
                    "status_date": "20190808",
                    "status_description": "New Court Probation",
                }
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1345495-20190808-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_supervision_type_parole(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1167633-20171012-2",
                    "sentence_status_external_id": "1167633-20171012-2-1",
                    "status_code": "10I1000",
                    "status_date": "20171012",
                    "status_description": "New Court Comm-Institution",
                },
                {
                    "sentence_external_id": "1167633-20171012-2",
                    "sentence_status_external_id": "1167633-20171012-2-2",
                    "status_code": "40O1010",
                    "status_date": "20190913",
                    "status_description": "Parole Release",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1167633-20171012-2"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

    def test_supervision_type_court_parole(self) -> None:
        # Court Parole is actually probation
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1344959-20190718-1",
                    "sentence_status_external_id": "1344959-20190718-1-1",
                    "status_code": "15I1200",
                    "status_date": "20190718",
                    "status_description": "New Court Parole",
                }
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1344959-20190718-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_supervision_type_conditional_release_cr(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "505542-20120927-1",
                    "sentence_status_external_id": "505542-20120927-1-1",
                    "status_code": "15I1000",
                    "status_date": "20150808",
                    "status_description": "New Court Probation",
                },
                {
                    "sentence_external_id": "505542-20120927-1",
                    "sentence_status_external_id": "505542-20120927-1-11",
                    "status_code": "40I2300",
                    "status_date": "20151105",
                    "status_description": "Prob Rev Ret-Technical",
                },
                {
                    "sentence_external_id": "505542-20120927-1",
                    "sentence_status_external_id": "505542-20120927-1-12",
                    "status_code": "45O2000",
                    "status_date": "20151105",
                    "status_description": "Prob Rev-Technical",
                },
                {
                    "sentence_external_id": "505542-20120927-1",
                    "sentence_status_external_id": "505542-20120927-1-13",
                    "status_code": "40O3020",
                    "status_date": "20180527",
                    "status_description": "CR To Custody/Detainer",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["505542-20120927-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

    def test_supervision_type_board_holdover_release(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1333144-20180912-1",
                    "sentence_status_external_id": "1333144-20180912-1-1",
                    "status_code": "10I1000",
                    "status_date": "20180912",
                    "status_description": "New Court Comm-Institution",
                },
                {
                    "sentence_external_id": "1333144-20180912-1",
                    "sentence_status_external_id": "1333144-20180912-1-2",
                    "status_code": "40O1010",
                    "status_date": "20190131",
                    "status_description": "Parole Release",
                },
                {
                    "sentence_external_id": "1333144-20180912-1",
                    "sentence_status_external_id": "1333144-20180912-1-5",
                    "status_code": "45O0050",
                    "status_date": "20191003",
                    "status_description": "Board Holdover",
                },
                {
                    "sentence_external_id": "1333144-20180912-1",
                    "sentence_status_external_id": "1333144-20180912-1-4",
                    "status_code": "40I0050",
                    "status_date": "20191003",
                    "status_description": "Board Holdover",
                },
                {
                    "sentence_external_id": "1333144-20180912-1",
                    "sentence_status_external_id": "1333144-20180912-1-7",
                    "status_code": "40O0050",
                    "status_date": "20191029",
                    "status_description": "Board Holdover Release",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1333144-20180912-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

    def test_supervision_type_lifetime_supervision(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "13252-20160627-1",
                    "sentence_status_external_id": "13252-20160627-1-1",
                    "status_code": "10I1000",
                    "status_date": "20160627",
                    "status_description": "New Court Comm-Institution",
                },
                {
                    "sentence_external_id": "13252-20160627-1",
                    "sentence_status_external_id": "13252-20160627-1-2",
                    "status_code": "90O1070",
                    "status_date": "20190415",
                    "status_description": "Director's Rel Comp-Life Supv",
                },
                {
                    "sentence_external_id": "13252-20160627-1",
                    "sentence_status_external_id": "13252-20160627-1-3",
                    "status_code": "40O6020",
                    "status_date": "20190415",
                    "status_description": "Release for Lifetime Supv",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["13252-20160627-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

    def test_supervision_type_lifetime_supervision_after_inst_completion(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "13252-20160627-1",
                    "sentence_status_external_id": "13252-20160627-1-1",
                    "status_code": "10I1000",
                    "status_date": "20160627",
                    "status_description": "New Court Comm-Institution",
                },
                {
                    "sentence_external_id": "13252-20160627-1",
                    "sentence_status_external_id": "13252-20160627-1-2",
                    "status_code": "90O1010",
                    "status_date": "20190415",
                    "status_description": "Inst. Expiration of Sentence",
                },
                {
                    "sentence_external_id": "13252-20160627-1",
                    "sentence_status_external_id": "13252-20160627-1-3",
                    "status_code": "40O6020",
                    "status_date": "20190415",
                    "status_description": "Release for Lifetime Supv",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["13252-20160627-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

    def test_lifetime_supervision_no_supervision_in(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1096616-20060515-3",
                    "sentence_status_external_id": "1096616-20060515-3-5",
                    "status_code": "20I1000",
                    "status_date": "20090611",
                    "status_description": "Court Comm-Inst-Addl Charge",
                },
                {
                    "sentence_external_id": "1096616-20060515-3",
                    "sentence_status_external_id": "1096616-20060515-3-10",
                    "status_code": "90O1070",
                    "status_date": "20151129",
                    "status_description": "Director's Rel Comp-Life Supv",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1096616-20060515-3"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

    def test_probation_after_investigation_status_list_unsorted(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "282443-20180427-1",
                    "sentence_status_external_id": "282443-20180427-1-3",
                    "status_code": "35I1000",
                    "status_date": "20180525",
                    "status_description": "Court Probation-Revisit",
                },
                {
                    "sentence_external_id": "282443-20180427-1",
                    "sentence_status_external_id": "282443-20180427-1-2",
                    "status_code": "95O5630",
                    "status_date": "20180525",
                    "status_description": "SAR Cancelled by Court",
                },
                {
                    "sentence_external_id": "282443-20180427-1",
                    "sentence_status_external_id": "282443-20180427-1-1",
                    "status_code": "05I5600",
                    "status_date": "20180427",
                    "status_description": "New Sentencing Assessment",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["282443-20180427-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_diversion_probation_after_investigation(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1324786-20180214-1",
                    "sentence_status_external_id": "1324786-20180214-1-1",
                    "status_code": "05I5500",
                    "status_date": "20180214",
                    "status_description": "New Diversion Investigation",
                },
                {
                    "sentence_external_id": "1324786-20180214-1",
                    "sentence_status_external_id": "1324786-20180214-1-2",
                    "status_code": "95O5500",
                    "status_date": "20180323",
                    "status_description": "Diversion Invest Completed",
                },
                {
                    "sentence_external_id": "1324786-20180214-1",
                    "sentence_status_external_id": "1324786-20180214-1-3",
                    "status_code": "35I2000",
                    "status_date": "20180323",
                    "status_description": "Diversion Supv-Revisit",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1324786-20180214-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

        # Also test that we count the person as on probation if we are looking at the exact day they started this
        # supervision.
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1324786-20180214-1"],
                datetime.date(year=2018, month=3, day=23),
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_diversion_probation_after_community_court_ref_investigation(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1324786-20180214-1",
                    "sentence_status_external_id": "1324786-20180214-1-1",
                    "status_code": "05I5100",
                    "status_date": "20180214",
                    "status_description": "New Community Corr Court Ref",
                },
                {
                    "sentence_external_id": "1324786-20180214-1",
                    "sentence_status_external_id": "1324786-20180214-1-2",
                    "status_code": "95O5100",
                    "status_date": "20180323",
                    "status_description": "Comm Corr Court Ref Closed",
                },
                {
                    "sentence_external_id": "1324786-20180214-1",
                    "sentence_status_external_id": "1324786-20180214-1-3",
                    "status_code": "35I2000",
                    "status_date": "20180323",
                    "status_description": "Diversion Supv-Revisit",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1324786-20180214-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_interstate_compact_parole_classified_as_probation(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "165467-20171227-1",
                    "sentence_status_external_id": "165467-20171227-1-1",
                    "status_code": "05I5200",
                    "status_date": "20171227",
                    "status_description": "New Interstate Compact-Invest",
                },
                {
                    "sentence_external_id": "165467-20171227-1",
                    "sentence_status_external_id": "165467-20171227-1-2",
                    "status_code": "95O5200",
                    "status_date": "20180123",
                    "status_description": "Interstate Invest Closed",
                },
                {
                    "sentence_external_id": "165467-20171227-1",
                    "sentence_status_external_id": "165467-20171227-1-3",
                    "status_code": "35I4100",
                    "status_date": "20180129",
                    "status_description": "IS Compact-Parole-Revisit",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["165467-20171227-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_interstate_compact_parole_classified_as_probation_2(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1269010-20140403-1",
                    "sentence_status_external_id": "1269010-20140403-1-1",
                    "status_code": "10I4000",
                    "status_date": "20140403",
                    "status_description": "New Interstate Compact-Inst",
                },
                {
                    "sentence_external_id": "1269010-20140403-1",
                    "sentence_status_external_id": "1269010-20140403-1-2",
                    "status_code": "40O7400",
                    "status_date": "20151118",
                    "status_description": "IS Compact Parole to Missouri",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1269010-20140403-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_probation_starts_same_day_as_new_investigation(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1344336-20190703-1",
                    "sentence_status_external_id": "1344336-20190703-1-1",
                    "status_code": "05I5210",
                    "status_date": "20190703",
                    "status_description": "IS Comp-Reporting Instr Given",
                },
                {
                    "sentence_external_id": "1344336-20190703-1",
                    "sentence_status_external_id": "1344336-20190703-1-2",
                    "status_code": "95O5210",
                    "status_date": "20190716",
                    "status_description": "IS Comp-Report Instruct Closed",
                },
                {
                    "sentence_external_id": "1344336-20190703-1",
                    "sentence_status_external_id": "1344336-20190703-1-3",
                    "status_code": "35I5200",
                    "status_date": "20190716",
                    "status_description": "IS Compact-Invest-Revisit",
                },
                {
                    "sentence_external_id": "1344336-20190703-1",
                    "sentence_status_external_id": "1344336-20190703-1-4",
                    "status_code": "95O5200",
                    "status_date": "20190716",
                    "status_description": "Interstate Invest Closed",
                },
                {
                    "sentence_external_id": "1344336-20190703-1",
                    "sentence_status_external_id": "1344336-20190703-1-5",
                    "status_code": "35I4000",
                    "status_date": "20190716",
                    "status_description": "IS Compact-Prob-Revisit",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1344336-20190703-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_resentenced_probation_revisit(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1254438-20130418-2",
                    "sentence_status_external_id": "1254438-20130418-2-2",
                    "status_code": "25I1000",
                    "status_date": "20140610",
                    "status_description": "Court Probation-Addl Charge",
                },
                {
                    "sentence_external_id": "1254438-20130418-2",
                    "sentence_status_external_id": "1254438-20130418-2-8",
                    "status_code": "95O1040",
                    "status_date": "20170717",
                    "status_description": "Resentenced",
                },
                {
                    "sentence_external_id": "1254438-20130418-2",
                    "sentence_status_external_id": "1254438-20130418-2-9",
                    "status_code": "35I1000",
                    "status_date": "20170717",
                    "status_description": "Court Probation-Revisit",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1254438-20130418-2"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_release_to_field_other_sentence(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1328840-20180523-3",
                    "sentence_status_external_id": "1328840-20180523-3-2",
                    "status_code": "25I1000",
                    "status_date": "20180523",
                    "status_description": "Court Probation-Addl Charge",
                },
                {
                    "sentence_external_id": "1328840-20180523-3",
                    "sentence_status_external_id": "1328840-20180523-3-3",
                    "status_code": "40I7000",
                    "status_date": "20181011",
                    "status_description": "Field Supv to DAI-Oth Sentence",
                },
                {
                    "sentence_external_id": "1328840-20180523-3",
                    "sentence_status_external_id": "1328840-20180523-3-4",
                    "status_code": "45O7000",
                    "status_date": "20181011",
                    "status_description": "Field to DAI-Other Sentence",
                },
                {
                    "sentence_external_id": "1328840-20180523-3",
                    "sentence_status_external_id": "1328840-20180523-3-5",
                    "status_code": "40O7000",
                    "status_date": "20181017",
                    "status_description": "Rel to Field-DAI Other Sent",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1328840-20180523-3"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_prob_rev_codes_not_applicable(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1163420-20180116-1",
                    "sentence_status_external_id": "1163420-20180116-1-1",
                    "status_code": "15I1000",
                    "status_date": "20180116",
                    "status_description": "New Court Probation",
                },
                {
                    "sentence_external_id": "1163420-20180116-1",
                    "sentence_status_external_id": "1163420-20180116-1-3",
                    "status_code": "95O2120",
                    "status_date": "20180925",
                    "status_description": "Prob Rev-Codes Not Applicable",
                },
                {
                    "sentence_external_id": "1163420-20180116-1",
                    "sentence_status_external_id": "1163420-20180116-1-4",
                    "status_code": "35I1000",
                    "status_date": "20180925",
                    "status_description": "Court Probation-Revisit",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1163420-20180116-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_incarcerated_on_date(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "13252-20160627-1",
                    "sentence_status_external_id": "13252-20160627-1-1",
                    "status_code": "10I1000",
                    "status_date": "20160627",
                    "status_description": "New Court Comm-Institution",
                }
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["13252-20160627-1"],
                self.validation_date,
            ),
            None,
        )

    def test_suspended_and_reinstated(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1001298-20160310-1",
                    "sentence_status_external_id": "1001298-20160310-1-1",
                    "status_code": "15I1000",
                    "status_date": "20160310",
                    "status_description": "New Court Probation",
                },
                {
                    "sentence_external_id": "1001298-20160310-1",
                    "sentence_status_external_id": "1001298-20160310-1-2",
                    "status_code": "65O2015",
                    "status_date": "20160712",
                    "status_description": "Court Probation Suspension",
                },
                {
                    "sentence_external_id": "1001298-20160310-1",
                    "sentence_status_external_id": "1001298-20160310-1-3",
                    "status_code": "65I2015",
                    "status_date": "20180726",
                    "status_description": "Court Probation Reinstated",
                },
                {
                    "sentence_external_id": "1001298-20160310-1",
                    "sentence_status_external_id": "1001298-20160310-1-4",
                    "status_code": "65O2015",
                    "status_date": "20191030",
                    "status_description": "Court Probation Suspension",
                },
                {
                    "sentence_external_id": "1001298-20160310-1",
                    "sentence_status_external_id": "1001298-20160310-1-5",
                    "status_code": "99O1000",
                    "status_date": "20200220",
                    "status_description": "Court Probation Discharge",
                },
            ]
        )

        # Suspension - treated same as termination
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1001298-20160310-1"],
                datetime.date(2016, 7, 12),
            ),
            None,
        )

        # Suspension end - back on probation
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1001298-20160310-1"],
                datetime.date(2018, 7, 26),
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

        # Suspension #2 - treated same as termination
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1001298-20160310-1"],
                datetime.date(2019, 10, 30),
            ),
            None,
        )

        # Actual discharge
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1001298-20160310-1"],
                datetime.date(2020, 2, 20),
            ),
            None,
        )

    def test_release_to_field_other_sentence_lookback(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1061945-20030505-7",
                    "sentence_status_external_id": "1061945-20030505-7-26",
                    "status_code": "35I1000",
                    "status_date": "20180716",
                    "status_description": "Court Probation-Revisit",
                },
                {
                    "sentence_external_id": "1061945-20030505-7",
                    "sentence_status_external_id": "1061945-20030505-7-28",
                    "status_code": "40I7000",
                    "status_date": "20180716",
                    "status_description": "Field Supv to DAI-Oth Sentence",
                },
                {
                    "sentence_external_id": "1061945-20030505-7",
                    "sentence_status_external_id": "1061945-20030505-7-30",
                    "status_code": "40O7000",
                    "status_date": "20180816",
                    "status_description": "Rel to Field-DAI Other Sent",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1061945-20030505-7"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_release_to_field_statuses_cancel_each_other_out(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1061945-20030505-7",
                    "sentence_status_external_id": "1061945-20030505-7-26",
                    "status_code": "35I1000",
                    "status_date": "20180716",
                    "status_description": "Court Probation-Revisit",
                },
                # These three statuses below all happened in the same day and represent a commitment and
                # release to supervision.
                {
                    "sentence_external_id": "1061945-20030505-7",
                    "sentence_status_external_id": "1061945-20030505-7-28",
                    "status_code": "40I7000",
                    "status_date": "20180716",
                    "status_description": "Field Supv to DAI-Oth Sentence",
                },
                {
                    "sentence_external_id": "1061945-20030505-7",
                    "sentence_status_external_id": "1061945-20030505-7-29",
                    "status_code": "45O7000",
                    "status_date": "20180716",
                    "status_description": "Field to DAI-Other Sentence",
                },
                {
                    "sentence_external_id": "1061945-20030505-7",
                    "sentence_status_external_id": "1061945-20030505-7-30",
                    "status_code": "40O7000",
                    "status_date": "20180716",
                    "status_description": "Rel to Field-DAI Other Sent",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1061945-20030505-7"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_interstate_transfer_not_on_supervision(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1343861-20190620-2",
                    "sentence_status_external_id": "1343861-20190620-2-2",
                    "status_code": "25I1000",
                    "status_date": "20190710",
                    "status_description": "Court Probation-Addl Charge",
                },
                {
                    "sentence_external_id": "1343861-20190620-2",
                    "sentence_status_external_id": "1343861-20190620-2-3",
                    "status_code": "75O3000",
                    "status_date": "20190814",
                    "status_description": "MO Field-Interstate Transfer",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1343861-20190620-2"],
                self.validation_date,
            ),
            None,
        )

    def test_interstate_transfer_same_day_as_new_charge(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1343861-20190620-2",
                    "sentence_status_external_id": "1343861-20190620-2-2",
                    "status_code": "25I1000",
                    "status_date": "20190710",
                    "status_description": "Court Probation-Addl Charge",
                },
                {
                    "sentence_external_id": "1343861-20190620-2",
                    "sentence_status_external_id": "1343861-20190620-2-3",
                    "status_code": "75O3000",
                    "status_date": "20190710",
                    "status_description": "MO Field-Interstate Transfer",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1343861-20190620-2"],
                self.validation_date,
            ),
            None,
        )

    def test_probation_reinstated_on_validation_date(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1313746-20170505-1",
                    "sentence_status_external_id": "1313746-20170505-1-1",
                    "status_code": "15I1000",
                    "status_date": "20170505",
                    "status_description": "New Court Probation",
                },
                {
                    "sentence_external_id": "1313746-20170505-1",
                    "sentence_status_external_id": "1313746-20170505-1-2",
                    "status_code": "65O2015",
                    "status_date": "20191001",
                    "status_description": "Court Probation Suspension",
                },
                {
                    "sentence_external_id": "1313746-20170505-1",
                    "sentence_status_external_id": "1313746-20170505-1-3",
                    "status_code": "65I2015",
                    "status_date": "20191031",
                    "status_description": "Court Probation Reinstated",
                },
                {
                    "sentence_external_id": "1313746-20170505-1",
                    "sentence_status_external_id": "1313746-20170505-1-4",
                    "status_code": "99O1011",
                    "status_date": "20200201",
                    "status_description": "Ct Prob ECC Disc-CONFIDENTIAL",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1313746-20170505-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_conditional_release(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1123534-20041220-5",
                    "sentence_status_external_id": "1123534-20041220-5-18",
                    "status_code": "20I1000",
                    "status_date": "20130603",
                    "status_description": "Court Comm-Inst-Addl Charge",
                },
                {
                    "sentence_external_id": "1123534-20041220-5",
                    "sentence_status_external_id": "1123534-20041220-5-21",
                    "status_code": "40O3010",
                    "status_date": "20180525",
                    "status_description": "Conditional Release",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1123534-20041220-5"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

    def test_conditional_re_release(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1123534-20041220-5",
                    "sentence_status_external_id": "1123534-20041220-5-18",
                    "status_code": "20I1000",
                    "status_date": "20130603",
                    "status_description": "Court Comm-Inst-Addl Charge",
                },
                {
                    "sentence_external_id": "1123534-20041220-5",
                    "sentence_status_external_id": "1123534-20041220-5-21",
                    "status_code": "40O3010",
                    "status_date": "20180525",
                    "status_description": "Conditional Release",
                },
                {
                    "sentence_external_id": "1123534-20041220-5",
                    "sentence_status_external_id": "1123534-20041220-5-31",
                    "status_code": "40I3060",
                    "status_date": "20190509",
                    "status_description": "CR Ret-Treatment Center",
                },
                {
                    "sentence_external_id": "1123534-20041220-5",
                    "sentence_status_external_id": "1123534-20041220-5-32",
                    "status_code": "45O3060",
                    "status_date": "20190509",
                    "status_description": "CR Ret-Treatment Center",
                },
                {
                    "sentence_external_id": "1123534-20041220-5",
                    "sentence_status_external_id": "1123534-20041220-5-34",
                    "status_code": "40O3030",
                    "status_date": "20191022",
                    "status_description": "Conditional Re-Release",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1123534-20041220-5"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

    def test_interstate_transfer_and_return_same_day(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1291992-20151103-1",
                    "sentence_status_external_id": "1291992-20151103-1-3",
                    "status_code": "35I1000",
                    "status_date": "20160105",
                    "status_description": "Court Probation-Revisit",
                },
                {
                    "sentence_external_id": "1291992-20151103-1",
                    "sentence_status_external_id": "1291992-20151103-1-5",
                    "status_code": "75I3000",
                    "status_date": "20160111",
                    "status_description": "MO Field-Interstate Returned",
                },
                {
                    "sentence_external_id": "1291992-20151103-1",
                    "sentence_status_external_id": "1291992-20151103-1-4",
                    "status_code": "75O3000",
                    "status_date": "20160111",
                    "status_description": "MO Field-Interstate Transfer",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1291992-20151103-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_crc_converted_from_dai_to_parole(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "38140-19800131-8",
                    "sentence_status_external_id": "38140-19800131-8-1",
                    "status_code": "10I1000",
                    "status_date": "19800131",
                    "status_description": "New Court Comm-Institution",
                },
                {
                    "sentence_external_id": "38140-19800131-8",
                    "sentence_status_external_id": "38140-19800131-8-8",
                    "status_code": "40O4099",
                    "status_date": "19950918",
                    "status_description": "Inmate Release to RF",
                },
                {
                    "sentence_external_id": "38140-19800131-8",
                    "sentence_status_external_id": "38140-19800131-8-18",
                    "status_code": "40N1010",
                    "status_date": "20020220",
                    "status_description": "Parole Assigned To CRC",
                },
                {
                    "sentence_external_id": "38140-19800131-8",
                    "sentence_status_external_id": "38140-19800131-8-27",
                    "status_code": "40O6000",
                    "status_date": "20080701",
                    "status_description": "Converted-CRC DAI to CRC Field",
                },
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["38140-19800131-8"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

    def test_no_statuses(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate([])

        # Initial commit day
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1000044-20100920-1"],
                datetime.date(2012, 1, 25),
            ),
            None,
        )

    def test_no_previous_supervision(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1000044-20100920-1",
                    "sentence_status_external_id": "1000044-20100920-1-6",
                    "status_code": "10I1000",
                    "status_date": "20120125",
                    "status_description": "New Court Comm-Institution",
                },
            ]
        )

        # Initial commit day
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1000044-20100920-1"],
                datetime.date(2012, 1, 25),
            ),
            None,
        )

    def test_board_hold_revocation(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                {
                    "sentence_external_id": "1000044-20100920-1",
                    "sentence_status_external_id": "1000044-20100920-1-6",
                    "status_code": "10I1000",
                    "status_date": "20120125",
                    "status_description": "New Court Comm-Institution",
                },
                {
                    "sentence_external_id": "1000044-20100920-1",
                    "sentence_status_external_id": "1000044-20100920-1-7",
                    "status_code": "40O1010",
                    "status_date": "20150507",
                    "status_description": "Parole Release",
                },
                {
                    "sentence_external_id": "1000044-20100920-1",
                    "sentence_status_external_id": "1000044-20100920-1-9",
                    "status_code": "40I0050",
                    "status_date": "20171108",
                    "status_description": "Board Holdover",
                },
                {
                    "sentence_external_id": "1000044-20100920-1",
                    "sentence_status_external_id": "1000044-20100920-1-10",
                    "status_code": "45O0050",
                    "status_date": "20171108",
                    "status_description": "Board Holdover",
                },
                {
                    "sentence_external_id": "1000044-20100920-1",
                    "sentence_status_external_id": "1000044-20100920-1-12",
                    "status_code": "50N1010",
                    "status_date": "20171130",
                    "status_description": "Parole Update-Tech Viol",
                },
                {
                    "sentence_external_id": "1000044-20100920-1",
                    "sentence_status_external_id": "1000044-20100920-1-13",
                    "status_code": "40O1010",
                    "status_date": "20180330",
                    "status_description": "Parole Release",
                },
                {
                    "sentence_external_id": "1000044-20100920-1",
                    "sentence_status_external_id": "1000044-20100920-1-13",
                    "status_code": "95O2010",
                    "status_date": "20190120",
                    "status_description": "Parole Completion",
                },
            ]
        )

        # Supervision type BEFORE first day of first stint on Parole is None
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1000044-20100920-1"],
                datetime.date(2015, 5, 6),
            ),
            None,
        )

        # Supervision type on starting day of first stint on Parole is PAROLE
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1000044-20100920-1"],
                datetime.date(2015, 5, 7),
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

        # After Board Holdover day
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1000044-20100920-1"],
                datetime.date(2017, 11, 8),
            ),
            None,
        )

        # Parole Update day
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1000044-20100920-1"],
                datetime.date(2017, 11, 30),
            ),
            None,
        )

        # Second stint on parole
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1000044-20100920-1"],
                datetime.date(2018, 3, 31),
            ),
            StateSupervisionSentenceSupervisionType.PAROLE,
        )

        # After sentence completion
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1000044-20100920-1"],
                datetime.date(2019, 1, 31),
            ),
            None,
        )
