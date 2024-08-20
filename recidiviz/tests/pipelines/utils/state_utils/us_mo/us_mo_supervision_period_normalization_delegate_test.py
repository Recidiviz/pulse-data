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

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSentence,
    StateSentenceStatusSnapshot,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_sentence_classification import (
    SupervisionTypeSpan,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_supervision_period_normalization_delegate import (
    UsMoSupervisionNormalizationDelegate,
)


class TestUsMoSupervisionPeriodNormalizationDelegate(unittest.TestCase):
    """Unit tests for UsMoSupervisionPeriodNormalizationDelegate"""

    def setUp(self) -> None:
        self.person_id = 2900000000000000123

        self.unique_id_patcher = mock.patch(
            "recidiviz.persistence.entity."
            "normalized_entities_utils.generate_primary_key"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 2900000000012312345

        self.validation_date = datetime.date(year=2019, month=10, day=31)

    def tearDown(self) -> None:
        self.unique_id_patcher.stop()

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
            supervising_officer_staff_external_id="ABCDE",
            supervising_officer_staff_external_id_type="MO_STAFF",
        )

        sentence = StateSentence(
            state_code="US_MO",
            external_id="ss1",
            sentence_status_snapshots=[
                StateSentenceStatusSnapshot(
                    sequence_num=1,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 1),
                    status=StateSentenceStatus.SERVING,
                    status_raw_text='40O1010@@"PAROLE RELEASE "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
                StateSentenceStatusSnapshot(
                    sequence_num=2,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 15),
                    status=StateSentenceStatus.COMPLETED,
                    status_raw_text='99O2010@@"PAROLE DISCHARGE "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
                StateSentenceStatusSnapshot(
                    sequence_num=3,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 15),
                    status=StateSentenceStatus.SERVING,
                    status_raw_text='15I1000@@"NEW COURT PROBATION "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
                StateSentenceStatusSnapshot(
                    sequence_num=4,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 10, 1),
                    status=StateSentenceStatus.COMPLETED,
                    status_raw_text='99O1000@@"COURT PROBATION DISCHARGE "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
            ],
        )

        delegate = UsMoSupervisionNormalizationDelegate([sentence])

        expected_periods = [
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                external_id=f"{self.person_id}-0-NORMALIZED",
                start_date=date(2020, 9, 1),
                termination_date=date(2020, 9, 15),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
                admission_reason_raw_text="40O1010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="15I1000,99O2010",
                supervision_site=None,
                supervision_period_id=2900000000012312345,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry(
                        supervision_case_type_entry_id=2900000000012312345,
                        state_code="US_MO",
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                    )
                ],
                supervising_officer_staff_external_id="ABCDE",
                supervising_officer_staff_external_id_type="MO_STAFF",
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                external_id=f"{self.person_id}-1-NORMALIZED",
                start_date=date(2020, 9, 15),
                termination_date=date(2020, 10, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                admission_reason_raw_text="15I1000,99O2010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="99O1000",
                supervision_site=None,
                supervision_period_id=2900000000012312345,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry(
                        supervision_case_type_entry_id=2900000000012312345,
                        state_code="US_MO",
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                    )
                ],
                supervising_officer_staff_external_id="ABCDE",
                supervising_officer_staff_external_id_type="MO_STAFF",
            ),
        ]

        for sp in expected_periods:
            for cte in sp.case_type_entries:
                cte.supervision_period = sp

        results = delegate.split_periods_based_on_sentences(
            person_id=self.person_id,
            supervision_periods=[supervision_period],
        )
        self.assertEqual(expected_periods, results)

    def test_split_periods_based_on_sentences_no_periods(self) -> None:
        sentence = StateSentence(
            state_code="US_MO",
            external_id="ss1",
            sentence_status_snapshots=[
                StateSentenceStatusSnapshot(
                    sequence_num=1,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 1),
                    status=StateSentenceStatus.SERVING,
                    status_raw_text='40O1010@@"PAROLE RELEASE "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
                StateSentenceStatusSnapshot(
                    sequence_num=2,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 15),
                    status=StateSentenceStatus.COMPLETED,
                    status_raw_text='99O2010@@"PAROLE DISCHARGE "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
                StateSentenceStatusSnapshot(
                    sequence_num=3,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 15),
                    status=StateSentenceStatus.SERVING,
                    status_raw_text='15I1000@@"NEW COURT PROBATION "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
                StateSentenceStatusSnapshot(
                    sequence_num=4,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 10, 1),
                    status=StateSentenceStatus.COMPLETED,
                    status_raw_text='99O1000@@"COURT PROBATION DISCHARGE "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
            ],
        )
        delegate = UsMoSupervisionNormalizationDelegate(
            [sentence],
        )

        expected_periods = [
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                external_id=f"{self.person_id}-0-NORMALIZED",
                start_date=date(2020, 9, 1),
                termination_date=date(2020, 9, 15),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
                admission_reason_raw_text="40O1010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="15I1000,99O2010",
                supervision_site=None,
                supervision_period_id=2900000000012312345,
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                external_id=f"{self.person_id}-1-NORMALIZED",
                start_date=date(2020, 9, 15),
                termination_date=date(2020, 10, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                admission_reason_raw_text="15I1000,99O2010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="99O1000",
                supervision_site=None,
                supervision_period_id=2900000000012312345,
            ),
        ]

        results = delegate.split_periods_based_on_sentences(
            person_id=self.person_id,
            supervision_periods=[],
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
            supervising_officer_staff_external_id="ABCDE",
            supervising_officer_staff_external_id_type="MO_STAFF",
        )

        sentence = StateSentence(
            state_code="US_MO",
            external_id="ss1",
            sentence_status_snapshots=[
                StateSentenceStatusSnapshot(
                    sequence_num=1,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 1),
                    status=StateSentenceStatus.SERVING,
                    status_raw_text='40O1010@@"PAROLE RELEASE "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
                StateSentenceStatusSnapshot(
                    sequence_num=2,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 15),
                    status=StateSentenceStatus.COMPLETED,
                    status_raw_text='99O2010@@"PAROLE DISCHARGE "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
                StateSentenceStatusSnapshot(
                    sequence_num=3,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 15),
                    status=StateSentenceStatus.SERVING,
                    status_raw_text='15I1000@@"NEW COURT PROBATION "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
            ],
        )
        delegate = UsMoSupervisionNormalizationDelegate(sentences=[sentence])

        expected_periods = [
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                external_id=f"{self.person_id}-0-NORMALIZED",
                start_date=date(2020, 9, 1),
                termination_date=date(2020, 9, 15),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
                admission_reason_raw_text="40O1010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="15I1000,99O2010",
                supervision_site=None,
                supervision_period_id=2900000000012312345,
                supervising_officer_staff_external_id="ABCDE",
                supervising_officer_staff_external_id_type="MO_STAFF",
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                external_id=f"{self.person_id}-1-NORMALIZED",
                start_date=date(2020, 9, 15),
                termination_date=None,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                admission_reason_raw_text="15I1000,99O2010",
                termination_reason=None,
                termination_reason_raw_text=None,
                supervision_site=None,
                supervision_period_id=2900000000012312345,
                supervising_officer_staff_external_id="ABCDE",
                supervising_officer_staff_external_id_type="MO_STAFF",
            ),
        ]

        results = delegate.split_periods_based_on_sentences(
            person_id=self.person_id,
            supervision_periods=[supervision_period],
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
            supervising_officer_staff_external_id="ABCDE",
            supervising_officer_staff_external_id_type="MO_STAFF",
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=2,
            external_id="sp2",
            state_code="US_MO",
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            admission_reason_raw_text="65L9100",
            start_date=date(2020, 9, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.SUSPENSION,
            termination_reason_raw_text="65O2015",
            termination_date=date(2020, 10, 1),
            supervision_type=None,
            supervising_officer_staff_external_id="FGHIJ",
            supervising_officer_staff_external_id_type="MO_STAFF",
        )

        sentence = StateSentence(
            state_code="US_MO",
            external_id="ss1",
            sentence_status_snapshots=[
                StateSentenceStatusSnapshot(
                    sequence_num=1,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 1),
                    status=StateSentenceStatus.SERVING,
                    status_raw_text='15I1000@@"NEW COURT PROBATION "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
                StateSentenceStatusSnapshot(
                    sequence_num=2,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 9, 15),
                    status=StateSentenceStatus.SERVING,
                    status_raw_text='65L9100@@"OFFENDER DECLARED ABSCONDER "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
                StateSentenceStatusSnapshot(
                    sequence_num=3,
                    state_code="US_MO",
                    status_update_datetime=datetime.datetime(2020, 10, 1),
                    status=StateSentenceStatus.SUSPENDED,
                    status_raw_text='65O2015@@"COURT PROBATION SUSPENSION "',
                    sentence_status_snapshot_id=None,
                    person=None,
                    sentence=None,
                ),
            ],
        )
        delegate = UsMoSupervisionNormalizationDelegate(
            [sentence],
        )

        expected_periods = [
            attr.evolve(
                supervision_period_1,
                external_id=f"{self.person_id}-0-NORMALIZED",
                supervision_period_id=2900000000012312345,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
            attr.evolve(
                supervision_period_2,
                external_id=f"{self.person_id}-1-NORMALIZED",
                supervision_period_id=2900000000012312345,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
        ]

        results = delegate.split_periods_based_on_sentences(
            person_id=self.person_id,
            supervision_periods=[supervision_period_1, supervision_period_2],
        )

        self.assertEqual(expected_periods, results)

    def test_supervision_type_new_probation(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                StateSentence(
                    external_id="1345495-20190808-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 8, 8),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='15I1000@@"NEW COURT PROBATION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1167633-20171012-2",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2017, 10, 12),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='10I1000@@"NEW COURT COMM-INSTITUTION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 9, 13),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O1010@@"PAROLE RELEASE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1344959-20190718-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 7, 18),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='15I1200@@"NEW COURT PAROLE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="505542-20120927-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2015, 8, 8),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='15I1000@@"NEW COURT PROBATION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=11,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2015, 11, 5),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40I2300@@"PROB REV RET-TECHNICAL "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=12,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2015, 11, 5),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='45O2000@@"PROB REV-TECHNICAL "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=13,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 5, 27),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O3020@@"CR TO CUSTODY/DETAINER "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1333144-20180912-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 9, 12),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='10I1000@@"NEW COURT COMM-INSTITUTION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 1, 31),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O1010@@"PAROLE RELEASE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=5,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 10, 3),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='45O0050@@"BOARD HOLDOVER "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=4,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 10, 3),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40I0050@@"BOARD HOLDOVER "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=7,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 10, 29),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O0050@@"BOARD HOLDOVER RELEASE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="13252-20160627-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2016, 6, 27),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='10I1000@@"NEW COURT COMM-INSTITUTION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 4, 15),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='90O1070@@"DIRECTOR\'S REL COMP-LIFE SUPV "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 4, 15),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O6020@@"RELEASE FOR LIFETIME SUPV "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="13252-20160627-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2016, 6, 27),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='10I1000@@"NEW COURT COMM-INSTITUTION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 4, 15),
                            status=StateSentenceStatus.COMPLETED,
                            status_raw_text='90O1010@@"INST. EXPIRATION OF SENTENCE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 4, 15),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O6020@@"RELEASE FOR LIFETIME SUPV "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1096616-20060515-3",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=5,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2009, 6, 11),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='20I1000@@"COURT COMM-INST-ADDL CHARGE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=10,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2015, 11, 29),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='90O1070@@"DIRECTOR\'S REL COMP-LIFE SUPV "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="282443-20180427-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 5, 25),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I1000@@"COURT PROBATION-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 5, 25),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='95O5630@@"SAR CANCELLED BY COURT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 4, 27),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='05I5600@@"NEW SENTENCING ASSESSMENT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1324786-20180214-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 2, 14),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='05I5500@@"NEW DIVERSION INVESTIGATION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 3, 23),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='95O5500@@"DIVERSION INVEST COMPLETED "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 3, 23),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I2000@@"DIVERSION SUPV-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1324786-20180214-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 2, 14),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='05I5100@@"NEW COMMUNITY CORR COURT REF "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 3, 23),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='95O5100@@"COMM CORR COURT REF CLOSED "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 3, 23),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I2000@@"DIVERSION SUPV-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="165467-20171227-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2017, 12, 27),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='05I5200@@"NEW INTERSTATE COMPACT-INVEST "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 1, 23),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='95O5200@@"INTERSTATE INVEST CLOSED "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 1, 29),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I4100@@"IS COMPACT-PAROLE-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1269010-20140403-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2014, 4, 3),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='10I4000@@"NEW INTERSTATE COMPACT-INST "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2015, 11, 18),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O7400@@"IS COMPACT PAROLE TO MISSOURI "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1344336-20190703-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 7, 3),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='05I5210@@"IS COMP-REPORTING INSTR GIVEN "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 7, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='95O5210@@"IS COMP-REPORT INSTRUCT CLOSED "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 7, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I5200@@"IS COMPACT-INVEST-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=4,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 7, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='95O5200@@"INTERSTATE INVEST CLOSED "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=5,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 7, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I4000@@"IS COMPACT-PROB-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1254438-20130418-2",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2014, 6, 10),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='25I1000@@"COURT PROBATION-ADDL CHARGE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=8,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2017, 7, 17),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='95O1040@@"RESENTENCED "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=9,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2017, 7, 17),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I1000@@"COURT PROBATION-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1344336-20190703-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 5, 23),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='25I1000@@"COURT PROBATION-ADDL CHARGE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                ),
                StateSentence(
                    external_id="1328840-20180523-3",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 10, 11),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40I7000@@"FIELD SUPV TO DAI-OTH SENTENCE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=4,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 10, 11),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='45O7000@@"FIELD TO DAI-OTHER SENTENCE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=5,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 10, 17),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O7000@@"REL TO FIELD-DAI OTHER SENT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                ),
            ]
        )

        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1328840-20180523-3"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.INTERNAL_UNKNOWN,
        )
        self.assertEqual(
            self.get_sentence_supervision_type_on_day(
                delegate._supervision_type_spans["1344336-20190703-1"],
                self.validation_date,
            ),
            StateSupervisionSentenceSupervisionType.PROBATION,
        )

    def test_prob_rev_codes_not_applicable(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate(
            [
                StateSentence(
                    external_id="1163420-20180116-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 1, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='15I1000@@"NEW COURT PROBATION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 9, 25),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='95O2120@@"PROB REV-CODES NOT APPLICABLE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=4,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 9, 25),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I1000@@"COURT PROBATION-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="is1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2016, 6, 27),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='10I1000@@"NEW COURT COMM-INSTITUTION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1001298-20160310-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2016, 3, 10),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='15I1000@@"NEW COURT PROBATION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2016, 7, 12),
                            status=StateSentenceStatus.SUSPENDED,
                            status_raw_text='65O2015@@"COURT PROBATION SUSPENSION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 7, 26),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='65I2015@@"COURT PROBATION REINSTATED "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=4,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 10, 30),
                            status=StateSentenceStatus.SUSPENDED,
                            status_raw_text='65O2015@@"COURT PROBATION SUSPENSION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=5,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2020, 2, 20),
                            status=StateSentenceStatus.COMPLETED,
                            status_raw_text='99O1000@@"COURT PROBATION DISCHARGE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1061945-20030505-7",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=26,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 7, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I1000@@"COURT PROBATION-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=28,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 7, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40I7000@@"FIELD SUPV TO DAI-OTH SENTENCE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=30,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 8, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O7000@@"REL TO FIELD-DAI OTHER SENT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1061945-20030505-7",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=26,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 7, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I1000@@"COURT PROBATION-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        # These three statuses below all happened in the same day and represent a commitment and
                        # release to supervision.
                        StateSentenceStatusSnapshot(
                            sequence_num=28,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 7, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40I7000@@"FIELD SUPV TO DAI-OTH SENTENCE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=29,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 7, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='45O7000@@"FIELD TO DAI-OTHER SENTENCE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=30,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 7, 16),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O7000@@"REL TO FIELD-DAI OTHER SENT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1343861-20190620-2",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 7, 10),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='25I1000@@"COURT PROBATION-ADDL CHARGE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 8, 14),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='75O3000@@"MO FIELD-INTERSTATE TRANSFER "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1343861-20190620-2",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 7, 10),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='25I1000@@"COURT PROBATION-ADDL CHARGE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 7, 10),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='75O3000@@"MO FIELD-INTERSTATE TRANSFER "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1313746-20170505-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2017, 5, 5),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='15I1000@@"NEW COURT PROBATION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=2,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 10, 1),
                            status=StateSentenceStatus.SUSPENDED,
                            status_raw_text='65O2015@@"COURT PROBATION SUSPENSION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 10, 31),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='65I2015@@"COURT PROBATION REINSTATED "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=4,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2020, 2, 1),
                            status=StateSentenceStatus.COMPLETED,
                            status_raw_text='99O1011@@"CT PROB ECC DISC-CONFIDENTIAL "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1123534-20041220-5",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=18,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2013, 6, 3),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='20I1000@@"COURT COMM-INST-ADDL CHARGE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=21,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 5, 25),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O3010@@"CONDITIONAL RELEASE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1123534-20041220-5",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=18,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2013, 6, 3),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='20I1000@@"COURT COMM-INST-ADDL CHARGE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=21,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 5, 25),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O3010@@"CONDITIONAL RELEASE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=31,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 5, 9),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40I3060@@"CR RET-TREATMENT CENTER "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=32,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 5, 9),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='45O3060@@"CR RET-TREATMENT CENTER "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=34,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 10, 22),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O3030@@"CONDITIONAL RE-RELEASE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1291992-20151103-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=3,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2016, 1, 5),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='35I1000@@"COURT PROBATION-REVISIT "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=5,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2016, 1, 11),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='75I3000@@"MO FIELD-INTERSTATE RETURNED "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=4,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2016, 1, 11),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='75O3000@@"MO FIELD-INTERSTATE TRANSFER "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="38140-19800131-8",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=1,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(1980, 1, 31),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='10I1000@@"NEW COURT COMM-INSTITUTION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=8,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(1995, 9, 18),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O4099@@"INMATE RELEASE TO RF "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=18,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2002, 2, 20),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40N1010@@"PAROLE ASSIGNED TO CRC "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=27,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2008, 7, 1),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O6000@@"CONVERTED-CRC DAI TO CRC FIELD "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1000044-20100920-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=6,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2012, 1, 25),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='10I1000@@"NEW COURT COMM-INSTITUTION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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
                StateSentence(
                    external_id="1000044-20100920-1",
                    state_code="US_MO",
                    sentence_status_snapshots=[
                        StateSentenceStatusSnapshot(
                            sequence_num=6,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2012, 1, 25),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='10I1000@@"NEW COURT COMM-INSTITUTION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=7,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2015, 5, 7),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O1010@@"PAROLE RELEASE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=9,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2017, 11, 8),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40I0050@@"BOARD HOLDOVER "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=10,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2017, 11, 8),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='45O0050@@"BOARD HOLDOVER "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=12,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2017, 11, 30),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='50N1010@@"PAROLE UPDATE-TECH VIOL "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=13,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2018, 3, 30),
                            status=StateSentenceStatus.SERVING,
                            status_raw_text='40O1010@@"PAROLE RELEASE "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                        StateSentenceStatusSnapshot(
                            sequence_num=13,
                            state_code="US_MO",
                            status_update_datetime=datetime.datetime(2019, 1, 20),
                            status=StateSentenceStatus.COMPLETED,
                            status_raw_text='95O2010@@"PAROLE COMPLETION "',
                            sentence_status_snapshot_id=None,
                            person=None,
                            sentence=None,
                        ),
                    ],
                )
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

    def test_absconsion_overrides_basic(self) -> None:
        # Periods between an absconsion and a return from absconsion should have their
        # supervision type/level set to ABSCONSION.
        delegate = UsMoSupervisionNormalizationDelegate([])

        sp_ending_in_absconsion = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-1",
            start_date=datetime.date(2015, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_date=datetime.date(2016, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_mid_absconsion_no_type_or_level = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-2",
            start_date=datetime.date(2016, 1, 1),
            termination_date=datetime.date(2017, 1, 1),
        )
        sp_mid_absconsion_has_type_and_level = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-3",
            start_date=datetime.date(2017, 1, 1),
            termination_date=datetime.date(2018, 1, 1),
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_starting_in_return = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-4",
            start_date=datetime.date(2018, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            termination_date=datetime.date(2019, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sps = [
            sp_ending_in_absconsion,
            sp_mid_absconsion_no_type_or_level,
            sp_mid_absconsion_has_type_and_level,
            sp_starting_in_return,
        ]

        # Verify that the level/type overrides trigger for these periods, whether this means
        # setting a previously absent StateSupervisionLevel/StateSupervisionPeriodSupervisionType,
        # or overriding an existing value. Because the logic for overriding type and level are
        # essentially the same, the tests that follow will only test supervision_type_override
        # under the assumption that supervision_level_override works the same way.
        override_missing_level = delegate.supervision_level_override(
            supervision_period_list_index=1,
            sorted_supervision_periods=sps,
        )
        override_existing_level = delegate.supervision_level_override(
            supervision_period_list_index=2,
            sorted_supervision_periods=sps,
        )
        override_missing_type = delegate.supervision_type_override(
            supervision_period_list_index=1,
            sorted_supervision_periods=sps,
        )
        override_existing_type = delegate.supervision_type_override(
            supervision_period_list_index=2,
            sorted_supervision_periods=sps,
        )

        self.assertEqual(override_missing_level, StateSupervisionLevel.ABSCONSION)
        self.assertEqual(override_existing_level, StateSupervisionLevel.ABSCONSION)
        self.assertEqual(
            override_missing_type, StateSupervisionPeriodSupervisionType.ABSCONSION
        )
        self.assertEqual(
            override_existing_type, StateSupervisionPeriodSupervisionType.ABSCONSION
        )

    def test_do_not_override(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate([])

        sp_ending_in_absconsion = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-1",
            start_date=datetime.date(2015, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_date=datetime.date(2016, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_return_in_next_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-2",
            start_date=datetime.date(2017, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            termination_date=datetime.date(2018, 1, 1),
        )
        sp_after_absconsion = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-3",
            start_date=datetime.date(2018, 1, 1),
            termination_date=datetime.date(2019, 1, 1),
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        # Here, there is an absconsion start and end with a gap between the bounding periods.
        # This gap will be filled with an inferred absconsion period in normalization, but the
        # bounding periods should still be left alone.
        no_override_absconsion_start = delegate.supervision_type_override(
            supervision_period_list_index=0,
            sorted_supervision_periods=[
                sp_ending_in_absconsion,
                sp_return_in_next_period,
            ],
        )
        no_override_absconsion_end = delegate.supervision_type_override(
            supervision_period_list_index=1,
            sorted_supervision_periods=[
                sp_ending_in_absconsion,
                sp_return_in_next_period,
            ],
        )

        # If there is an absconsion followed by a return and no other absconsions, subsequent
        # periods should be left alone.
        no_override_past_absconsion = delegate.supervision_type_override(
            supervision_period_list_index=2,
            sorted_supervision_periods=[
                sp_ending_in_absconsion,
                sp_return_in_next_period,
                sp_after_absconsion,
            ],
        )

        # We only want to infer supervision type/level for periods between absconsions
        # and returns; this means that no overrides occur for periods that follow absconsions
        # but aren't eventually followed by returns.
        no_override_open_absconsion_span = delegate.supervision_type_override(
            supervision_period_list_index=1,
            sorted_supervision_periods=[
                sp_ending_in_absconsion,
                sp_after_absconsion,
            ],
        )
        self.assertEqual(
            no_override_absconsion_start,
            StateSupervisionPeriodSupervisionType.PROBATION,
        )
        self.assertIsNone(no_override_absconsion_end)
        self.assertEqual(
            no_override_past_absconsion, StateSupervisionPeriodSupervisionType.PROBATION
        )
        self.assertEqual(
            no_override_open_absconsion_span,
            StateSupervisionPeriodSupervisionType.PROBATION,
        )

    def test_multiple_absconsion_events(self) -> None:
        delegate = UsMoSupervisionNormalizationDelegate([])

        sp_ending_in_absconsion_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-1",
            start_date=datetime.date(2014, 1, 1),
            termination_date=datetime.date(2015, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_mid_first_absconsion = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-2",
            start_date=datetime.date(2015, 1, 1),
            termination_date=datetime.date(2015, 6, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_return_between_absconsions = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-3",
            start_date=datetime.date(2015, 6, 1),
            termination_date=datetime.date(2016, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
        )
        sp_ending_in_absconsion_2a = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-4",
            start_date=datetime.date(2016, 1, 1),
            termination_date=datetime.date(2017, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_ending_in_absconsion_2b = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-5",
            start_date=datetime.date(2016, 1, 1),
            termination_date=datetime.date(2017, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_after_absconsions = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-6",
            start_date=datetime.date(2022, 1, 1),
            termination_date=datetime.date(2023, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_starting_in_return = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-7",
            start_date=datetime.date(2023, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
        )

        # Check that there aren't indexing errors when only one period exists.
        only_period_ends_in_absconsion = delegate.supervision_type_override(
            supervision_period_list_index=0,
            sorted_supervision_periods=[sp_ending_in_absconsion_1],
        )

        sps_two_absconsions_before_return = [
            sp_ending_in_absconsion_1,
            sp_mid_first_absconsion,
            sp_ending_in_absconsion_2a,
            sp_after_absconsions,
            sp_starting_in_return,
        ]

        # Check that overrides do not occur for a period that follows an absconsion and
        # precedes a return BUT with another absconsion prior to the return.
        after_absconsion_before_absconsion_and_return = (
            delegate.supervision_type_override(
                supervision_period_list_index=1,
                sorted_supervision_periods=sps_two_absconsions_before_return,
            )
        )
        # Same set of periods as above, but where the period being normalized IS the period ending in
        # the second absconsion, which should still not receive an override.
        absconsion_after_absconsion_before_return = delegate.supervision_type_override(
            supervision_period_list_index=2,
            sorted_supervision_periods=sps_two_absconsions_before_return,
        )

        # Same set of periods as above, but where the period being normalized is the period
        # between the second absconsion and the return, which should receive an override (since
        # there are no absconsions between the period and the return that follows).
        multiple_absconsions_before_period = delegate.supervision_type_override(
            supervision_period_list_index=3,
            sorted_supervision_periods=sps_two_absconsions_before_return,
        )

        # Check that a period between an absconsion and return still receives the override
        # even with absconsions in the future, as long as the return precedes future absconsions.
        absconsion_return_absconsion_return = delegate.supervision_type_override(
            supervision_period_list_index=1,
            sorted_supervision_periods=[
                sp_ending_in_absconsion_1,
                sp_mid_first_absconsion,
                sp_return_between_absconsions,
                sp_ending_in_absconsion_2a,
                sp_after_absconsions,
                sp_starting_in_return,
            ],
        )

        # Further test the sorting and picking logic for past absconsion periods to include
        # perfectly overlapping periods ending in absconsion.
        simultaneous_absconsions_before_period = delegate.supervision_type_override(
            supervision_period_list_index=3,
            sorted_supervision_periods=[
                sp_ending_in_absconsion_1,
                sp_ending_in_absconsion_2a,
                sp_ending_in_absconsion_2b,
                sp_after_absconsions,
                sp_starting_in_return,
            ],
        )
        self.assertEqual(
            only_period_ends_in_absconsion,
            StateSupervisionPeriodSupervisionType.PROBATION,
        )
        self.assertEqual(
            after_absconsion_before_absconsion_and_return,
            StateSupervisionPeriodSupervisionType.PROBATION,
        )
        self.assertEqual(
            absconsion_after_absconsion_before_return,
            StateSupervisionPeriodSupervisionType.PROBATION,
        )
        self.assertEqual(
            multiple_absconsions_before_period,
            StateSupervisionPeriodSupervisionType.ABSCONSION,
        )
        self.assertEqual(
            absconsion_return_absconsion_return,
            StateSupervisionPeriodSupervisionType.ABSCONSION,
        )
        self.assertEqual(
            simultaneous_absconsions_before_period,
            StateSupervisionPeriodSupervisionType.ABSCONSION,
        )

        sp_return_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-8",
            start_date=datetime.date(2017, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            termination_date=datetime.date(2018, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_return_2a = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-9",
            start_date=datetime.date(2018, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            termination_date=datetime.date(2019, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_return_2b = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-10",
            start_date=datetime.date(2018, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            termination_date=datetime.date(2019, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )
        sp_return_and_re_abscond = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            external_id="123-456-1-11",
            start_date=datetime.date(2019, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            termination_date=datetime.date(2020, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        # The next three tests check the same things as the prior tests, but for absconsion
        # returns rather than absconsions themselves.
        only_period_is_return = delegate.supervision_type_override(
            supervision_period_list_index=0,
            sorted_supervision_periods=[sp_return_1],
        )

        multiple_returns_before_period = delegate.supervision_type_override(
            supervision_period_list_index=3,
            sorted_supervision_periods=[
                sp_ending_in_absconsion_1,
                sp_return_1,
                sp_return_2a,
                sp_after_absconsions,
            ],
        )
        simultaneous_returns_before_period = delegate.supervision_type_override(
            supervision_period_list_index=4,
            sorted_supervision_periods=[
                sp_ending_in_absconsion_1,
                sp_return_1,
                sp_return_2a,
                sp_return_2b,
                sp_after_absconsions,
            ],
        )

        # Check that periods starting with absconsion returns and ending with new absconsions
        # can be handled without error, and do not receive overrides.
        absconsion_on_both_ends = delegate.supervision_type_override(
            supervision_period_list_index=0,
            sorted_supervision_periods=[sp_return_and_re_abscond],
        )

        # Check that when a period starts with an absconsion return and ends with an absconsion,
        # it's treated as a normal absconsion for overriding future supervision types/levels.
        override_period_after_re_abscond = delegate.supervision_type_override(
            supervision_period_list_index=2,
            sorted_supervision_periods=[
                sp_ending_in_absconsion_1,
                sp_return_and_re_abscond,
                sp_after_absconsions,
                sp_starting_in_return,
            ],
        )
        no_override_re_abscond_period = delegate.supervision_type_override(
            supervision_period_list_index=1,
            sorted_supervision_periods=[
                sp_ending_in_absconsion_1,
                sp_return_and_re_abscond,
                sp_after_absconsions,
                sp_starting_in_return,
            ],
        )

        self.assertEqual(
            only_period_is_return,
            StateSupervisionPeriodSupervisionType.PROBATION,
        )
        self.assertEqual(
            multiple_returns_before_period,
            StateSupervisionPeriodSupervisionType.PROBATION,
        )
        self.assertEqual(
            simultaneous_returns_before_period,
            StateSupervisionPeriodSupervisionType.PROBATION,
        )
        self.assertEqual(
            absconsion_on_both_ends,
            StateSupervisionPeriodSupervisionType.PROBATION,
        )
        self.assertEqual(
            override_period_after_re_abscond,
            StateSupervisionPeriodSupervisionType.ABSCONSION,
        )
        self.assertEqual(
            no_override_re_abscond_period,
            StateSupervisionPeriodSupervisionType.PROBATION,
        )
