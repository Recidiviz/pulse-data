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

# pylint: disable=unused-import,wrong-import-order,protected-access

"""Tests for recidivism/identifier.py."""
import unittest
from datetime import date
from typing import Any, Dict, List, Optional
from unittest import mock

import pytest

from recidiviz.calculator.pipeline.recidivism import identifier
from recidiviz.calculator.pipeline.recidivism.events import (
    NonRecidivismReleaseEvent,
    RecidivismReleaseEvent,
    ReleaseEvent,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_incarceration_period_pre_processing_delegate import (
    UsXxIncarcerationPreProcessingDelegate,
)

_COUNTY_OF_RESIDENCE = "county"
_COUNTY_OF_RESIDENCE_ROWS = [
    {
        "state_code": "US_XX",
        "person_id": 123,
        "county_of_residence": _COUNTY_OF_RESIDENCE,
    }
]


class TestClassifyReleaseEvents(unittest.TestCase):
    """Tests for the find_release_events function."""

    def setUp(self) -> None:
        self.pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils.get_state_specific_incarceration_period_pre_processing_delegate"
        )
        self.mock_pre_processing_delegate = self.pre_processing_delegate_patcher.start()
        self.mock_pre_processing_delegate.return_value = (
            UsXxIncarcerationPreProcessingDelegate()
        )
        self.identifier = identifier.RecidivismIdentifier()

    def tearDown(self) -> None:
        self.pre_processing_delegate_patcher.stop()

    def _test_find_release_events(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
        persons_to_recent_county_of_residence: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[int, List[ReleaseEvent]]:
        return self.identifier._find_release_events(
            incarceration_periods=incarceration_periods,
            supervision_periods=(supervision_periods or []),
            persons_to_recent_county_of_residence=(
                persons_to_recent_county_of_residence or _COUNTY_OF_RESIDENCE_ROWS
            ),
        )

    def testFindReleaseEvents_ignoreTemporaryCustody(self) -> None:
        """Tests the find_release_events function where a person has
        multiple admissions to periods of temporary custody. Releases from temporary
        custody should not be counted in release cohorts, and admissions to temporary
        custody should not be counted as reincarcerations.
        """
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        temporary_custody_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2014, 4, 14),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        temporary_custody_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2019, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            release_date=date(2019, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        revocation_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4444,
            external_id="4",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2020, 4, 14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        incarceration_periods = [
            initial_incarceration_period,
            temporary_custody_1,
            revocation_incarceration_period,
            temporary_custody_2,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(1, len(release_events_by_cohort))

        assert initial_incarceration_period.admission_date is not None
        assert initial_incarceration_period.release_date is not None
        assert revocation_incarceration_period.admission_date is not None
        self.assertCountEqual(
            [
                RecidivismReleaseEvent(
                    state_code="US_XX",
                    original_admission_date=initial_incarceration_period.admission_date,
                    release_date=initial_incarceration_period.release_date,
                    release_facility=None,
                    reincarceration_date=revocation_incarceration_period.admission_date,
                    reincarceration_facility=None,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                )
            ],
            release_events_by_cohort[2010],
        )

    def testFindReleaseEvents_ParoleBoardHoldThenRevocation(self) -> None:
        """Tests the find_release_events function where a parole board
        hold period is followed by a revocation period. In this test case the person
        did recidivate, and the revocation admission should be counted as the date of
        reincarceration.
        """
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        temporary_custody_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2014, 4, 14),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        revocation_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2014, 4, 14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        incarceration_periods = [
            initial_incarceration_period,
            temporary_custody_reincarceration,
            revocation_incarceration_period,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(1, len(release_events_by_cohort))

        assert initial_incarceration_period.admission_date is not None
        assert initial_incarceration_period.release_date is not None
        assert revocation_incarceration_period.admission_date is not None
        self.assertCountEqual(
            [
                RecidivismReleaseEvent(
                    state_code="US_XX",
                    original_admission_date=initial_incarceration_period.admission_date,
                    release_date=initial_incarceration_period.release_date,
                    release_facility=None,
                    reincarceration_date=revocation_incarceration_period.admission_date,
                    reincarceration_facility=None,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                )
            ],
            release_events_by_cohort[2010],
        )

    def testFindReleaseEvents_ReleaseToParoleThenParoleBoardHold(self) -> None:
        """Tests the find_release_events function where release to parole is followed by
        an admission to a parole board hold. In this test case the person
        did not recidivate.
        """
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        temporary_custody_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2014, 4, 14),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        incarceration_periods = [
            initial_incarceration_period,
            temporary_custody_reincarceration,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(1, len(release_events_by_cohort))

        assert initial_incarceration_period.admission_date is not None
        assert initial_incarceration_period.release_date is not None
        self.assertCountEqual(
            [
                NonRecidivismReleaseEvent(
                    state_code="US_XX",
                    original_admission_date=initial_incarceration_period.admission_date,
                    release_date=initial_incarceration_period.release_date,
                    release_facility=None,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                )
            ],
            release_events_by_cohort[2010],
        )

    def testFindReleaseEvents_ReleaseToParoleThenTemporaryCustody(self) -> None:
        """Tests the find_release_events function where release to parole is followed by
        an admission to a period of temporary custody. In this test case the person
        did not recidivate.
        """
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        temporary_custody_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2014, 4, 14),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        incarceration_periods = [
            initial_incarceration_period,
            temporary_custody_reincarceration,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(1, len(release_events_by_cohort))

        assert initial_incarceration_period.admission_date is not None
        assert initial_incarceration_period.release_date is not None
        self.assertCountEqual(
            [
                NonRecidivismReleaseEvent(
                    state_code="US_XX",
                    original_admission_date=initial_incarceration_period.admission_date,
                    release_date=initial_incarceration_period.release_date,
                    release_facility=None,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                )
            ],
            release_events_by_cohort[2010],
        )

    def test_find_release_events(self) -> None:
        """Tests the find_release_events function path where the
        person did recidivate."""

        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        subsequent_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2017, 1, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
            subsequent_reincarceration_period,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(2, len(release_events_by_cohort))

        assert initial_incarceration_period.admission_date is not None
        assert initial_incarceration_period.release_date is not None
        assert first_reincarceration_period.admission_date is not None
        assert first_reincarceration_period.release_date is not None
        assert subsequent_reincarceration_period.admission_date is not None
        self.assertCountEqual(
            [
                RecidivismReleaseEvent(
                    state_code="US_XX",
                    original_admission_date=initial_incarceration_period.admission_date,
                    release_date=initial_incarceration_period.release_date,
                    release_facility=None,
                    reincarceration_date=first_reincarceration_period.admission_date,
                    reincarceration_facility=None,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                )
            ],
            release_events_by_cohort[2010],
        )

        self.assertCountEqual(
            [
                RecidivismReleaseEvent(
                    state_code="US_XX",
                    original_admission_date=first_reincarceration_period.admission_date,
                    release_date=first_reincarceration_period.release_date,
                    release_facility=None,
                    reincarceration_date=subsequent_reincarceration_period.admission_date,
                    reincarceration_facility=None,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                )
            ],
            release_events_by_cohort[2014],
        )

    def test_find_release_events_no_incarcerations_at_all(self) -> None:
        """Tests the find_release_events function when the person
        has no StateIncarcerationPeriods."""
        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=[]
        )

        assert not release_events_by_cohort

    def test_find_release_events_no_recidivism_after_first(self) -> None:
        """Tests the find_release_events function when the person
        does not have any StateIncarcerationPeriods after their first."""
        only_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2000, 1, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2003, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=[only_incarceration_period]
        )

        assert len(release_events_by_cohort) == 1

        assert only_incarceration_period.admission_date is not None
        assert only_incarceration_period.release_date is not None
        assert release_events_by_cohort[2003] == [
            NonRecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=only_incarceration_period.admission_date,
                release_date=only_incarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None,
            )
        ]

    def test_find_release_events_still_incarcerated(self) -> None:
        """Tests the find_release_events function where the
        person is still incarcerated on their very first
         StateIncarcerationPeriod."""
        only_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=[only_incarceration_period]
        )

        assert not release_events_by_cohort

    def test_find_release_events_invalid_open_period(self) -> None:
        """Tests the find_release_events function where the person has an open IN_CUSTODY period that is
        invalid because the person was released elsewhere after the admission to the period."""
        invalid_open_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        closed_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 3, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=[
                invalid_open_incarceration_period,
                closed_incarceration_period,
            ]
        )

        assert closed_incarceration_period.admission_date is not None
        assert closed_incarceration_period.release_date is not None
        assert release_events_by_cohort[2009] == [
            NonRecidivismReleaseEvent(
                state_code="US_XX",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                original_admission_date=closed_incarceration_period.admission_date,
                release_date=closed_incarceration_period.release_date,
                release_facility=None,
            )
        ]

    def test_find_release_events_overlapping_periods(self) -> None:
        """Tests the find_release_events function where the person has two overlapping periods, caused by
        data entry errors. We don't want to create a ReleaseEvent for a release that overlaps with another period of
        incarceration, so we only produce a ReleaseEvent for the period with the later release."""
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 4, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 4, 16),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=[incarceration_period_2, incarceration_period_1]
        )

        assert incarceration_period_1.admission_date is not None
        assert incarceration_period_1.release_date is not None
        self.assertCountEqual(
            [
                NonRecidivismReleaseEvent(
                    state_code="US_XX",
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    original_admission_date=incarceration_period_1.admission_date,
                    release_date=incarceration_period_1.release_date,
                    release_facility=None,
                )
            ],
            release_events_by_cohort[2009],
        )

    def test_find_release_events_release_same_day(self) -> None:
        """Tests the find_release_events function where the person has two periods with release dates on
        the same day. The second period is entirely nested within the first period, so will be filtered out in ip
        pre-processing."""
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 4, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 12, 21),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 4, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=[incarceration_period_2, incarceration_period_1]
        )

        assert incarceration_period_1.admission_date is not None
        assert incarceration_period_1.release_date is not None
        self.assertCountEqual(
            [
                NonRecidivismReleaseEvent(
                    state_code="US_XX",
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    original_admission_date=incarceration_period_1.admission_date,
                    release_date=incarceration_period_1.release_date,
                    release_facility=None,
                )
            ],
            release_events_by_cohort[2009],
        )

    def test_find_release_events_two_open_periods(self) -> None:
        """Tests the find_release_events function where the person has two open periods, caused by
        data entry errors. We don't want to create any release events in this situation."""
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id="1111",
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id="2222",
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=[incarceration_period_2, incarceration_period_1]
        )

        self.assertEqual({}, release_events_by_cohort)

    def test_find_release_events_no_recid_cond_release(self) -> None:
        """Tests the find_release_events function when the person
        does not have any StateIncarcerationPeriods after their first, and they
        were released conditionally."""
        only_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2000, 1, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2003, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=[only_incarceration_period]
        )

        assert len(release_events_by_cohort) == 1

        assert only_incarceration_period.admission_date is not None
        assert only_incarceration_period.release_date is not None
        assert release_events_by_cohort[2003] == [
            NonRecidivismReleaseEvent(
                state_code="US_XX",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                original_admission_date=only_incarceration_period.admission_date,
                release_date=only_incarceration_period.release_date,
                release_facility=None,
            )
        ]

    def test_find_release_events_parole_revocation(self) -> None:
        """Tests the find_release_events function path where the
        person was conditionally released on parole and returned for a parole
        revocation."""

        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        assert len(release_events_by_cohort) == 2

        assert initial_incarceration_period.admission_date is not None
        assert initial_incarceration_period.release_date is not None
        assert first_reincarceration_period.admission_date is not None
        assert first_reincarceration_period.release_date is not None
        assert release_events_by_cohort[2010] == [
            RecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=initial_incarceration_period.admission_date,
                release_date=initial_incarceration_period.release_date,
                release_facility=None,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                reincarceration_date=first_reincarceration_period.admission_date,
                reincarceration_facility=None,
            )
        ]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=first_reincarceration_period.admission_date,
                release_date=first_reincarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None,
            )
        ]

    def test_find_release_events_probation_revocation(self) -> None:
        """Tests the find_release_events function path where the
        person was conditionally released and returned for a probation
        revocation."""

        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        assert len(release_events_by_cohort) == 2

        assert initial_incarceration_period.admission_date is not None
        assert initial_incarceration_period.release_date is not None
        assert first_reincarceration_period.admission_date is not None
        assert first_reincarceration_period.release_date is not None
        assert release_events_by_cohort[2010] == [
            RecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=initial_incarceration_period.admission_date,
                release_date=initial_incarceration_period.release_date,
                release_facility=None,
                reincarceration_date=first_reincarceration_period.admission_date,
                reincarceration_facility=None,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            )
        ]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=first_reincarceration_period.admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            )
        ]

    def test_find_release_events_cond_release_new_admit(self) -> None:
        """Tests the find_release_events function path where the
        person was conditionally released on parole but returned as a new
         admission."""

        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        assert len(release_events_by_cohort) == 2

        assert initial_incarceration_period.admission_date is not None
        assert initial_incarceration_period.release_date is not None
        assert first_reincarceration_period.admission_date is not None
        assert first_reincarceration_period.release_date is not None
        assert release_events_by_cohort[2010] == [
            RecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=initial_incarceration_period.admission_date,
                release_date=initial_incarceration_period.release_date,
                release_facility=None,
                reincarceration_date=first_reincarceration_period.admission_date,
                reincarceration_facility=None,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            )
        ]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=first_reincarceration_period.admission_date,
                release_date=first_reincarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None,
            )
        ]

    def test_find_release_events_sentence_served_prob_rev(self) -> None:
        """Tests the find_release_events function path where the
        person served their first sentence, then later returned on a probation
        revocation."""

        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        assert len(release_events_by_cohort) == 2

        assert initial_incarceration_period.admission_date is not None
        assert initial_incarceration_period.release_date is not None
        assert first_reincarceration_period.admission_date is not None
        assert first_reincarceration_period.release_date is not None
        assert release_events_by_cohort[2010] == [
            RecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=initial_incarceration_period.admission_date,
                release_date=initial_incarceration_period.release_date,
                release_facility=None,
                reincarceration_date=first_reincarceration_period.admission_date,
                reincarceration_facility=None,
                county_of_residence=_COUNTY_OF_RESIDENCE,
            )
        ]

        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=first_reincarceration_period.admission_date,
                release_date=first_reincarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None,
            )
        ]

    def test_find_release_events_transfer_no_recidivism(self) -> None:
        """Tests the find_release_events function path where the
        person was transferred between two incarceration periods."""

        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        assert len(release_events_by_cohort) == 1

        assert initial_incarceration_period.admission_date is not None
        assert first_reincarceration_period.release_date is not None
        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=initial_incarceration_period.admission_date,
                release_date=first_reincarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None,
            )
        ]

    def test_find_release_events_transfer_out_but_recid(self) -> None:
        """Tests the find_release_events function path where the
        person was transferred out of state, then later returned on a new
        admission."""
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
        ]

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=incarceration_periods
        )

        assert len(release_events_by_cohort) == 1

        assert first_reincarceration_period.admission_date is not None
        assert first_reincarceration_period.release_date is not None
        assert release_events_by_cohort[2014] == [
            NonRecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=first_reincarceration_period.admission_date,
                release_date=first_reincarceration_period.release_date,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_facility=None,
            )
        ]

    def test_find_release_events_only_placeholder_periods(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code="US_XX",
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        release_events_by_cohort = self._test_find_release_events(
            incarceration_periods=[incarceration_period]
        )

        assert len(release_events_by_cohort) == 0


_SHOULD_BE_FILTERED_OUT_IN_VALIDATION_ADMISSION: List[
    StateIncarcerationPeriodAdmissionReason
] = [
    StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
    StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
    StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
    StateIncarcerationPeriodAdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE,
    StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
]

# Stores whether each release type should be included in a release cohort
RELEASE_REASON_INCLUSION: Dict[StateIncarcerationPeriodReleaseReason, bool] = {
    StateIncarcerationPeriodReleaseReason.COMMUTED: True,
    StateIncarcerationPeriodReleaseReason.COMPASSIONATE: True,
    StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE: True,
    StateIncarcerationPeriodReleaseReason.COURT_ORDER: False,
    StateIncarcerationPeriodReleaseReason.DEATH: False,
    StateIncarcerationPeriodReleaseReason.ESCAPE: False,
    StateIncarcerationPeriodReleaseReason.EXECUTION: False,
    StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN: False,
    StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN: False,
    StateIncarcerationPeriodReleaseReason.PARDONED: True,
    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION: True,
    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY: False,
    StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION: True,
    StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR: False,
    StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED: True,
    StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE: False,
    StateIncarcerationPeriodReleaseReason.TRANSFER: False,
    StateIncarcerationPeriodReleaseReason.TRANSFER_OUT_OF_STATE: False,
    StateIncarcerationPeriodReleaseReason.STATUS_CHANGE: False,
    StateIncarcerationPeriodReleaseReason.VACATED: True,
}


class TestShouldIncludeInReleaseCohort(unittest.TestCase):
    """Tests the should_include_in_release_cohort function."""

    def setUp(self) -> None:
        self.identifier = identifier.RecidivismIdentifier()

    def test_should_include_in_release_cohort(self) -> None:
        """Tests the should_include_in_release_cohort_function for all
        possible combinations of release reason and admission reason."""
        release_date = date(2000, 1, 1)
        status = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY

        for release_reason in StateIncarcerationPeriodReleaseReason:
            should_include = self.identifier._should_include_in_release_cohort(
                status=status,
                release_date=release_date,
                release_reason=release_reason,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                next_incarceration_period=None,
            )
            self.assertEqual(
                RELEASE_REASON_INCLUSION.get(release_reason), should_include
            )

    def test_should_include_in_release_cohort_in_custody(self) -> None:
        status = StateIncarcerationPeriodStatus.IN_CUSTODY
        should_include = self.identifier._should_include_in_release_cohort(
            status=status,
            release_date=None,
            release_reason=None,
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            next_incarceration_period=None,
        )
        self.assertFalse(should_include)

    def test_should_include_in_release_cohort_no_release_reason(self) -> None:
        status = StateIncarcerationPeriodStatus.IN_CUSTODY
        release_date = date(2000, 1, 1)
        should_include = self.identifier._should_include_in_release_cohort(
            status=status,
            release_date=release_date,
            release_reason=None,
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            next_incarceration_period=None,
        )
        self.assertFalse(should_include)

    def test_should_include_in_release_cohort_release_from_temp_custody(self) -> None:
        status = StateIncarcerationPeriodStatus.IN_CUSTODY
        release_date = date(2000, 1, 1)

        for release_reason in StateIncarcerationPeriodReleaseReason:
            should_include = self.identifier._should_include_in_release_cohort(
                status=status,
                release_date=release_date,
                release_reason=release_reason,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
                next_incarceration_period=None,
            )
            self.assertFalse(should_include)

    def test_should_include_in_release_cohort_release_from_board_hold(self) -> None:
        status = StateIncarcerationPeriodStatus.IN_CUSTODY
        release_date = date(2000, 1, 1)

        for release_reason in StateIncarcerationPeriodReleaseReason:
            should_include = self.identifier._should_include_in_release_cohort(
                status=status,
                release_date=release_date,
                release_reason=release_reason,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                next_incarceration_period=None,
            )
            self.assertFalse(should_include)

    def test_should_include_in_release_cohort_release_while_incarcerated(self) -> None:
        status = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY
        release_date = date(2000, 1, 31)
        next_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(1999, 12, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 4, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        should_include = self.identifier._should_include_in_release_cohort(
            status=status,
            release_date=release_date,
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            next_incarceration_period=next_incarceration_period,
        )
        self.assertFalse(should_include)

    def test_coverage_of_inclusion_map(self) -> None:
        release_reason_keys = RELEASE_REASON_INCLUSION.keys()

        for release_reason in StateIncarcerationPeriodReleaseReason:
            self.assertTrue(
                release_reason in release_reason_keys,
                "StateIncarcerationPeriodReleaseReason enum not "
                "handled in SHOULD_INCLUDE_WITH_RETURN_TYPE.",
            )

    def test_should_include_in_release_cohort_coverage(self) -> None:
        status = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY
        release_date = date(1978, 11, 1)

        for release_reason in StateIncarcerationPeriodReleaseReason:
            # Assert that no error is raised
            _ = self.identifier._should_include_in_release_cohort(
                status=status,
                release_date=release_date,
                release_reason=release_reason,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                next_incarceration_period=None,
            )


class TestFindValidReincarcerationPeriod(unittest.TestCase):
    """Tests the find_valid_reincarceration_period function."""

    def setUp(self) -> None:
        self.identifier = identifier.RecidivismIdentifier()

    def test_find_valid_reincarceration_period(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 4, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2010, 3, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        assert incarceration_periods[0].release_date is not None
        reincarceration = self.identifier._find_valid_reincarceration_period(
            incarceration_periods,
            index=0,
            release_date=incarceration_periods[0].release_date,
        )

        self.assertEqual(incarceration_period_2, reincarceration)

    def test_find_valid_reincarceration_period_overlapping_periods(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 4, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 4, 16),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        # The release on incarceration_period_1 overlaps with incarceration_period_2, and should be excluded from the
        # release cohort
        assert incarceration_periods[0].release_date is not None
        with self.assertRaises(ValueError):
            _ = self.identifier._find_valid_reincarceration_period(
                incarceration_periods,
                index=0,
                release_date=incarceration_periods[0].release_date,
            )

    def test_find_valid_reincarceration_period_invalid_admission_reason(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 4, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        for admission_reason in _SHOULD_BE_FILTERED_OUT_IN_VALIDATION_ADMISSION:
            incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code="US_XX",
                admission_date=date(2010, 3, 2),
                admission_reason=admission_reason,
                release_date=date(2012, 12, 1),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            )

            incarceration_periods = [incarceration_period_1, incarceration_period_2]

            assert incarceration_periods[0].release_date is not None
            reincarceration = self.identifier._find_valid_reincarceration_period(
                incarceration_periods,
                index=0,
                release_date=incarceration_periods[0].release_date,
            )

            self.assertIsNone(reincarceration)

    def test_find_valid_reincarceration_period_admission_reason_coverage(self) -> None:
        release_date = date(2009, 4, 21)
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=release_date,
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code="US_XX",
                admission_date=date(2010, 3, 2),
                admission_reason=admission_reason,
                release_date=date(2012, 12, 1),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            )

            incarceration_periods = [incarceration_period_1, incarceration_period_2]

            if (
                admission_reason
                == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
            ):
                with self.assertRaises(ValueError):
                    # Assert that this fails for this ingest-only admission_reason
                    _ = self.identifier._find_valid_reincarceration_period(
                        incarceration_periods,
                        index=0,
                        release_date=release_date,
                    )
            else:
                # Assert that this does not fail for all valid admission_reasons
                _ = self.identifier._find_valid_reincarceration_period(
                    incarceration_periods,
                    index=0,
                    release_date=release_date,
                )
