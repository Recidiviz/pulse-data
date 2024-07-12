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
"""Tests the functions in us_pa_commitment_from_supervision_utils.py"""

import unittest
from datetime import date
from typing import List, Optional

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
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.metrics.utils import commitment_from_supervision_utils
from recidiviz.pipelines.metrics.utils.commitment_from_supervision_utils import (
    CommitmentDetails,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_commitment_from_supervision_delegate import (
    UsPaCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_incarceration_delegate import (
    UsPaIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_incarceration_period_normalization_delegate import (
    PURPOSE_FOR_INCARCERATION_PVC,
    SHOCK_INCARCERATION_PVC,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.tests.pipelines.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_ip_index_for_tests,
)
from recidiviz.utils.types import assert_type

_DEFAULT_SUPERVISION_PERIOD_ID = 999
_DEFAULT_SUPERVISION_PERIOD_ID_2 = 888
_DEFAULT_SUPERVISION_PERIOD_ID_3 = 777

STATE_CODE = "US_PA"


class TestPreCommitmentSupervisionTypeIdentification(unittest.TestCase):
    """Tests the get_commitment_from_supervision_supervision_type function on the
    UsPaCommitmentFromSupervisionDelegate."""

    def setUp(self) -> None:
        self.delegate = UsPaCommitmentFromSupervisionDelegate()

    def _test_get_commitment_from_supervision_supervision_type(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        previous_supervision_period: Optional[NormalizedStateSupervisionPeriod] = None,
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        return self.delegate.get_commitment_from_supervision_supervision_type(
            incarceration_period=incarceration_period,
            previous_supervision_period=previous_supervision_period,
        )

    def test_us_pa_get_pre_commitment_supervision_type_default(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="ADMN",
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        supervision_type_pre_commitment = (
            self._test_get_commitment_from_supervision_supervision_type(
                incarceration_period=incarceration_period
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_pre_commitment,
        )

    def test_us_pa_get_pre_commitment_supervision_type_temporary_custody(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2009, 1, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
        )

        supervision_type_pre_commitment = (
            self._test_get_commitment_from_supervision_supervision_type(
                incarceration_period=incarceration_period
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_pre_commitment,
        )

    def test_us_pa_get_pre_commitment_supervision_type_sanction_admission_dual(
        self,
    ) -> None:
        """Right now, all sanction admissions are assumed to be from parole."""
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            start_date=date(2008, 3, 5),
            termination_date=date(2008, 12, 16),
            supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            admission_reason_raw_text="ADMN",
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        supervision_type_pre_commitment = (
            self._test_get_commitment_from_supervision_supervision_type(
                incarceration_period=incarceration_period,
                previous_supervision_period=supervision_period,
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type_pre_commitment,
        )

    def test_us_pa_get_pre_commitment_supervision_type_erroneous(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="ADMN",
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        with self.assertRaises(ValueError):
            _ = self._test_get_commitment_from_supervision_supervision_type(
                incarceration_period=incarceration_period,
            )


class TestGetCommitmentDetails(unittest.TestCase):
    """Tests the get_commitment_from_supervision_details function."""

    @staticmethod
    def _test_get_commitment_from_supervision_details(
        incarceration_period: NormalizedStateIncarcerationPeriod,
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]] = None,
        incarceration_period_index: Optional[NormalizedIncarcerationPeriodIndex] = None,
    ) -> CommitmentDetails:
        """Helper function for testing get_commitment_from_supervision_details."""
        incarceration_period_index = (
            incarceration_period_index
            or default_normalized_ip_index_for_tests(
                incarceration_periods=[incarceration_period],
                incarceration_delegate=UsPaIncarcerationDelegate(),
            )
        )

        supervision_period_index = NormalizedSupervisionPeriodIndex(
            sorted_supervision_periods=(supervision_periods or [])
        )

        return commitment_from_supervision_utils.get_commitment_from_supervision_details(
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            supervision_period_index=supervision_period_index,
            commitment_from_supervision_delegate=UsPaCommitmentFromSupervisionDelegate(),
            supervision_delegate=UsPaSupervisionDelegate(),
        )

    def test_get_commitment_from_supervision_details_pvc(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code=STATE_CODE,
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            state_code=STATE_CODE,
            supervision_site="DISTRICT_1|OFFICE_2|ORG_CODE",
            start_date=date(2017, 12, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
            supervising_officer_staff_id=12345,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 3, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            # Program 26 indicates a revocation to a PVC
            specialized_purpose_for_incarceration_raw_text=PURPOSE_FOR_INCARCERATION_PVC,
            release_date=date(2019, 5, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            purpose_for_incarceration_subtype=SHOCK_INCARCERATION_PVC,
        )

        assert incarceration_period.incarceration_period_id is not None
        ip_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period],
            incarceration_delegate=UsPaIncarcerationDelegate(),
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period,
            supervision_periods=[supervision_period],
            incarceration_period_index=ip_index,
        )

        assert supervision_period.supervision_period_id is not None
        self.assertEqual(
            commitment_details,
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype="PVC",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_staff_id=12345,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
        )

    def test_get_commitment_from_supervision_details_transfer_on_admission(
        self,
    ) -> None:
        """Tests that the period *prior to the incarceration admission*, the one that
        overlaps with the board hold, is chosen when a person is transferred to a new
        supervision period on the date of an admission to incarceration, and has a
        supervision period that terminated a few months before the parole board hold."""
        board_hold = NormalizedStateIncarcerationPeriod(
            state_code=STATE_CODE,
            incarceration_period_id=111,
            external_id="ip1",
            sequence_num=0,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_date=date(2019, 11, 18),
            release_date=date(2020, 1, 1),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        shock_period = NormalizedStateIncarcerationPeriod(
            state_code=STATE_CODE,
            incarceration_period_id=222,
            external_id="ip2",
            sequence_num=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            admission_date=date(2020, 1, 1),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        pre_board_hold_sp = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            sequence_num=0,
            state_code=STATE_CODE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=date(2010, 12, 1),
            termination_date=date(2019, 8, 3),
        )

        pre_commitment_sp = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID_2,
            external_id="sp2",
            sequence_num=1,
            state_code=STATE_CODE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=date(2019, 8, 3),
            termination_date=shock_period.admission_date,
            supervising_officer_staff_id=12345,
        )

        supervision_period_while_in_prison = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID_3,
            external_id="sp3",
            sequence_num=2,
            state_code=STATE_CODE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=assert_type(shock_period.admission_date, date),
        )

        ip_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[board_hold, shock_period],
            incarceration_delegate=UsPaIncarcerationDelegate(),
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period=shock_period,
            supervision_periods=[
                pre_board_hold_sp,
                pre_commitment_sp,
                supervision_period_while_in_prison,
            ],
            incarceration_period_index=ip_index,
        )

        assert pre_commitment_sp.supervision_period_id is not None
        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=pre_commitment_sp.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_staff_id=12345,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=pre_commitment_sp.supervision_level,
                supervision_level_raw_text=pre_commitment_sp.supervision_level_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_transfer_on_board_hold(
        self,
    ) -> None:
        """Tests that the period *prior to the incarceration admission*, the one that
        ends when the board hold starts, is chosen when a person is transferred to a new
        supervision period on the date of an admission to incarceration, and has a
        supervision period that terminated a few months before the parole board hold."""
        board_hold = NormalizedStateIncarcerationPeriod(
            state_code=STATE_CODE,
            incarceration_period_id=111,
            external_id="ip1",
            sequence_num=0,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_date=date(2019, 11, 18),
            release_date=date(2020, 1, 1),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        shock_period = NormalizedStateIncarcerationPeriod(
            state_code=STATE_CODE,
            incarceration_period_id=222,
            external_id="ip2",
            sequence_num=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            admission_date=date(2020, 1, 1),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        pre_board_hold_sp = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            sequence_num=0,
            state_code=STATE_CODE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=date(2010, 12, 1),
            termination_date=board_hold.admission_date,
            supervising_officer_staff_id=12345,
        )

        board_hold_sp = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID_2,
            external_id="sp2",
            sequence_num=1,
            state_code=STATE_CODE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=assert_type(board_hold.admission_date, date),
            termination_date=shock_period.admission_date,
        )

        supervision_period_while_in_prison = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID_3,
            external_id="sp3",
            sequence_num=2,
            state_code=STATE_CODE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=assert_type(shock_period.admission_date, date),
        )

        ip_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[board_hold, shock_period],
            incarceration_delegate=UsPaIncarcerationDelegate(),
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period=shock_period,
            supervision_periods=[
                pre_board_hold_sp,
                board_hold_sp,
                supervision_period_while_in_prison,
            ],
            incarceration_period_index=ip_index,
        )

        assert pre_board_hold_sp.supervision_period_id is not None
        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=pre_board_hold_sp.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_staff_id=12345,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=pre_board_hold_sp.supervision_level,
                supervision_level_raw_text=pre_board_hold_sp.supervision_level_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            commitment_details,
        )
