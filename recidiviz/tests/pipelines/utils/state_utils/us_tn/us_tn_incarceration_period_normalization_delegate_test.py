#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests us_tn_incarceration_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_incarceration_period_normalization_delegate import (
    UsTnIncarcerationNormalizationDelegate,
)
from recidiviz.tests.pipelines.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)

_STATE_CODE = StateCode.US_TN.value


class TestUsTnIncarcerationNormalizationDelegate(unittest.TestCase):
    """Tests functions in TestUsTnIncarcerationNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsTnIncarcerationNormalizationDelegate()

    def test_get_incarceration_admission_violation_type_technical(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            admission_reason_raw_text="PAFA-VIOLT",
            external_id="ip-1",
        )

        result = self.delegate.get_incarceration_admission_violation_type(
            incarceration_period_1
        )

        self.assertEqual(result, StateSupervisionViolationType.TECHNICAL)

    def test_get_incarceration_admission_violation_type_law(self) -> None:
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            admission_reason_raw_text="PRFA-VIOLW",
            external_id="ip-2",
        )

        result = self.delegate.get_incarceration_admission_violation_type(
            incarceration_period_2
        )

        self.assertEqual(result, StateSupervisionViolationType.LAW)

    def test_get_incarceration_admission_violation_type_none(self) -> None:
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            admission_reason_raw_text="PRFA-NEWCH",
            external_id="ip-2",
        )

        result = self.delegate.get_incarceration_admission_violation_type(
            incarceration_period_2
        )

        self.assertEqual(result, None)

    # Test normalizing IPs with NEW ADMISSION that have overlapping or abutting supervision period override to TEMPORARY CUSTODY
    def test_normalized_incarceration_periods_new_admission_with_abutting_period_override_to_temporary_custody(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 5, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2018, 5, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=[incarceration_period],
                original_sorted_incarceration_periods=[incarceration_period],
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[supervision_period]
                ),
            )
        )

        self.assertEqual(updated_period, validated_incarceration_periods)

    # Test normalizing IPs with NEW ADMISSION that does not have overlapping or abutting supervision period so no override should occur
    def test_normalized_incarceration_periods_new_admission_do_not_override_to_temporary_custody(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 5, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 5, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=[incarceration_period],
                original_sorted_incarceration_periods=[incarceration_period],
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[supervision_period]
                ),
            )
        )

        self.assertEqual(updated_period, validated_incarceration_periods)

    # Test normalizing IPs with NEW ADMISSION that have overlapping open supervision period override to TEMPORARY CUSTODY
    def test_normalized_incarceration_periods_new_admission_with_overlapping_open_sp_override_to_temporary_custody(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=None,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 5, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2018, 5, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=[incarceration_period],
                original_sorted_incarceration_periods=[incarceration_period],
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[supervision_period]
                ),
            )
        )

        self.assertEqual(updated_period, validated_incarceration_periods)

    # Test normalizing IPs with NEW ADMISSION that have overlapping closed supervision period override to TEMPORARY CUSTODY
    def test_normalized_incarceration_periods_new_admission_with_overlapping_closed_sp_override_to_temporary_custody(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 6, 10),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 5, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2018, 5, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=[incarceration_period],
                original_sorted_incarceration_periods=[incarceration_period],
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[supervision_period]
                ),
            )
        )

        self.assertEqual(updated_period, validated_incarceration_periods)
