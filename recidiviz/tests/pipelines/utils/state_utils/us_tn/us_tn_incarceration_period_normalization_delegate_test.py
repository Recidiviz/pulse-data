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
from typing import List, Optional

import mock

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
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
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
        self.person_id = 4700000000000000123

        self.unique_id_patcher = mock.patch(
            "recidiviz.persistence.entity."
            "normalized_entities_utils.generate_primary_key"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 4700000000012312345

    def tearDown(self) -> None:
        self.unique_id_patcher.stop()

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
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
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
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
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
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
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
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=None,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
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
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
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
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 6, 10),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 5, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            release_reason_raw_text="PAFA-PAVOK",
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
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text="PAFA-PAVOK",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
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

    # Test adding IP for an open IN_CUSTODY supervision period when a person has no incarceration periods
    def test_inferred_incarceration_periods_from_open_IC_supervision_period_with_no_ips(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=None,
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        new_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4700000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 3, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=None,
            release_reason=None,
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        additional_ip = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=[],
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[supervision_period]
            ),
        )

        expected_periods = [new_period]
        self.assertEqual(expected_periods, additional_ip)

    # Test adding IP for a closed IN_CUSTODY supervision period when a person has no incarceration periods
    def test_inferred_incarceration_periods_from_closed_IC_supervision_period_with_no_ips(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 4, 20),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        new_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4700000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 3, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 4, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        additional_ip = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=[],
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[supervision_period]
            ),
        )

        expected_periods = [new_period]
        self.assertEqual(expected_periods, additional_ip)

    # Test adding IP for a closed IN_CUSTODY supervision period when a person has an incarceration period admission date that starts on the same day as the SP termination date
    def test_inferred_incarceration_periods_from_closed_IC_supervision_period_that_abutts_IP_start(
        self,
    ) -> None:
        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 4, 20),
            termination_date=date(2017, 4, 30),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 9, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            release_reason_raw_text="PAFA-PAVOK",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason_raw_text="FAFA-TRANS",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        new_inferred_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4700000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 4, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 4, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        additional_ip = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[supervision_period_1]
            ),
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            new_inferred_period,
        ]
        self.assertEqual(expected_periods, additional_ip)

    # Test not adding IP for a closed IN_CUSTODY supervision period when a person has an incarceration period admission date that starts after SP termination date
    def test_inferred_incarceration_periods_from_closed_IC_supervision_period_that_terminates_before_IP_start(
        self,
    ) -> None:
        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 4, 20),
            termination_date=date(2017, 4, 25),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 9, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            release_reason_raw_text="PAFA-PAVOK",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason_raw_text="FAFA-TRANS",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        new_inferred_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4700000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 4, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 4, 25),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        additional_ip = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[supervision_period_1]
            ),
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            new_inferred_period,
        ]
        self.assertEqual(expected_periods, additional_ip)

    # Test adding IP for a closed IN_CUSTODY supervision period when a person has an incarceration period admission date that is before SP termination date but within 3 months of start
    def test_inferred_incarceration_periods_from_closed_IC_supervision_period_that_overlaps_IP_start(
        self,
    ) -> None:
        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 4, 20),
            termination_date=date(2017, 5, 7),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 9, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            release_reason_raw_text="PAFA-PAVOK",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-TRANS",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        new_inferred_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4700000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 4, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 4, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        additional_ip = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[supervision_period_1]
            ),
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            new_inferred_period,
        ]
        self.assertEqual(expected_periods, additional_ip)


class TestNormalizedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_TN-specific aspects of the
    normalized_incarceration_periods_and_additional_attributes function on the
    IncarcerationNormalizationManager when a UsTnIncarcerationNormalizationDelegate
    is provided."""

    def setUp(self) -> None:
        self.person_id = 4700000000000000123

        self.unique_id_patcher = mock.patch(
            "recidiviz.persistence.entity."
            "normalized_entities_utils.generate_primary_key"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 4700000000012312345

    def tearDown(self) -> None:
        self.unique_id_patcher.stop()

    @staticmethod
    def _normalized_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        normalized_supervision_periods: Optional[
            List[NormalizedStateSupervisionPeriod]
        ] = None,
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        """Normalizes TN incarceration periods for calculations."""
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        sp_index = default_normalized_sp_index_for_tests(normalized_supervision_periods)

        ip_normalization_manager = IncarcerationPeriodNormalizationManager(
            person_id=42000001234,
            incarceration_periods=incarceration_periods,
            normalization_delegate=UsTnIncarcerationNormalizationDelegate(),
            normalized_supervision_period_index=sp_index,
            normalized_violation_responses=violation_responses,
            incarceration_sentences=[],
            field_index=CoreEntityFieldIndex(),
            earliest_death_date=earliest_death_date,
        )

        (
            ips,
            _,
        ) = (
            ip_normalization_manager.normalized_incarceration_periods_and_additional_attributes()
        )

        return ips

    # Tests creating IPs for a person with no other IPS but has IN_CUSTODY SPs that indicates we should infer an IP
    def test_inferred_additional_ips_when_no_other_ips(self) -> None:

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 3, 5),
            termination_date=None,
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        new_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4700000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 3, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=None,
            release_reason=None,
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        normalized_ips = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[],
            normalized_supervision_periods=[supervision_period],
        )

        expected_periods = [new_period]

        self.assertEqual(expected_periods, normalized_ips)

    # Tests not creating any additional IPs for a person when they don't have any IN_CUSTODY SPs
    def test_no_inferred_additional_ips_when_no_ic_sps(self) -> None:

        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_TN",
            start_date=date(2017, 4, 20),
            termination_date=date(2017, 5, 7),
            supervision_level=StateSupervisionLevel.MINIMUM,
            sequence_num=0,
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2018, 9, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text="PAFA-PAVOK",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-TRANS",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        normalized_ips = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            normalized_supervision_periods=[supervision_period_1],
        )

        expected_periods = [incarceration_period_1, incarceration_period_2]

        self.assertEqual(expected_periods, normalized_ips)
