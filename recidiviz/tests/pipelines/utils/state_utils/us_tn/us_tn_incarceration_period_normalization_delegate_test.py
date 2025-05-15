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
from copy import deepcopy
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
from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
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
            admission_reason_raw_text="PAFA-VIOLT-T",
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
            admission_reason_raw_text="PRFA-VIOLW-T",
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
            admission_reason_raw_text="PRFA-NEWCH-T",
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
            release_reason_raw_text="PAFA-PAVOK-P",
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
            release_reason_raw_text="PAFA-PAVOK-P",
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
            release_reason_raw_text="PAFA-PAVOK-P",
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
            new_inferred_period,
            incarceration_period_1,
            incarceration_period_2,
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
            release_reason_raw_text="PAFA-PAVOK-P",
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
            new_inferred_period,
            incarceration_period_1,
            incarceration_period_2,
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
            release_reason_raw_text="PAFA-PAVOK-P",
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
            new_inferred_period,
            incarceration_period_1,
            incarceration_period_2,
        ]
        self.assertEqual(expected_periods, additional_ip)

    def test_override_custodial_authority_for_temporary_movements(
        self,
    ) -> None:
        # Basic test case for _us_tn_override_custodial_authority_for_temporary_movements.
        # Check that if a period has "-T" in the admission reason raw text, and comes after
        # a period with "-P" in the admission reason raw text, then the custodial authority
        # for the "T" period gets set to whatever the custodial authority for the "P" period
        # was. In this test, the custodial authority for periods 2 and 3 are overrided using
        # period 1's custodial authority, and period 5 has its custodial authority overrided
        # using period 4's custodial authority.

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAORD-P",
            release_date=date(2018, 9, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-MEDTE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-MEDTE-T",
            release_date=date(2018, 10, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-MEDTE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.COUNTY,
        )
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip3",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 10, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-MEDTE-T",
            release_date=date(2018, 11, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-MEDTE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.OTHER_STATE,
        )
        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 11, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAILT-P",
            release_date=date(2018, 12, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-MEDTE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.FEDERAL,
        )
        incarceration_period_5 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=555,
            external_id="ip5",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 12, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-MEDTE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.COUNTY,
        )

        ips_with_custodial_authority_override = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=deepcopy(
                [
                    incarceration_period_1,
                    incarceration_period_2,
                    incarceration_period_3,
                    incarceration_period_4,
                    incarceration_period_5,
                ]
            ),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[]
            ),
        )
        ip1_normalized = deepcopy(incarceration_period_1)
        ip2_normalized = deep_entity_update(
            deepcopy(incarceration_period_2),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )
        ip3_normalized = deep_entity_update(
            deepcopy(incarceration_period_3),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )
        ip4_normalized = deepcopy(incarceration_period_4)
        ip5_normalized = deep_entity_update(
            deepcopy(incarceration_period_5),
            custodial_authority=StateCustodialAuthority.FEDERAL,
        )
        expected_periods = [
            ip1_normalized,
            ip2_normalized,
            ip3_normalized,
            ip4_normalized,
            ip5_normalized,
        ]
        self.assertEqual(expected_periods, ips_with_custodial_authority_override)

    def test_only_override_custodial_authority_when_applicable(
        self,
    ) -> None:
        incarceration_period_open_after_permanent_move = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAORD-P",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )
        normalized_periods_permanent_move_only = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=deepcopy(
                [incarceration_period_open_after_permanent_move]
            ),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[]
            ),
        )
        expected_periods_permanent_move_only = [
            deepcopy(incarceration_period_open_after_permanent_move)
        ]
        # Test that infer_additional_periods doesn't cause index out-of-bound errors if someone's
        # set of incarceration periods includes only a single period, with a "permanent" admission reason.
        self.assertEqual(
            expected_periods_permanent_move_only, normalized_periods_permanent_move_only
        )

        incarceration_period_open_after_temporary_move = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAORD-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )
        normalized_periods_temporary_move_only = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=deepcopy(
                [incarceration_period_open_after_temporary_move]
            ),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[]
            ),
        )
        expected_periods_temporary_move_only = [
            deepcopy(incarceration_period_open_after_temporary_move)
        ]
        # Test that infer_additional_periods doesn't cause index out-of-bound errors if someone's
        # set of incarceration periods includes only a single period, with a "temporary" admission reason.
        self.assertEqual(
            expected_periods_temporary_move_only, normalized_periods_temporary_move_only
        )

        incarceration_period_permanent_admission_to_state_prison = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip3",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAORD-P",
            release_date=date(2018, 10, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-MEDTE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )
        incarceration_period_temporary_admission_to_state_prison = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 10, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-MEDTE-T",
            release_date=date(2018, 11, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-JAORD-P",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )
        normalized_periods_temporary_after_permanent_same_custody = (
            self.delegate.infer_additional_periods(
                person_id=self.person_id,
                incarceration_periods=deepcopy(
                    [
                        incarceration_period_permanent_admission_to_state_prison,
                        incarceration_period_temporary_admission_to_state_prison,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[]
                ),
            )
        )
        expected_periods_temporary_after_permanent_same_custody = [
            deepcopy(incarceration_period_permanent_admission_to_state_prison),
            deepcopy(incarceration_period_temporary_admission_to_state_prison),
        ]
        # Test that infer_additional_periods won't change the custodial authority for an
        # incarceration period that has a "temporary" admission reason, if the custodial
        # authority has not changed since the most recent incarceration period with a
        # "permanent" admission reason.
        self.assertEqual(
            expected_periods_temporary_after_permanent_same_custody,
            normalized_periods_temporary_after_permanent_same_custody,
        )

        incarceration_period_permanent_admission_to_county = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=555,
            external_id="ip5",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 11, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAORD-P",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.COUNTY,
        )
        normalized_periods_permanent_after_temporary_changed_custody = (
            self.delegate.infer_additional_periods(
                person_id=self.person_id,
                incarceration_periods=deepcopy(
                    [
                        incarceration_period_temporary_admission_to_state_prison,
                        incarceration_period_permanent_admission_to_county,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[]
                ),
            )
        )
        expected_periods_permanent_after_temporary_changed_custody = [
            deepcopy(incarceration_period_temporary_admission_to_state_prison),
            deepcopy(incarceration_period_permanent_admission_to_county),
        ]
        # Test that infer_additional_periods won't change the custodial authority for an
        # incarceration period that has a "temporary" admission reason if the only incarceration
        # period(s) with a "permanent" admission reason occur AFTER the temporary one, even
        # if these periods have a different custodial authority.
        self.assertEqual(
            expected_periods_permanent_after_temporary_changed_custody,
            normalized_periods_permanent_after_temporary_changed_custody,
        )
        incarceration_period_permanent_admission_missing_custody = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=666,
            external_id="ip6",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAORD-P",
            release_date=date(2018, 10, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-MEDTE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_temporary_admission_with_custody = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=777,
            external_id="ip7",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 10, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-MEDTE-T",
            release_date=date(2018, 11, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-JAORD-P",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )
        normalized_periods_temporary_after_permanent_with_missing_custody = (
            self.delegate.infer_additional_periods(
                person_id=self.person_id,
                incarceration_periods=deepcopy(
                    [
                        incarceration_period_permanent_admission_missing_custody,
                        incarceration_period_temporary_admission_with_custody,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[]
                ),
            )
        )
        expected_periods_permanent_after_temporary_changed_custody = [
            deepcopy(incarceration_period_permanent_admission_missing_custody),
            deepcopy(incarceration_period_temporary_admission_with_custody),
        ]
        # Test that infer_additional_periods won't change the custodial authority for an
        # incarceration period that has a "temporary" admission reason if the most recent
        # preceding period with a "permanent" admission reason has NULL for custodial_authority.
        self.assertEqual(
            expected_periods_permanent_after_temporary_changed_custody,
            normalized_periods_temporary_after_permanent_with_missing_custody,
        )

    def test_override_custodial_authority_edge_cases(
        self,
    ) -> None:
        # Edge case 1: The period with the "T" admission reason has a different custodial
        # authority than the most recent period with a "P" admission reason, but the custodial
        # authority isn't actually changing in the "T" period: it's the same as the custodial
        # authority for the period between the "P" and "T" periods, which doesn't have a "T"
        # or "P" flag. Here, we don't override the custodial authority for the "T" period.
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAORD-P",
            release_date=date(2018, 1, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            release_reason_raw_text="CUSTCHANGEFH-CUST_CHANGE",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 1, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            admission_reason_raw_text="CUSTCHANGEFH-CUST_CHANGE",
            release_date=date(2018, 9, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-MEDTE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.COUNTY,
        )
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip3",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-MEDTE-T",
            release_date=date(2018, 10, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-MEDTE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.COUNTY,
        )
        normalized_periods_temporary_after_permanent_with_unflagged_period_in_between = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=deepcopy(
                [incarceration_period_1, incarceration_period_2, incarceration_period_3]
            ),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[]
            ),
        )
        expected_periods_temporary_after_permanent_with_unflagged_period_in_between = (
            deepcopy(
                [incarceration_period_1, incarceration_period_2, incarceration_period_3]
            )
        )
        self.assertEqual(
            expected_periods_temporary_after_permanent_with_unflagged_period_in_between,
            normalized_periods_temporary_after_permanent_with_unflagged_period_in_between,
        )

        # Edge case 2: Same as above case, but the custodial authority for the "T" period
        # is different than it was for both of the preceding periods. As such, it does receive
        # the override. However, we only ever use the custodial authority from "P" periods
        # to override the custodial authority for "T" periods, so even though the unflagged
        # period starts after the "P" period, the "P" period's custodial authority is used for the override.
        ip3_changed_custodial_authority = deep_entity_update(
            deepcopy(incarceration_period_3),
            custodial_authority=StateCustodialAuthority.FEDERAL,
        )
        normalized_periods_temporary_after_permanent_with_unflagged_period_in_between_2 = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=deepcopy(
                [
                    incarceration_period_1,
                    incarceration_period_2,
                    ip3_changed_custodial_authority,
                ]
            ),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[]
            ),
        )
        expected_periods_temporary_after_permanent_with_unflagged_period_in_between_2 = [
            deepcopy(incarceration_period_1),
            deepcopy(incarceration_period_2),
            deep_entity_update(
                deepcopy(ip3_changed_custodial_authority),
                custodial_authority=StateCustodialAuthority.STATE_PRISON,
            ),
        ]
        self.assertEqual(
            expected_periods_temporary_after_permanent_with_unflagged_period_in_between_2,
            normalized_periods_temporary_after_permanent_with_unflagged_period_in_between_2,
        )

        # Edge case 3: Another "P" period starts on the same date as the "P" period in the
        # previous tests, but this one is a zero-day period. When applying the override
        # to the "T" period, we use the custodial authority from the non-zero-day "P" period.
        ip1_zero_day = deep_entity_update(
            deepcopy(incarceration_period_1),
            release_date=date(2017, 4, 30),
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
        )
        normalized_periods_permanent_period_overlapping_with_zero_day_period = (
            self.delegate.infer_additional_periods(
                person_id=self.person_id,
                incarceration_periods=deepcopy(
                    [
                        incarceration_period_1,
                        ip1_zero_day,
                        incarceration_period_2,
                        ip3_changed_custodial_authority,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[]
                ),
            )
        )
        expected_periods_permanent_period_overlapping_with_zero_day_period = [
            deepcopy(ip1_zero_day),
            deepcopy(incarceration_period_1),
            deepcopy(incarceration_period_2),
            deep_entity_update(
                deepcopy(ip3_changed_custodial_authority),
                custodial_authority=StateCustodialAuthority.STATE_PRISON,
            ),
        ]
        self.assertEqual(
            expected_periods_permanent_period_overlapping_with_zero_day_period,
            normalized_periods_permanent_period_overlapping_with_zero_day_period,
        )

        # Edge case 4: Same zero-day period as above, but this time there isn't a non-zero
        # day "P" period preceding the "T" period. When deciding which preceding period to
        # use for the override, we prioritize non-zero day periods if multiple periods share
        # an admission date, but if the only option is a zero-day period we'll use it nonetheless.
        normalized_periods_non_overlapping_zero_day_period = (
            self.delegate.infer_additional_periods(
                person_id=self.person_id,
                incarceration_periods=deepcopy(
                    [
                        ip1_zero_day,
                        incarceration_period_2,
                        ip3_changed_custodial_authority,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[]
                ),
            )
        )
        expected_periods_non_overlapping_zero_day_period = [
            deepcopy(ip1_zero_day),
            deepcopy(incarceration_period_2),
            deep_entity_update(
                deepcopy(ip3_changed_custodial_authority),
                custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            ),
        ]
        self.assertEqual(
            expected_periods_non_overlapping_zero_day_period,
            normalized_periods_non_overlapping_zero_day_period,
        )

    def test_infer_period_between_temporary_transfer(
        self,
    ) -> None:
        # Tests that when a period ends in work release (FAWR) / transfer to other jurisdiction (FAOJ), with
        # a "-T" flag in the release reason, and then the next period starts after the first one
        # ends, with an admission reason containing the paired movement code (WRFA for FAWR, OJFA for FAOJ)
        # and a "T" flag, and the 2 periods have the same custodial authority, then a new
        # period is inferred to fill the gap between them.

        incarceration_period_ending_in_work_release = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAORD-P",
            release_date=date(2018, 4, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            release_reason_raw_text="FAWR-WKREL-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON_CUSTODIAL_AUTHORITY",
        )
        incarceration_period_starting_in_work_release_return_after_gap = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 5, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE,
            admission_reason_raw_text="WRFA-RETWK-T",
            release_date=date(2018, 6, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            release_reason_raw_text="FAWR-WKREL-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON_CUSTODIAL_AUTHORITY",
        )
        incarceration_period_starting_in_work_release_return_no_gap = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip3",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 6, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE,
            admission_reason_raw_text="WRFA-RETWK-T",
            release_date=date(2018, 7, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            release_reason_raw_text="FAWR-WKREL-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON_CUSTODIAL_AUTHORITY",
        )
        incarceration_period_not_work_release_after_gap = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 1, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFH-MEDTE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON_CUSTODIAL_AUTHORITY",
        )
        wr_ips_for_testing = [
            incarceration_period_ending_in_work_release,
            incarceration_period_starting_in_work_release_return_after_gap,
            incarceration_period_starting_in_work_release_return_no_gap,
            incarceration_period_not_work_release_after_gap,
        ]
        wr_normalized_periods = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=deepcopy(wr_ips_for_testing),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[]
            ),
        )
        wr_new_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4700000000012312345,
            external_id="ip1-INFERRED-WR",
            state_code="US_TN",
            admission_date=date(2018, 4, 30),
            release_date=date(2018, 5, 30),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON_CUSTODIAL_AUTHORITY",
            facility="INFERRED_WORK_RELEASE",
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )
        wr_expected_periods = [
            deepcopy(incarceration_period_ending_in_work_release),
            wr_new_period,
            deepcopy(incarceration_period_starting_in_work_release_return_after_gap),
            deepcopy(incarceration_period_starting_in_work_release_return_no_gap),
            deepcopy(incarceration_period_not_work_release_after_gap),
        ]

        # - A new period is inferred between the first and second periods.
        # - No period is inferred between periods 2 and 3 because even though the admission/release
        #   reasons are the same as for periods 1 and 2, there's no gap between period 2's release
        #   date and period 3's admission date.
        # - No period is inferred between periods 3 and 4 because even though there's a gap,
        #   the admission reason for the 4th period doesn't contain a FAOJ/FAWR code.
        self.assertEqual(wr_expected_periods, wr_normalized_periods)

        # Here, we just repeat the same test as above, but with the FAOJ/OJFA codes instead of
        # FAWR/WRFA codes. This has the same results as the other test: only the external ID
        # and the facility code for the inferred period are changed.
        new_ips_with_oj_admission_reasons = [
            (
                deep_entity_update(
                    deepcopy(ip),
                    admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
                    admission_reason_raw_text="OJFA-RTIAD-T",
                )
                if ip.admission_reason_raw_text == "WRFA-RETWK-T"
                else deepcopy(ip)
            )
            for ip in wr_ips_for_testing
        ]
        oj_ips_for_testing = [
            (
                deep_entity_update(
                    deepcopy(ip),
                    release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION,
                    release_reason_raw_text="FAOJ-IADDE-T",
                )
                if ip.release_reason_raw_text == "FAWR-WKREL-T"
                else deepcopy(ip)
            )
            for ip in new_ips_with_oj_admission_reasons
        ]
        oj_normalized_periods = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=deepcopy(oj_ips_for_testing),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[]
            ),
        )
        new_oj_period = deep_entity_update(
            deepcopy(wr_new_period),
            external_id="ip1-INFERRED-OJ",
            facility="INFERRED_OTHER_JURISDICTION",
        )
        oj_expected_periods = [
            deepcopy(oj_ips_for_testing[0]),
            new_oj_period,
            deepcopy(oj_ips_for_testing[1]),
            deepcopy(oj_ips_for_testing[2]),
            deepcopy(oj_ips_for_testing[3]),
        ]
        self.assertEqual(oj_expected_periods, oj_normalized_periods)

    def test_cases_where_period_cannot_be_inferred(
        self,
    ) -> None:
        # Start with a simplified version of the case in the previous test, where a new period
        # can be inferred between the two periods. After asserting that this test case results
        # in a new period being inferred, we can tweak the periods to make sure that the
        # period stops being inferred when certain criteria aren't met.
        ip1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FACT-OUTYS-T",
            release_date=date(2018, 4, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION,
            release_reason_raw_text="FAOJ-IADDE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON_CUSTODIAL_AUTHORITY",
        )
        ip2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 5, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            admission_reason_raw_text="OJFA-RTIAD-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON_CUSTODIAL_AUTHORITY",
        )

        normalized_periods_valid_gap = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=deepcopy(
                [
                    ip1,
                    ip2,
                ]
            ),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[]
            ),
        )
        new_period_valid_gap = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4700000000012312345,
            external_id="ip1-INFERRED-OJ",
            state_code="US_TN",
            admission_date=date(2018, 4, 30),
            release_date=date(2018, 5, 30),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON_CUSTODIAL_AUTHORITY",
            facility="INFERRED_OTHER_JURISDICTION",
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )
        expected_periods_valid_gap = [
            deepcopy(ip1),
            new_period_valid_gap,
            deepcopy(ip2),
        ]
        self.assertEqual(expected_periods_valid_gap, normalized_periods_valid_gap)

        # Now, change the admission reason raw text for one of the periods to no longer
        # contain a "T" flag. Since the release reason for the first period and the admission
        # reason for the second period both need to have "T" flags, no period should be inferred.
        ip1_non_temporary_release = deep_entity_update(
            deepcopy(ip1), release_reason_raw_text="FAOJ-DETAN-P"
        )
        normalized_periods_non_temporary_release = (
            self.delegate.infer_additional_periods(
                person_id=self.person_id,
                incarceration_periods=deepcopy(
                    [
                        ip1_non_temporary_release,
                        ip2,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[]
                ),
            )
        )
        expected_periods_non_temporary_release = [
            deepcopy(ip1_non_temporary_release),
            deepcopy(ip2),
        ]
        self.assertEqual(
            expected_periods_non_temporary_release,
            normalized_periods_non_temporary_release,
        )

        # Now, change the custodial authority for one of the periods so that the 2 periods
        # don't have the same custodial authority. As a result, no period should be inferred.
        ip1_different_custodial_authority = deep_entity_update(
            deepcopy(ip1),
            custodial_authority=StateCustodialAuthority.COUNTY,
            custodial_authority_raw_text="COUNTY_CUSTODIAL_AUTHORITY",
        )
        normalized_periods_different_custodial_authority = (
            self.delegate.infer_additional_periods(
                person_id=self.person_id,
                incarceration_periods=deepcopy(
                    [
                        ip1_different_custodial_authority,
                        ip2,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[]
                ),
            )
        )
        expected_periods_different_custodial_authority = [
            deepcopy(ip1_different_custodial_authority),
            deepcopy(ip2),
        ]
        self.assertEqual(
            expected_periods_different_custodial_authority,
            normalized_periods_different_custodial_authority,
        )

        # Now, move the periods around so that the OJFA admission comes before the FAOJ
        # release. Even though there's still a gap between the two periods, no period should
        # be inferred.
        ip2_before_ip1 = deep_entity_update(
            deepcopy(ip2),
            admission_date=date(2015, 4, 30),
            release_date=date(2015, 4, 30),
        )
        normalized_periods_ip2_before_ip1 = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=deepcopy(
                [
                    ip1,
                    ip2_before_ip1,
                ]
            ),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[]
            ),
        )
        expected_periods_ip2_before_ip1 = [
            deepcopy(ip2_before_ip1),
            deepcopy(ip1),
        ]
        self.assertEqual(
            expected_periods_ip2_before_ip1,
            normalized_periods_ip2_before_ip1,
        )

    def test_custodial_authority_override_then_infer_period(
        self,
    ) -> None:
        # Tests that _us_tn_override_custodial_authority_for_temporary_movements and
        # _us_tn_infer_periods_between_temporary_transfers work together as expected.
        # In this test case, the two periods look like they should have a new period inferred
        # between them, but the custodial authority for the second period is different from
        # the first period's custodial authority, which would normally prevent _us_tn_infer_periods_between_temporary_transfers
        # from inferring a new period. However, the first period has a "-P" flag in the
        # admission reason, so before _us_tn_infer_periods_between_temporary_transfers runs,
        # _us_tn_override_custodial_authority_for_temporary_movements will override the custodial
        # authority for the second period to match the first period's custodial authority.
        # As a result, by the time _us_tn_infer_periods_between_temporary_transfers runs,
        # the periods will have the same custodial authority and a new period can be inferred
        # between them.
        ip1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAORD-P",
            release_date=date(2018, 4, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION,
            release_reason_raw_text="FAOJ-IADDE-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON_CUSTODIAL_AUTHORITY",
        )
        ip2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 5, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            admission_reason_raw_text="OJFA-RTIAD-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.COUNTY,
            custodial_authority_raw_text="COUNTY_CUSTODIAL_AUTHORITY",
        )
        normalized_periods = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=deepcopy([ip1, ip2]),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[]
            ),
        )
        new_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4700000000012312345,
            external_id="ip1-INFERRED-OJ",
            state_code="US_TN",
            admission_date=date(2018, 4, 30),
            release_date=date(2018, 5, 30),
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="STATE_PRISON_CUSTODIAL_AUTHORITY",
            facility="INFERRED_OTHER_JURISDICTION",
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )
        expected_periods = [
            deepcopy(ip1),
            new_period,
            deep_entity_update(
                deepcopy(ip2), custodial_authority=StateCustodialAuthority.STATE_PRISON
            ),
        ]

        self.assertEqual(expected_periods, normalized_periods)

    # Test overriding the PFI for periods with "SAREC" in the admission reason, along with all
    # subsequent periods until a period is found with a NEW ADMISSION admission reason.
    # Also tests that the safekeeping-related logic doesn't interfere with the legacy_standardize_purpose_for_incarceration_values
    # function, except for where safekeeping periods are concerned.
    def test_pfi_override_for_safekeeping_periods(
        self,
    ) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.WEEKEND_CONFINEMENT,
            admission_reason_raw_text="CCFA-WKEND",
            release_date=date(2018, 9, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="TRANSFER-CODE-WITH-NO-PFI-MAPPING",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.WEEKEND_CONFINEMENT,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="TRANSFER-CODE-WITH-NO-PFI-MAPPING",
            release_date=date(2018, 10, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="CTFA-SAREC-T",
        )
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip3",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 10, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="CTFA-SAREC-T",
            release_date=date(2018, 11, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=444,
            external_id="ip4",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 11, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            release_date=date(2018, 12, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FACT-SARET-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_5 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=555,
            external_id="ip5",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 12, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FACT-SARET-T",
            release_date=date(2019, 1, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_6 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=666,
            external_id="ip6",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 1, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            release_date=date(2019, 2, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="CTFA-SAREC-T",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_7 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=777,
            external_id="ip7",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 2, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="CTFA-SAREC-T",
            release_date=date(2019, 3, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_8 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=888,
            external_id="ip8",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 3, 9),
            release_date=date(2019, 6, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_9 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=999,
            external_id="ip9",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 6, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="CTFA-NEWAD-P",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ips_with_pfi_override = (
            self.delegate.standardize_purpose_for_incarceration_values(
                incarceration_periods=deepcopy(
                    [
                        incarceration_period_1,
                        incarceration_period_2,
                        incarceration_period_3,
                        incarceration_period_4,
                        incarceration_period_5,
                        incarceration_period_6,
                        incarceration_period_7,
                        incarceration_period_8,
                        incarceration_period_9,
                    ]
                )
            )
        )

        # This period has a PFI of WEEKEND_CONFINEMENT which will be unchanged in normalization.
        ip1_normalized = deepcopy(incarceration_period_1)

        # This period is missing a PFI, but because it's a transfer from the period with a PFI
        # of WEEKEND_CONFINEMENT, it'll be normalized with a PFI of WEEKEND_CONFINEMENT by the
        # legacy_standardize_purpose_for_incarceration_values function.
        ip2_normalized = deep_entity_update(
            deepcopy(incarceration_period_2),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.WEEKEND_CONFINEMENT,
        )

        # This period has a PFI of GENERAL, which initially gets set to WEEKEND_CONFINEMENT by the
        # legacy_standardize_purpose_for_incarceration_values function. However, the admission
        # reason contains "SAREC" which indicates the start of a safekeeping period, meaning the PFI
        # will be set to SAFEKEEPING instead.
        ip3_normalized = deep_entity_update(
            deepcopy(incarceration_period_3),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SAFEKEEPING,
        )

        # This period doesn't have "SAREC" in the admission reason, but it comes after a period that does,
        # with no "SARET" admission reasons in between. The person will still be considered to
        # be in safekeeping until a "SARET" movement occurs, so this period's PFI is also set to SAFEKEEPING.
        ip4_normalized = deep_entity_update(
            deepcopy(incarceration_period_4),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SAFEKEEPING,
        )

        # This period has "SARET" in the admission reason, indicating it is a SAFEKEEPING person returning
        # from court to facility (CTFA). We were told by TN that safekeeping only ends when a person is
        # fully released or you see a new admission so we want the PFI of TEMPORARY_CUSTODY to continue.
        ip5_normalized = deep_entity_update(
            deepcopy(incarceration_period_5),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SAFEKEEPING,
        )

        # Because there has not been an admission reason of NEW_ADDMISSION since the "SAREC" code we
        # expect this to return TEMPORARY_CUSTODY
        ip6_normalized = deep_entity_update(
            deepcopy(incarceration_period_6),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SAFEKEEPING,
        )

        # This period has "SAREC" in the admission reason, so it's considered a safekeeping period and
        # we have not seen a NEW_ADDMISSION so we continue to set the PFI to SAFEKEEPING.
        ip7_normalized = deep_entity_update(
            deepcopy(incarceration_period_7),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SAFEKEEPING,
        )

        # This is the person's last period, and comes after a "SAREC" period. Its PFI is therefore
        # set to SAFEKEEPING–the same would be true for all periods in the future until we
        # see a NEW_ADMISSION.
        ip8_normalized = deep_entity_update(
            deepcopy(incarceration_period_8),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SAFEKEEPING,
        )
        # This period has an admission reason of NEW_ADMISSION so we set the PFI back to GENERAL
        ip9_normalized = deep_entity_update(
            deepcopy(incarceration_period_9),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        expected_periods = [
            ip1_normalized,
            ip2_normalized,
            ip3_normalized,
            ip4_normalized,
            ip5_normalized,
            ip6_normalized,
            ip7_normalized,
            ip8_normalized,
            ip9_normalized,
        ]
        self.assertEqual(expected_periods, ips_with_pfi_override)

    # Safekeeping periods get closed (i.e. we stop overriding the PFI with TEMPORARY_CUSTODY)
    # once someone reaches an IP with admission reason NEW_ADMISSION or REVOCATION. This test
    # checks that we consider a safekeeping period to be closed immediately if one of these
    # admission reasons occurs on the same day that the safekeeping period starts. This means
    # that the PFI override will never trigger, since the safekeeping period became closed
    # as soon as it began.
    def test_pfi_override_for_safekeeping_periods_when_overlapping(
        self,
    ) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 10, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="CTFA-NEWAD",
            release_date=date(2018, 10, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_TN",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 10, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="CTFA-SAREC-T",
            release_date=date(2018, 11, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ips_with_pfi_override = (
            self.delegate.standardize_purpose_for_incarceration_values(
                incarceration_periods=deepcopy(
                    [
                        incarceration_period_1,
                        incarceration_period_2,
                    ]
                )
            )
        )
        ip1_normalized = deepcopy(incarceration_period_1)
        ip2_normalized = deepcopy(incarceration_period_2)
        expected_periods = [
            ip1_normalized,
            ip2_normalized,
        ]
        self.assertEqual(expected_periods, ips_with_pfi_override)


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
            incarceration_periods=incarceration_periods,
            normalization_delegate=UsTnIncarcerationNormalizationDelegate(),
            normalized_supervision_period_index=sp_index,
            normalized_violation_responses=violation_responses,
            person_id=42000001234,
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
            release_reason_raw_text="PAFA-PAVOK-P",
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
