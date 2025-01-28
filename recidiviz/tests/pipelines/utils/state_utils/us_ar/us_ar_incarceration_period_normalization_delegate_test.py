#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests us_ar_incarceration_period_normalization_delegate.py."""
import unittest
from copy import deepcopy
from datetime import date
from typing import List, Optional

import attr
import mock

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
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
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_incarceration_period_normalization_delegate import (
    UsArIncarcerationNormalizationDelegate,
)
from recidiviz.tests.pipelines.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)

_STATE_CODE = StateCode.US_AR.value


class TestUsArIncarcerationNormalizationDelegate(unittest.TestCase):
    """Tests functions in TestUsArIncarcerationNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsArIncarcerationNormalizationDelegate()

        self.person_id = 500000000000000123

        self.unique_id_patcher = mock.patch(
            "recidiviz.persistence.entity."
            "normalized_entities_utils.generate_primary_key"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 500000000012312345

    def tearDown(self) -> None:
        self.unique_id_patcher.stop()

    @staticmethod
    def _build_delegate() -> UsArIncarcerationNormalizationDelegate:
        return UsArIncarcerationNormalizationDelegate()

    # ~~ Add new tests here ~~
    def test_normalize_90_day_revocations(
        self,
    ) -> None:
        ip_90_day_rev = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="111-1",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="90_DAY",
        )

        ip_non_90_day_rev = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2,
            external_id="111-2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2024, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

        # Because this IP has a specialized_purpose_for_incarceration_raw_text of "90_DAY",
        # its PFI is normalized to SHOCK_INCARCERATION. We have to rely on the raw text instead
        # of the specialized_purpose_for_incarceration itself, because we can't set
        # specialized_purpose_for_incarceration to SHOCK_INCARCERATION during ingest if we don't
        # want the admission_reason to be normalized to SANCTION_ADMISSION.
        ip_90_day_rev_normalized = deep_entity_update(
            deepcopy(ip_90_day_rev),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        # This IP looks the same, but doesn't have specialized_purpose_for_incarceration_raw_text = "90_DAY",
        # and therefore is only normalized using legacy_standardize_purpose_for_incarceration_values,
        # which sets the missing PFI to "GENERAL".
        ip_non_90_day_rev_normalized = deep_entity_update(
            deepcopy(ip_non_90_day_rev),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        self.assertEqual(
            self.delegate.standardize_purpose_for_incarceration_values(
                [ip_90_day_rev, ip_non_90_day_rev]
            ),
            [ip_90_day_rev_normalized, ip_non_90_day_rev_normalized],
        )

    def test_normalize_sanction_admissions(
        self,
    ) -> None:
        # When people are returned to prison for a sanction admission, specialized_purpose_for_incarceration
        # will be normalized to SHOCK_INCARCERATION. This normalization logic differs from the
        # approach to 90-day revocations in 2 ways:
        # 1. We check admission_reason rather than specialized_purpose_for_incarceration_raw_text
        # 2. The override takes effect before, rather than after, the legacy_standardize_purpose_for_incarceration_values
        # call. This means that the normalized SHOCK_INCARCERATION PFI can be carried through
        # subsequent tranfer periods, whereas for 90-day revocations, the override will only affect
        # periods with the flag in specialized_purpose_for_incarceration_raw_text.
        ip_sanction_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="111-1",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            release_date=date(2021, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        ip_transfer = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2,
            external_id="111-2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2021, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2022, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )
        ip_new_commitment = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3,
            external_id="111-3",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2023, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2024, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        ip_sanction_admission_normalized = deep_entity_update(
            deepcopy(ip_sanction_admission),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )
        ip_transfer_normalized = deep_entity_update(
            deepcopy(ip_transfer),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )
        ip_new_commitment_normalized = deep_entity_update(
            deepcopy(ip_new_commitment),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        self.assertEqual(
            self.delegate.standardize_purpose_for_incarceration_values(
                [ip_sanction_admission, ip_transfer, ip_new_commitment]
            ),
            [
                ip_sanction_admission_normalized,
                ip_transfer_normalized,
                ip_new_commitment_normalized,
            ],
        )

    def test_normalize_admitted_from_supervision_basic(
        self,
    ) -> None:
        sp_rev_just_before_ip = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="111-1",
            state_code=_STATE_CODE,
            start_date=date(2018, 11, 1),
            termination_date=date(2019, 11, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            sequence_num=0,
        )
        sp_rev_day_of_ip = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="111-1",
            state_code=_STATE_CODE,
            start_date=date(2015, 11, 1),
            termination_date=date(2020, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            sequence_num=0,
        )
        sp_non_rev_just_before_ip = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="111-1",
            state_code=_STATE_CODE,
            start_date=date(2018, 11, 1),
            termination_date=date(2019, 11, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            sequence_num=0,
        )
        sp_rev_long_before_ip = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="111-1",
            state_code=_STATE_CODE,
            start_date=date(2015, 11, 1),
            termination_date=date(2017, 11, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            sequence_num=0,
        )
        sp_rev_after_ip = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="111-1",
            state_code=_STATE_CODE,
            start_date=date(2015, 11, 1),
            termination_date=date(2023, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            sequence_num=0,
        )

        ip_adm_from_sup = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="111-1",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2022, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        ip_specific_adm_from_sup_type = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="111-1",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            release_date=date(2022, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_normalized_to_revocation = deep_entity_update(
            deepcopy(ip_adm_from_sup),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )
        incarceration_period_normalized_to_unknown = deep_entity_update(
            deepcopy(ip_adm_from_sup),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
        )
        incarceration_period_unchanged_admission_reason = deepcopy(
            ip_specific_adm_from_sup_type
        )

        # Supervision period terminates in REVOCATION less than 12 months prior to an IP with
        # admission reason ADMITTED_FROM_SUPERVISION: admission reason is inferred to be REVOCATION.
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=deepcopy([ip_adm_from_sup]),
                original_sorted_incarceration_periods=deepcopy([ip_adm_from_sup]),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_just_before_ip]
                ),
            ),
            incarceration_period_normalized_to_revocation,
        )
        # Same as above, but with the supervision termination and incarceration admission
        # on the same day: admission reason is inferred to be REVOCATION.
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=deepcopy([ip_adm_from_sup]),
                original_sorted_incarceration_periods=deepcopy([ip_adm_from_sup]),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_day_of_ip]
                ),
            ),
            incarceration_period_normalized_to_revocation,
        )

        # Supervision period terminates less than 12 months prior to an IP with admission reason
        # ADMITTED_FROM_SUPERVISION, but the SP termination reason is not REVOCATION:
        # admission reason is set to INTERNAL_UNKNOWN.
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=deepcopy([ip_adm_from_sup]),
                original_sorted_incarceration_periods=deepcopy([ip_adm_from_sup]),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_non_rev_just_before_ip]
                ),
            ),
            incarceration_period_normalized_to_unknown,
        )

        # Supervision period terminates in REVOCATION *more* than 12 months prior to an IP with admission
        # reason ADMITTED_FROM_SUPERVISION: admission reason is set to INTERNAL_UNKNOWN.
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=deepcopy([ip_adm_from_sup]),
                original_sorted_incarceration_periods=deepcopy([ip_adm_from_sup]),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_long_before_ip]
                ),
            ),
            incarceration_period_normalized_to_unknown,
        )
        # Supervision period terminates in REVOCATION after the IP with admission reason
        # ADMITTED_FROM_SUPERVISION: admission reason is set to INTERNAL_UNKNOWN.
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=deepcopy([ip_adm_from_sup]),
                original_sorted_incarceration_periods=deepcopy([ip_adm_from_sup]),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_after_ip]
                ),
            ),
            incarceration_period_normalized_to_unknown,
        )
        # Supervision period terminates in REVOCATION less than 12 months prior to an IP representing
        # a commitment from supervision, but with a specific admission reason set at ingest
        # (SANCTION_ADMISSION or TEMPORARY_CUSTODY). Normalization only looks at IPs with the
        # generic ADMITTED_FROM_SUPERVISION admission reason, so this IP is unchanged.
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=deepcopy([ip_specific_adm_from_sup_type]),
                original_sorted_incarceration_periods=deepcopy(
                    [ip_specific_adm_from_sup_type]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_just_before_ip]
                ),
            ),
            incarceration_period_unchanged_admission_reason,
        )

    def test_normalize_admitted_from_supervision_multiple_periods(
        self,
    ) -> None:
        sp_rev_just_before_ip = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="111-1",
            state_code=_STATE_CODE,
            start_date=date(2018, 11, 1),
            termination_date=date(2019, 11, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            sequence_num=0,
        )

        ip_before_rev = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="111-1",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2017, 8, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ip_after_rev_before_adm_from_sup = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="111-2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 11, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_date=date(2019, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ip_adm_from_sup = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="111-3",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2021, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        ip_transfer_after_rev = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="111-4",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2021, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2022, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        # Supervision period ends in revocation, then incarceration period begins with
        # ADMITTED_FROM_SUPERVISION, then another incarceration period follows:
        # admission reason for the first IP gets normalized to REVOCATION.
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=deepcopy(
                    [ip_adm_from_sup, ip_transfer_after_rev]
                ),
                original_sorted_incarceration_periods=deepcopy(
                    [ip_adm_from_sup, ip_transfer_after_rev]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_just_before_ip]
                ),
            ),
            deep_entity_update(
                deepcopy(ip_adm_from_sup),
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            ),
        )
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=1,
                sorted_incarceration_periods=deepcopy(
                    [ip_adm_from_sup, ip_transfer_after_rev]
                ),
                original_sorted_incarceration_periods=deepcopy(
                    [ip_adm_from_sup, ip_transfer_after_rev]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_just_before_ip]
                ),
            ),
            deepcopy(ip_transfer_after_rev),
        )

        # Same case as above but with another incarceration period between the date of the
        # supervision period's REVOCATION termination and the date of the incarceration period's
        # ADMITTED_FROM_SUPERVISION admission. ADMITTED_FROM_SUPERVISION admission reasons
        # are only normalized to REVOCATION if there are no other IPs between the admission date
        # and the REVOCATION SP termination. Since that's not the case here, the ADMITTED_FROM_SUPERVISION
        # admission reason is set to INTERNAL_UNKNOWN, and the other IPs are unchanged.
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=0,
                sorted_incarceration_periods=deepcopy(
                    [
                        ip_after_rev_before_adm_from_sup,
                        ip_adm_from_sup,
                        ip_transfer_after_rev,
                    ]
                ),
                original_sorted_incarceration_periods=deepcopy(
                    [
                        ip_after_rev_before_adm_from_sup,
                        ip_adm_from_sup,
                        ip_transfer_after_rev,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_just_before_ip]
                ),
            ),
            deepcopy(ip_after_rev_before_adm_from_sup),
        )
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=1,
                sorted_incarceration_periods=deepcopy(
                    [
                        ip_after_rev_before_adm_from_sup,
                        ip_adm_from_sup,
                        ip_transfer_after_rev,
                    ]
                ),
                original_sorted_incarceration_periods=deepcopy(
                    [
                        ip_after_rev_before_adm_from_sup,
                        ip_adm_from_sup,
                        ip_transfer_after_rev,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_just_before_ip]
                ),
            ),
            deep_entity_update(
                deepcopy(ip_adm_from_sup),
                admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        )
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=2,
                sorted_incarceration_periods=deepcopy(
                    [
                        ip_after_rev_before_adm_from_sup,
                        ip_adm_from_sup,
                        ip_transfer_after_rev,
                    ]
                ),
                original_sorted_incarceration_periods=deepcopy(
                    [
                        ip_after_rev_before_adm_from_sup,
                        ip_adm_from_sup,
                        ip_transfer_after_rev,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_just_before_ip]
                ),
            ),
            deepcopy(ip_transfer_after_rev),
        )

        # Normalization for IP ADMITTED_FROM_SUPERVISION admission reasons only looks for
        # other IPs between the supervision termination date and the given IP's admission date.
        # IPs prior to the supervision revocation don't affect this logic.
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=1,
                sorted_incarceration_periods=deepcopy(
                    [
                        ip_before_rev,
                        ip_adm_from_sup,
                        ip_transfer_after_rev,
                    ]
                ),
                original_sorted_incarceration_periods=deepcopy(
                    [
                        ip_before_rev,
                        ip_adm_from_sup,
                        ip_transfer_after_rev,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_just_before_ip]
                ),
            ),
            deep_entity_update(
                deepcopy(ip_adm_from_sup),
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            ),
        )

        # If there are IPs preceding the IP being normalized, which fall both before and after
        # the supervision period ending in revocation, then the IP occurring after the revocation
        # should still be considered when normalizing the ADMITTED_FROM_SUPERVISION period.
        self.assertEqual(
            self.delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=2,
                sorted_incarceration_periods=deepcopy(
                    [
                        ip_before_rev,
                        ip_after_rev_before_adm_from_sup,
                        ip_adm_from_sup,
                        ip_transfer_after_rev,
                    ]
                ),
                original_sorted_incarceration_periods=deepcopy(
                    [
                        ip_before_rev,
                        ip_after_rev_before_adm_from_sup,
                        ip_adm_from_sup,
                        ip_transfer_after_rev,
                    ]
                ),
                supervision_period_index=default_normalized_sp_index_for_tests(
                    supervision_periods=[sp_rev_just_before_ip]
                ),
            ),
            deep_entity_update(
                deepcopy(ip_adm_from_sup),
                admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        )


class TestNormalizedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_AR-specific aspects of the
    normalized_incarceration_periods_and_additional_attributes function on the
    IncarcerationNormalizationManager when a UsArIncarcerationNormalizationDelegate
    is provided."""

    @staticmethod
    def _normalized_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]] = None,
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        """Normalizes incarceration periods for calculations. IP pre-processing for
        US_AR does not rely on violation responses."""

        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        ip_normalization_manager = IncarcerationPeriodNormalizationManager(
            incarceration_periods=incarceration_periods,
            normalization_delegate=UsArIncarcerationNormalizationDelegate(),
            normalized_supervision_period_index=sp_index,
            normalized_violation_responses=violation_responses,
            person_id=123,
            earliest_death_date=earliest_death_date,
        )

        (
            ips,
            _,
        ) = (
            ip_normalization_manager.normalized_incarceration_periods_and_additional_attributes()
        )

        return ips

    def test_pfi_override_for_inferred_revocation(
        self,
    ) -> None:

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="111-1",
            state_code=_STATE_CODE,
            start_date=date(2018, 11, 1),
            termination_date=date(2019, 11, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            sequence_num=0,
        )
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="111-3",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2021, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="90_DAY",
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_pfi_override_for_sanction_admission(
        self,
    ) -> None:

        ip_sanction_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="111-1",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            release_date=date(2021, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        ip_transfer = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2,
            external_id="111-2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2021, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2022, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )
        ip_new_commitment = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3,
            external_id="111-3",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2023, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2024, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        ip_sanction_admission_updated = attr.evolve(
            ip_sanction_admission,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )
        ip_transfer_updated = attr.evolve(
            ip_transfer,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )
        ip_new_commitment_updated = attr.evolve(
            ip_new_commitment,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[
                    ip_sanction_admission,
                    ip_transfer,
                    ip_new_commitment,
                ]
            )
        )

        self.assertEqual(
            [
                ip_sanction_admission_updated,
                ip_transfer_updated,
                ip_new_commitment_updated,
            ],
            validated_incarceration_periods,
        )
