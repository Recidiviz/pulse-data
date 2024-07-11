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
""""Tests the us_mi_incarceration_period_normalization_delegate.py file."""
import unittest
from datetime import date

import mock

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodCustodyLevel,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.normalized_entities_utils import (
    clear_entity_id_index_cache,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_incarceration_period_normalization_delegate import (
    UsMiIncarcerationNormalizationDelegate,
)
from recidiviz.tests.pipelines.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)

_STATE_CODE = StateCode.US_MI.value


class TestUsMiIncarcerationNormalizationDelegate(unittest.TestCase):
    """Tests the us_mi_violation_response_normalization_delegate."""

    def setUp(self) -> None:
        self.delegate = UsMiIncarcerationNormalizationDelegate()
        self.person_id = 26000001234

        clear_entity_id_index_cache()
        self.unique_id_patcher = mock.patch(
            "recidiviz.persistence.entity."
            "normalized_entities_utils._fixed_length_object_id_for_entity"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 5678

    @staticmethod
    def _build_delegate() -> UsMiIncarcerationNormalizationDelegate:
        return UsMiIncarcerationNormalizationDelegate()

    # Test case 1: test that new period gets created for temporary release period
    def test_basic_infer_additional_periods(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            admission_reason_raw_text="status change",
            release_date=date(2022, 7, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            release_reason_raw_text="temporary release",
            external_id="ip-1",
            custody_level=StateIncarcerationPeriodCustodyLevel.MEDIUM,
            custody_level_raw_text="medium",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="state prison",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="general",
            county_code="COUNTY A",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="state",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 8, 1),
            release_date=date(2022, 9, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            release_reason_raw_text="status change",
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE,
            admission_reason_raw_text="return",
            external_id="ip-2",
            custody_level=StateIncarcerationPeriodCustodyLevel.MAXIMUM,
            custody_level_raw_text="max",
            incarceration_type=StateIncarcerationType.FEDERAL_PRISON,
            incarceration_type_raw_text="federal prison",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.EXTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration_raw_text="unknown",
            county_code="COUNTY B",
            custodial_authority=StateCustodialAuthority.FEDERAL,
            custodial_authority_raw_text="federal",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        sp_index = default_normalized_sp_index_for_tests(supervision_periods=[])

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=260000012345678,
                external_id="ip-1-TEMPORARY_RELEASE",
                state_code=_STATE_CODE,
                admission_date=date(2022, 7, 1),
                release_date=date(2022, 8, 1),
                admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_RELEASE,
                release_reason=StateIncarcerationPeriodReleaseReason.RETURN_FROM_TEMPORARY_RELEASE,
                county_code="COUNTY A",
                custodial_authority=StateCustodialAuthority.STATE_PRISON,
                custodial_authority_raw_text="state",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                specialized_purpose_for_incarceration_raw_text="general",
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                incarceration_type_raw_text="state prison",
                custody_level=StateIncarcerationPeriodCustodyLevel.MEDIUM,
                custody_level_raw_text="medium",
            ),
        ]

        self.assertEqual(result, expected_periods)

    # Test case 2: test that new period does not get created if it's not a temporary release release reason
    def test_infer_additional_periods_not_release_release_reason(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 8, 1),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE,
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        sp_index = default_normalized_sp_index_for_tests(supervision_periods=[])

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
        ]

        self.assertEqual(result, expected_periods)

    # Test case 3: test that new period does not get created if it's not a temporary return from temp release admission reason
    def test_infer_additional_periods_not_release_admission_reason(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 8, 1),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        sp_index = default_normalized_sp_index_for_tests(supervision_periods=[])

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
        ]

        self.assertEqual(result, expected_periods)

    # Test case 4: test that new period does not get created if it's there's no return from temp IP
    def test_infer_additional_periods_no_return_from_temp_release_ip(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            external_id="ip-1",
        )

        incarceration_periods = [incarceration_period_1]

        sp_index = default_normalized_sp_index_for_tests(supervision_periods=[])

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        expected_periods = [
            incarceration_period_1,
        ]

        self.assertEqual(result, expected_periods)

    # Test case 5: test that new period does not get created if return from temp release is before temp release date
    def test_infer_additional_periods_return_before_release(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 8, 1),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        sp_index = default_normalized_sp_index_for_tests(supervision_periods=[])

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        expected_periods = [incarceration_period_1, incarceration_period_2]

        self.assertEqual(result, expected_periods)

    def test_get_incarceration_admission_violation_type(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            admission_reason_raw_text="15",
            external_id="ip-1",
        )

        result = self.delegate.get_incarceration_admission_violation_type(
            incarceration_period_1
        )

        self.assertEqual(result, StateSupervisionViolationType.TECHNICAL)

    # Test case 6: Test that an additional period is inferred when a revocation admission reason is encountered
    def test_infer_additional_periods_revocation_basic(self) -> None:

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 8, 1),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="15",
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1]

        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2021, 3, 5),
            termination_date=date(2021, 7, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
        )

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period_1]
        )

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        incarceration_period_0 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            incarceration_period_id=260000012345678,
            admission_date=date(2021, 7, 15),
            release_date=date(2021, 8, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="15",
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            custody_level=StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN,
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            external_id="ip-2-0-INFERRED(REVOCATION_IP)",
        )

        expected_periods = [incarceration_period_1, incarceration_period_0]

        self.assertEqual(result, expected_periods)

    # Test case 7: Test that when a revocation admission reason is encountered and there's an existing preceding IP, the admission reason of the existing preceding IP is updated
    def test_infer_additional_periods_revocation_overlapping(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 7, 1),
            release_date=date(2022, 8, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 8, 1),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="15",
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2021, 3, 5),
            termination_date=date(2021, 7, 5),
            termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
        )

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period_1]
        )

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        incarceration_period_1.admission_reason_raw_text = (
            incarceration_period_2.admission_reason_raw_text
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
        ]

        self.assertEqual(result, expected_periods)

    # Test case 7b: Test that when a revocation admission reason is encountered and there's an existing preceding IP and a gap, the admission reason of the existing preceding IP is updated and the gaps are filled in
    def test_infer_additional_periods_revocation_overlapping_b(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 7, 15),
            release_date=date(2021, 7, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 8, 1),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="15",
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2021, 3, 5),
            termination_date=date(2021, 7, 5),
            termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
        )

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period_1]
        )

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        incarceration_period_new_a = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            incarceration_period_id=260000012345678,
            admission_date=date(2021, 7, 5),
            release_date=date(2021, 7, 15),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text=incarceration_period_2.admission_reason_raw_text,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            custody_level=StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN,
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            external_id="ip-2-0-INFERRED(REVOCATION_IP)",
        )

        incarceration_period_new_b = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            incarceration_period_id=260000012345679,
            admission_date=date(2021, 7, 25),
            release_date=date(2021, 8, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text=incarceration_period_2.admission_reason_raw_text,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            custody_level=StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN,
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            external_id="ip-2-1-INFERRED(REVOCATION_IP)",
        )

        incarceration_period_1.admission_reason_raw_text = (
            incarceration_period_2.admission_reason_raw_text
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            incarceration_period_new_a,
            incarceration_period_new_b,
        ]

        self.assertEqual(result, expected_periods)

    # Test case 8: Test that an additional period is not inferred when a revocation admission reason is encountered but there's no preceding SP
    def test_infer_additional_periods_revocation_without_sup(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 8, 1),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1]

        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2022, 3, 5),
            termination_date=date(2022, 5, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
        )

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period_1]
        )

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        expected_periods = [incarceration_period_1]

        self.assertEqual(result, expected_periods)

    # Test case 8: Test that an additional period is not inferred when there's no revocation admission
    def test_infer_additional_periods_no_revocation(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 8, 1),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1]

        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2021, 3, 5),
            termination_date=date(2021, 7, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
        )

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period_1]
        )

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        expected_periods = [incarceration_period_1]

        self.assertEqual(result, expected_periods)

    # Test case 8: Test that an additional period is inferred when the previous SP ended with REVOCATION/ADMITTED_TO_INCARCERATION
    def test_infer_additional_periods_sp_revocation_end_basic(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 8, 1),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-2",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 9, 1),
            release_date=date(2022, 10, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-3",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2021, 3, 5),
            termination_date=date(2021, 7, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
        )

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period_1]
        )

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        incarceration_period_0 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            incarceration_period_id=260000012345678,
            admission_date=date(2021, 7, 15),
            release_date=date(2021, 8, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            custody_level=StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN,
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            external_id="sp1-INFERRED(REVOCATION_SP)",
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            incarceration_period_0,
        ]

        self.assertEqual(result, expected_periods)

    # Test case 9: Test that an additional period is not inferred when the previous SP
    # ended with REVOCATION/ADMITTED_TO_INCARCERATION but there was already an adjacent/overlapping IP
    def test_infer_additional_periods_sp_revocation_end_none(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 4, 1),
            release_date=None,
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-2",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 9, 1),
            release_date=date(2022, 10, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-3",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2021, 3, 5),
            termination_date=date(2021, 5, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
        )

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period_1]
        )

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
        ]

        self.assertEqual(result, expected_periods)

    # # Test case 10: Test that a TEMPORARY_CUSTODY period is inferred with there's a SP with missing supervision level
    # def test_infer_additional_periods_missing_sup_level(self) -> None:
    #     incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
    #         state_code=_STATE_CODE,
    #         admission_date=date(2021, 8, 1),
    #         release_date=date(2022, 9, 1),
    #         admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
    #         external_id="ip-2",
    #     )

    #     incarceration_periods = [incarceration_period_1]

    #     supervision_period_1 = NormalizedStateSupervisionPeriod(
    #         supervision_period_id=111,
    #         external_id="sp1",
    #         state_code=_STATE_CODE,
    #         start_date=date(2021, 3, 5),
    #         termination_date=date(2021, 5, 1),
    #         termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
    #         supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
    #         sequence_num=0,
    #     )

    #     sp_index = default_normalized_sp_index_for_tests(
    #         supervision_periods=[supervision_period_1]
    #     )

    #     result = self.delegate.infer_additional_periods(
    #         incarceration_periods=incarceration_periods,
    #         person_id=self.person_id,
    #         supervision_period_index=sp_index,
    #     )

    #     new_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
    #         state_code=_STATE_CODE,
    #         incarceration_period_id=260000012345678,
    #         admission_date=date(2021, 3, 5),
    #         release_date=date(2021, 5, 1),
    #         admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
    #         release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
    #         custody_level=StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN,
    #         incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
    #         custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
    #         external_id="sp1-0-MISSING-SUP-LEVEL",
    #     )

    #     expected_periods = [new_incarceration_period, incarceration_period_1]

    #     self.assertEqual(result, expected_periods)

    # # Test case 10B: Test that a TEMPORARY_CUSTODY period is not inferred if there's a SP with missing supervision level but it ends in DISCHARGE
    # def test_infer_additional_periods_missing_sup_level_discharge(self) -> None:
    #     incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
    #         state_code=_STATE_CODE,
    #         admission_date=date(2021, 8, 1),
    #         release_date=date(2022, 9, 1),
    #         admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
    #         external_id="ip-2",
    #     )

    #     incarceration_periods = [incarceration_period_1]

    #     supervision_period_1 = NormalizedStateSupervisionPeriod(
    #         supervision_period_id=111,
    #         external_id="sp1",
    #         state_code=_STATE_CODE,
    #         start_date=date(2021, 3, 5),
    #         termination_date=date(2021, 5, 1),
    #         termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
    #         supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
    #         sequence_num=0,
    #     )

    #     sp_index = default_normalized_sp_index_for_tests(
    #         supervision_periods=[supervision_period_1]
    #     )

    #     result = self.delegate.infer_additional_periods(
    #         incarceration_periods=incarceration_periods,
    #         person_id=self.person_id,
    #         supervision_period_index=sp_index,
    #     )

    #     expected_periods = [incarceration_period_1]

    #     self.assertEqual(result, expected_periods)

    # Test case 111: Test that a TEMPORARY_CUSTODY period is inferred with there's a SP with and IN_CUSTODY level
    def test_infer_additional_periods_in_custody_a(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 8, 1),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-2",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 9, 1),
            release_date=date(2022, 10, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-3",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2021, 3, 5),
            termination_date=date(2021, 5, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period_1]
        )

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        new_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            incarceration_period_id=260000012345678,
            admission_date=date(2021, 3, 5),
            release_date=date(2021, 5, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            external_id="sp1-0-IN-CUSTODY",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            new_incarceration_period,
        ]

        self.assertEqual(result, expected_periods)

    # Test case 12: Test that when there's an SP with IN_CUSTODY level, TEMPORARY_CUSTODY IPs are only infered in the gaps of time where there's not already an IP
    def test_infer_additional_periods_in_custody_b(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2021, 8, 1),
            release_date=date(2022, 8, 15),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-2",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 9, 1),
            release_date=date(2022, 10, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            external_id="ip-3",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2021, 7, 15),
            termination_date=date(2022, 10, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period_1]
        )

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods,
            person_id=self.person_id,
            supervision_period_index=sp_index,
        )

        new_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            incarceration_period_id=260000012345678,
            admission_date=date(2021, 7, 15),
            release_date=date(2021, 8, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            external_id="sp1-0-IN-CUSTODY",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        new_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            incarceration_period_id=260000012345679,
            admission_date=date(2022, 8, 15),
            release_date=date(2022, 9, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            external_id="sp1-1-IN-CUSTODY",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            new_incarceration_period_1,
            new_incarceration_period_2,
        ]

        self.assertEqual(result, expected_periods)
