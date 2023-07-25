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
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.pipelines.normalization.utils.normalized_entities_utils import (
    clear_entity_id_index_cache,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_incarceration_period_normalization_delegate import (
    UsMiIncarcerationNormalizationDelegate,
)

_STATE_CODE = StateCode.US_MI.value


class TestUsMiIncarcerationNormalizationDelegate(unittest.TestCase):
    """Tests the us_mi_violation_response_normalization_delegate."""

    def setUp(self) -> None:
        self.delegate = UsMiIncarcerationNormalizationDelegate()
        self.person_id = 26000001234

        clear_entity_id_index_cache()
        self.unique_id_patcher = mock.patch(
            "recidiviz.pipelines.normalization.utils."
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

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods, person_id=self.person_id
        )

        expected_periods = [
            incarceration_period_1,
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=260000012345678,
                external_id=None,  # TODO(#22531): Hydrate this external_id properly
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
            incarceration_period_2,
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

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods, person_id=self.person_id
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

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods, person_id=self.person_id
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

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods, person_id=self.person_id
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

        result = self.delegate.infer_additional_periods(
            incarceration_periods=incarceration_periods, person_id=self.person_id
        )

        expected_periods = [incarceration_period_1, incarceration_period_2]

        self.assertEqual(result, expected_periods)
