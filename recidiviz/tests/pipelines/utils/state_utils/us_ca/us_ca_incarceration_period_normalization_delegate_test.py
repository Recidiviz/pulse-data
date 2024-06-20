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
"""Tests us_ca_incarceration_period_normalization_delegate.py."""
import unittest
from datetime import date

import mock

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.normalized_entities_utils import (
    clear_entity_id_index_cache,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_incarceration_period_normalization_delegate import (
    UsCaIncarcerationNormalizationDelegate,
)
from recidiviz.tests.pipelines.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)

_STATE_CODE = StateCode.US_CA.value


class TestUsCaIncarcerationNormalizationDelegate(unittest.TestCase):
    """Tests functions in TestUsCaIncarcerationNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsCaIncarcerationNormalizationDelegate()
        self.person_id = 42000001234

        clear_entity_id_index_cache()
        self.unique_id_patcher = mock.patch(
            "recidiviz.persistence.entity."
            "normalized_entities_utils._fixed_length_object_id_for_entity"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 12345

    # ~~ Add new tests here ~~
    def test_infer_temp_IPs_for_in_custody_SPs(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2022, 3, 5),
            termination_date=None,
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        new_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4200000123412345,
            external_id="sp1-0-IN-CUSTODY",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2022, 3, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
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
        self.maxDiff = None
        self.assertEqual(expected_periods, additional_ip)
