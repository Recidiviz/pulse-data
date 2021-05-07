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
"""Tests functions in the commitment_from_supervision_utils file."""
import unittest
from datetime import date
from typing import Optional, List, Dict, Any

from recidiviz.calculator.pipeline.utils import commitment_from_supervision_utils
from recidiviz.calculator.pipeline.utils.commitment_from_supervision_utils import (
    CommitmentDetails,
    default_violation_history_window_pre_commitment_from_supervision,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_commitment_from_supervision_utils import (
    PURPOSE_FOR_INCARCERATION_PVC,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason as ReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodTerminationReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionLevel,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
    StateSupervisionCaseTypeEntry,
)


_DEFAULT_SUPERVISION_PERIOD_ID = 999
_DEFAULT_SSVR_ID = 999

DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS = {
    999: {"agent_id": 000, "agent_external_id": "XXX", "supervision_period_id": 999}
}

DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS = {
    _DEFAULT_SUPERVISION_PERIOD_ID: {
        "supervision_period_id": _DEFAULT_SUPERVISION_PERIOD_ID,
        "judicial_district_code": "XXX",
    }
}


class TestGetCommitmentDetails(unittest.TestCase):
    """Tests the get_commitment_from_supervision_details function."""

    @staticmethod
    def _test_get_commitment_from_supervision_details(
        incarceration_period: StateIncarcerationPeriod,
        supervision_period: Optional[StateSupervisionPeriod] = None,
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
        supervision_period_to_agent_associations: Optional[
            Dict[int, Dict[Any, Any]]
        ] = None,
    ):

        violation_responses = violation_responses or []
        supervision_period_to_agent_associations = (
            supervision_period_to_agent_associations
            or DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        return commitment_from_supervision_utils.get_commitment_from_supervision_details(
            incarceration_period=incarceration_period,
            pre_commitment_supervision_period=supervision_period,
            violation_responses=violation_responses,
            supervision_period_to_agent_associations=supervision_period_to_agent_associations,
        )

    def test_get_commitment_from_supervision_details(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="DISTRICT 999",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period, supervision_period
        )

        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_us_nd(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="X",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_ND",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period, supervision_period
        )
        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_us_mo(self):
        state_code = "US_MO"

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code=state_code,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="DISTRICT 999",
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="MIN",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period, supervision_period
        )

        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_no_supervision_period_us_mo(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_MO",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period
        )

        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=None,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=None,
                supervision_level_raw_text=None,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_no_revocation_type_us_nd(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="X",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_ND",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period, supervision_period
        )

        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_us_pa_pvc(self):
        state_code = "US_PA"

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code=state_code, case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            state_code=state_code,
            supervision_site="DISTRICT_1|OFFICE_2|ORG_CODE",
            start_date=date(2017, 12, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=state_code,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2018, 3, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            # Program 26 indicates a revocation to a PVC
            specialized_purpose_for_incarceration_raw_text=PURPOSE_FOR_INCARCERATION_PVC,
            release_date=date(2019, 5, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
        )

        # TODO(#6314): Don't send in this temporary reference
        temporary_sp_agent_associations = {
            _DEFAULT_SUPERVISION_PERIOD_ID: {
                "agent_id": 123,
                "agent_external_id": "DISTRICT_1|OFFICE_2|ORG_CODE#123: JACK STONE",
                "supervision_period_id": _DEFAULT_SUPERVISION_PERIOD_ID,
            }
        }

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period,
            supervision_period,
            # TODO(#6314): Don't send in this temporary reference
            supervision_period_to_agent_associations=temporary_sp_agent_associations,
        )

        self.assertEqual(
            commitment_details,
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype="PVC",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="123: JACK STONE",
                # TODO(#6314): Remove other value for supervising_officer_external_id and use:
                # supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                #     supervision_period.supervision_period_id
                # ).get(
                #     "agent_external_id"
                # ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
            ),
        )


class TestDefaultViolationHistoryWindowPreCommitmentFromSupervision(unittest.TestCase):
    """Tests the default_violation_history_window_pre_commitment_from_supervision
    function."""

    def test_default_violation_history_window_pre_commitment_from_supervision(
        self,
    ):
        state_code = "US_XX"

        supervision_violation_response_1 = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=123,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2008, 12, 7),
            )
        )

        supervision_violation_response_2 = (
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=234,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code=state_code,
                response_date=date(2009, 11, 13),
            )
        )

        supervision_violation_response_3 = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=345,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2009, 12, 1),
            )
        )

        violation_window = (
            default_violation_history_window_pre_commitment_from_supervision(
                admission_date=date(2009, 12, 14),
                sorted_and_filtered_violation_responses=[
                    supervision_violation_response_1,
                    supervision_violation_response_2,
                    supervision_violation_response_3,
                ],
            )
        )

        expected_violation_window = DateRange(
            lower_bound_inclusive_date=date(2008, 12, 1),
            upper_bound_exclusive_date=date(2009, 12, 2),
        )

        self.assertEqual(expected_violation_window, violation_window)

    def test_default_violation_history_window_pre_commitment_from_supervision_filter_after(
        self,
    ):
        state_code = "US_XX"

        supervision_violation_response_1 = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=123,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2008, 12, 7),
            )
        )

        supervision_violation_response_2 = (
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=234,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code=state_code,
                response_date=date(2009, 11, 13),
            )
        )

        # This is after the admission_date
        supervision_violation_response_3 = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=345,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2012, 12, 1),
            )
        )

        violation_window = (
            default_violation_history_window_pre_commitment_from_supervision(
                admission_date=date(2009, 12, 14),
                sorted_and_filtered_violation_responses=[
                    supervision_violation_response_1,
                    supervision_violation_response_2,
                    supervision_violation_response_3,
                ],
            )
        )

        expected_violation_window = DateRange(
            lower_bound_inclusive_date=date(2008, 11, 13),
            upper_bound_exclusive_date=date(2009, 11, 14),
        )

        self.assertEqual(expected_violation_window, violation_window)

    def test_default_violation_history_window_pre_commitment_from_supervision_no_responses(
        self,
    ):
        violation_window = (
            default_violation_history_window_pre_commitment_from_supervision(
                admission_date=date(2009, 12, 14),
                sorted_and_filtered_violation_responses=[],
            )
        )

        expected_violation_window = DateRange(
            lower_bound_inclusive_date=date(2008, 12, 14),
            upper_bound_exclusive_date=date(2009, 12, 15),
        )

        self.assertEqual(expected_violation_window, violation_window)
