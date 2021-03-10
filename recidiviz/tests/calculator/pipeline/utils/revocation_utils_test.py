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
"""Tests functions in the revocation_utils file."""
import unittest
from datetime import date
from typing import NamedTuple, Optional, List, Dict, Any

from recidiviz.calculator.pipeline.utils import revocation_utils
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_revocation_utils import (
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
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseRevocationType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
    StateSupervisionViolation,
    StateSupervisionViolationTypeEntry,
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


RevocationDetails = NamedTuple(
    "RevocationDetails",
    [
        ("revocation_type", Optional[StateSupervisionViolationResponseRevocationType]),
        ("revocation_type_subtype", Optional[str]),
        ("source_violation_type", Optional[StateSupervisionViolationType]),
        ("supervising_officer_external_id", Optional[str]),
        ("level_1_supervision_location_external_id", Optional[str]),
        ("level_2_supervision_location_external_id", Optional[str]),
    ],
)


class TestGetRevocationDetails(unittest.TestCase):
    """Tests the get_revocation_details function."""

    @staticmethod
    def _test_get_revocation_details(
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

        return revocation_utils.get_revocation_details(
            incarceration_period=incarceration_period,
            supervision_period=supervision_period,
            violation_responses=violation_responses,
            supervision_period_to_agent_associations=supervision_period_to_agent_associations,
        )

    def test_get_revocation_details(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="DISTRICT 999",
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_XX",
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                )
            ],
        )

        source_supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_date=date(2018, 4, 23),
                supervision_violation=supervision_violation,
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            source_supervision_violation_response=source_supervision_violation_response,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        revocation_details = self._test_get_revocation_details(
            incarceration_period, supervision_period
        )

        self.assertEqual(
            RevocationDetails(
                revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                revocation_type_subtype=None,
                source_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id
                ).get(
                    "agent_external_id"
                ),
            ),
            revocation_details,
        )

    def test_get_revocation_details_us_nd(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="X",
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_ND",
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_ND",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                )
            ],
        )

        source_supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_ND",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_date=date(2018, 4, 23),
                supervision_violation=supervision_violation,
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_ND",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            source_supervision_violation_response=source_supervision_violation_response,
        )

        revocation_details = self._test_get_revocation_details(
            incarceration_period, supervision_period
        )
        self.assertEqual(
            RevocationDetails(
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                revocation_type_subtype=None,
                source_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id
                ).get(
                    "agent_external_id"
                ),
            ),
            revocation_details,
        )

    def test_get_revocation_details_us_mo(self):
        state_code = "US_MO"

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code=state_code,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="DISTRICT 999",
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

        revocation_details = self._test_get_revocation_details(
            incarceration_period, supervision_period
        )

        self.assertEqual(
            RevocationDetails(
                revocation_type=StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON,
                revocation_type_subtype=None,
                source_violation_type=None,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id
                ).get(
                    "agent_external_id"
                ),
            ),
            revocation_details,
        )

    def test_get_revocation_details_no_supervision_period_us_mo(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_MO",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        revocation_details = self._test_get_revocation_details(incarceration_period)

        self.assertEqual(
            RevocationDetails(
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                revocation_type_subtype=None,
                source_violation_type=None,
                level_1_supervision_location_external_id=None,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=None,
            ),
            revocation_details,
        )

    def test_get_revocation_details_no_revocation_type_us_nd(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="X",
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_ND",
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_ND",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                )
            ],
        )

        source_supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_ND",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_date=date(2018, 4, 23),
                revocation_type=None,
                supervision_violation=supervision_violation,
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_ND",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            source_supervision_violation_response=source_supervision_violation_response,
        )

        revocation_details = self._test_get_revocation_details(
            incarceration_period, supervision_period
        )

        self.assertEqual(
            RevocationDetails(
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                revocation_type_subtype=None,
                source_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id
                ).get(
                    "agent_external_id"
                ),
            ),
            revocation_details,
        )

    def test_get_revocation_details_us_pa_pvc(self):
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

        revocation_details = self._test_get_revocation_details(
            incarceration_period,
            supervision_period,
            # TODO(#6314): Don't send in this temporary reference
            supervision_period_to_agent_associations=temporary_sp_agent_associations,
        )

        self.assertEqual(
            revocation_details,
            RevocationDetails(
                revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                revocation_type_subtype="PVC",
                source_violation_type=None,
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="123: JACK STONE",
                # TODO(#6314): Remove other value for supervising_officer_external_id and use:
                # supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                #     supervision_period.supervision_period_id
                # ).get(
                #     "agent_external_id"
                # ),
            ),
        )
