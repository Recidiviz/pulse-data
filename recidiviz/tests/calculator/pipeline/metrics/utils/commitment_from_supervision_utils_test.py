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
from typing import Any, Dict, List, Optional

from recidiviz.calculator.pipeline.metrics.utils import (
    commitment_from_supervision_utils,
)
from recidiviz.calculator.pipeline.metrics.utils.commitment_from_supervision_utils import (
    CommitmentDetails,
    period_is_commitment_from_supervision_admission_from_parole_board_hold,
)
from recidiviz.calculator.pipeline.metrics.utils.violation_utils import (
    VIOLATION_HISTORY_WINDOW_MONTHS,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_commitment_from_supervision_utils import (
    UsXxCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_delegate import (
    UsPaIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_period_normalization_delegate import (
    PURPOSE_FOR_INCARCERATION_PVC,
    SHOCK_INCARCERATION_PVC,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason as ReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.common.date import DateRange
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_ip_index_for_tests,
    default_normalized_sp_index_for_tests,
)

_DEFAULT_SUPERVISION_PERIOD_ID = 999
_DEFAULT_SUPERVISION_PERIOD_ID_2 = 888

DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS: Dict[int, Dict[Any, Any]] = {
    _DEFAULT_SUPERVISION_PERIOD_ID: {
        "agent_id": 123,
        "agent_external_id": "YYY",
        "supervision_period_id": _DEFAULT_SUPERVISION_PERIOD_ID,
    },
    _DEFAULT_SUPERVISION_PERIOD_ID_2: {
        "agent_id": 000,
        "agent_external_id": "XXX",
        "supervision_period_id": _DEFAULT_SUPERVISION_PERIOD_ID_2,
    },
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
        incarceration_period: NormalizedStateIncarcerationPeriod,
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]] = None,
        incarceration_period_index: Optional[NormalizedIncarcerationPeriodIndex] = None,
        supervision_period_to_agent_associations: Optional[
            Dict[int, Dict[Any, Any]]
        ] = None,
        supervision_delegate: StateSpecificSupervisionDelegate = UsXxSupervisionDelegate(),
    ) -> CommitmentDetails:
        """Helper function for testing get_commitment_from_supervision_details."""
        supervision_period_to_agent_associations = (
            supervision_period_to_agent_associations
            or DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )
        incarceration_period_index = (
            incarceration_period_index
            or default_normalized_ip_index_for_tests(
                incarceration_periods=[incarceration_period]
            )
        )

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        return commitment_from_supervision_utils.get_commitment_from_supervision_details(
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            supervision_period_index=supervision_period_index,
            # No state-agnostic tests require the sentences
            incarceration_sentences=[],
            supervision_sentences=[],
            commitment_from_supervision_delegate=UsXxCommitmentFromSupervisionDelegate(),
            supervision_delegate=supervision_delegate,
            supervision_period_to_agent_associations=supervision_period_to_agent_associations,
        )

    def test_get_commitment_from_supervision_details(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_site="DISTRICT 999",
        )

        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period, [supervision_period]
        )

        assert supervision_period.supervision_period_id is not None
        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id, {}
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_from_board_hold(self) -> None:
        terminated_supervision_period = (
            NormalizedStateSupervisionPeriod.new_with_defaults(
                supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
                sequence_num=0,
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervision_site="DISTRICT 999",
            )
        )

        overlapping_supervision_period = (
            NormalizedStateSupervisionPeriod.new_with_defaults(
                supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID_2,
                sequence_num=1,
                state_code="US_XX",
                start_date=date(2018, 5, 19),
                termination_date=date(2018, 8, 20),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervision_site="DISTRICT X",
            )
        )

        board_hold = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            sequence_num=0,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 6, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2018, 8, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        revocation_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            sequence_num=1,
            external_id="ip2",
            state_code="US_XX",
            admission_date=date(2018, 8, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        ip_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[board_hold, revocation_period]
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            revocation_period,
            [terminated_supervision_period, overlapping_supervision_period],
            incarceration_period_index=ip_index,
        )

        assert overlapping_supervision_period.supervision_period_id is not None
        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=overlapping_supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    overlapping_supervision_period.supervision_period_id, {}
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=overlapping_supervision_period.supervision_level,
                supervision_level_raw_text=overlapping_supervision_period.supervision_level_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_two_sps_not_board_hold(
        self,
    ) -> None:
        terminated_supervision_period = (
            NormalizedStateSupervisionPeriod.new_with_defaults(
                supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
                sequence_num=0,
                state_code="US_XX",
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_site="DISTRICT 999",
            )
        )

        overlapping_supervision_period = (
            NormalizedStateSupervisionPeriod.new_with_defaults(
                supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID_2,
                sequence_num=1,
                state_code="US_XX",
                start_date=date(2018, 5, 19),
                termination_date=date(2018, 10, 4),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_site="DISTRICT X",
            )
        )

        revocation_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_XX",
            admission_date=date(2018, 8, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        ip_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[revocation_period]
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            revocation_period,
            [terminated_supervision_period, overlapping_supervision_period],
            incarceration_period_index=ip_index,
        )

        assert terminated_supervision_period.supervision_period_id is not None
        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=terminated_supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    terminated_supervision_period.supervision_period_id, {}
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=terminated_supervision_period.supervision_level,
                supervision_level_raw_text=terminated_supervision_period.supervision_level_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_us_nd(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_site="X",
        )

        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_ND",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period,
            [supervision_period],
        )
        assert supervision_period.supervision_period_id is not None
        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id, {}
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_no_supervision_period(
        self,
    ) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period,
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
                supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_us_pa_pvc(self) -> None:
        state_code = "US_PA"

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code=state_code, case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            state_code=state_code,
            supervision_site="DISTRICT_1|OFFICE_2|ORG_CODE",
            start_date=date(2017, 12, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=state_code,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 3, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            # Program 26 indicates a revocation to a PVC
            specialized_purpose_for_incarceration_raw_text=PURPOSE_FOR_INCARCERATION_PVC,
            release_date=date(2019, 5, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
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
            supervision_delegate=UsPaSupervisionDelegate(),
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
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    supervision_period.supervision_period_id, {}
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
        )

    def test_get_commitment_from_supervision_details_transfer_on_admission(
        self,
    ) -> None:
        """Tests that the period *prior to the incarceration admission* is chosen
        when a person is transferred to a new supervision period on the date of an
        admission to incarceration."""
        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            admission_date=date(2020, 1, 1),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        pre_commitment_sp = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            sequence_num=0,
            state_code="US_XX",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=date(2010, 12, 1),
            termination_date=incarceration_period.admission_date,
        )

        supervision_period_while_in_prison = (
            NormalizedStateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                sequence_num=1,
                state_code="US_XX",
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                start_date=incarceration_period.admission_date,
            )
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            incarceration_period,
            [pre_commitment_sp, supervision_period_while_in_prison],
        )
        assert pre_commitment_sp.supervision_period_id is not None
        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=pre_commitment_sp.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    pre_commitment_sp.supervision_period_id, {}
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=pre_commitment_sp.supervision_level,
                supervision_level_raw_text=pre_commitment_sp.supervision_level_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            commitment_details,
        )

    def test_get_commitment_from_supervision_details_periods_adjacent_board_holds(
        self,
    ) -> None:
        pre_commitment_sp = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            state_code="US_XX",
            start_date=date(2016, 3, 5),
            termination_date=date(2016, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_site="DISTRICT 999",
        )

        bh_1 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            sequence_num=0,
            state_code="US_XX",
            admission_date=date(2016, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2018, 5, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        bh_2 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=2222,
            sequence_num=1,
            state_code="US_XX",
            admission_date=date(2018, 5, 21),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2018, 5, 25),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        rev_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            sequence_num=2,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        ip_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[bh_1, bh_2, rev_period]
        )

        commitment_details = self._test_get_commitment_from_supervision_details(
            rev_period,
            [pre_commitment_sp],
            incarceration_period_index=ip_index,
        )

        assert pre_commitment_sp.supervision_period_id is not None
        self.assertEqual(
            CommitmentDetails(
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype=None,
                level_1_supervision_location_external_id=pre_commitment_sp.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                    pre_commitment_sp.supervision_period_id, {}
                ).get(
                    "agent_external_id"
                ),
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=pre_commitment_sp.supervision_level,
                supervision_level_raw_text=pre_commitment_sp.supervision_level_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            commitment_details,
        )


class TestDefaultViolationHistoryWindowPreCommitmentFromSupervision(unittest.TestCase):
    """Tests the default behavior of the
    violation_history_window_pre_commitment_from_supervision function on the
    StateSpecificCommitmentFromSupervisionDelegate."""

    def test_default_violation_history_window_pre_commitment_from_supervision(
        self,
    ) -> None:
        state_code = "US_XX"

        supervision_violation_response_1 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=123,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2008, 12, 7),
            )
        )

        supervision_violation_response_2 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=234,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code=state_code,
                response_date=date(2009, 11, 13),
            )
        )

        supervision_violation_response_3 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=345,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2009, 12, 1),
            )
        )

        violation_window = UsXxCommitmentFromSupervisionDelegate().violation_history_window_pre_commitment_from_supervision(
            admission_date=date(2009, 12, 14),
            sorted_and_filtered_violation_responses=[
                supervision_violation_response_1,
                supervision_violation_response_2,
                supervision_violation_response_3,
            ],
            default_violation_history_window_months=VIOLATION_HISTORY_WINDOW_MONTHS,
        )

        expected_violation_window = DateRange(
            lower_bound_inclusive_date=date(2008, 12, 1),
            upper_bound_exclusive_date=date(2009, 12, 2),
        )

        self.assertEqual(expected_violation_window, violation_window)

    def test_default_violation_history_window_pre_commitment_from_supervision_filter_after(
        self,
    ) -> None:
        state_code = "US_XX"

        supervision_violation_response_1 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=123,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2008, 12, 7),
            )
        )

        supervision_violation_response_2 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=234,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code=state_code,
                response_date=date(2009, 11, 13),
            )
        )

        # This is after the admission_date
        supervision_violation_response_3 = (
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=345,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2012, 12, 1),
            )
        )

        violation_window = UsXxCommitmentFromSupervisionDelegate().violation_history_window_pre_commitment_from_supervision(
            admission_date=date(2009, 12, 14),
            sorted_and_filtered_violation_responses=[
                supervision_violation_response_1,
                supervision_violation_response_2,
                supervision_violation_response_3,
            ],
            default_violation_history_window_months=VIOLATION_HISTORY_WINDOW_MONTHS,
        )

        expected_violation_window = DateRange(
            lower_bound_inclusive_date=date(2008, 11, 13),
            upper_bound_exclusive_date=date(2009, 11, 14),
        )

        self.assertEqual(expected_violation_window, violation_window)

    def test_default_violation_history_window_pre_commitment_from_supervision_no_responses(
        self,
    ) -> None:
        violation_window = UsXxCommitmentFromSupervisionDelegate().violation_history_window_pre_commitment_from_supervision(
            admission_date=date(2009, 12, 14),
            sorted_and_filtered_violation_responses=[],
            default_violation_history_window_months=VIOLATION_HISTORY_WINDOW_MONTHS,
        )

        expected_violation_window = DateRange(
            lower_bound_inclusive_date=date(2008, 12, 14),
            upper_bound_exclusive_date=date(2009, 12, 15),
        )

        self.assertEqual(expected_violation_window, violation_window)


class TestCommitmentFromBoardHold(unittest.TestCase):
    """Tests the
    period_is_commitment_from_supervision_admission_from_parole_board_hold function."""

    def test_period_is_commitment_from_parole_board_hold(self) -> None:
        ip_1 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        ip_2 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 9, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2002, 9, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        self.assertTrue(
            period_is_commitment_from_supervision_admission_from_parole_board_hold(
                incarceration_period=ip_2, most_recent_board_hold_span=ip_1.duration
            )
        )

    def test_period_is_commitment_from_parole_board_hold_not_adjacent(self) -> None:
        ip_1 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        ip_2 = NormalizedStateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2010, 3, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2012, 9, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        self.assertFalse(
            period_is_commitment_from_supervision_admission_from_parole_board_hold(
                incarceration_period=ip_2, most_recent_board_hold_span=ip_1.duration
            )
        )
