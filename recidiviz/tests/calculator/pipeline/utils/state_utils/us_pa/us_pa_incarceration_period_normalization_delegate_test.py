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
"""Tests the us_pa_incarceration_period_normalization_delegate.py."""
import unittest
from datetime import date
from typing import Dict, List, Optional, Tuple

import attr

from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa import (
    us_pa_incarceration_period_normalization_delegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_delegate import (
    UsPaIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_period_normalization_delegate import (
    PURPOSE_FOR_INCARCERATION_PVC,
    SHOCK_INCARCERATION_6_MONTHS,
    SHOCK_INCARCERATION_9_MONTHS,
    SHOCK_INCARCERATION_12_MONTHS,
    SHOCK_INCARCERATION_PVC,
    SHOCK_INCARCERATION_UNDER_6_MONTHS,
    UsPaIncarcerationNormalizationDelegate,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
)
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)

STATE_CODE = "US_PA"


class TestNormalizedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_ID-specific aspects of the
    normalized_incarceration_periods_for_calculations function on the
    UsIdIncarcerationNormalizationManager."""

    @staticmethod
    def _normalized_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
        collapse_transfers: bool = False,
        overwrite_facility_information_in_transfers: bool = False,
        earliest_death_date: Optional[date] = None,
    ) -> Tuple[List[StateIncarcerationPeriod], Dict[int, Optional[str]]]:
        """Helper function for testing the
        normalized_incarceration_periods_for_calculations function for US_PA."""
        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        violation_responses = violation_responses or []

        ip_normalization_manager = IncarcerationPeriodNormalizationManager(
            incarceration_periods=incarceration_periods,
            normalization_delegate=UsPaIncarcerationNormalizationDelegate(),
            incarceration_delegate=UsPaIncarcerationDelegate(),
            normalized_supervision_period_index=sp_index,
            violation_responses=violation_responses,
            earliest_death_date=earliest_death_date,
            field_index=CoreEntityFieldIndex(),
        )

        ip_index = ip_normalization_manager.normalized_incarceration_period_index_for_calculations(
            collapse_transfers=collapse_transfers,
            overwrite_facility_information_in_transfers=overwrite_facility_information_in_transfers,
        )

        return ip_index.incarceration_periods, ip_index.ip_id_to_pfi_subtype

    def test_normalized_incarceration_periods_shock_incarceration_RESCR(
        self,
    ) -> None:
        parole_board_decision_entry = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_UNDER_6_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2018, month=5, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry],
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            # Program 46 indicates a commitment to a 6, 9 or 12 month stay
            specialized_purpose_for_incarceration_raw_text="CCIS-46",
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_pfi_subtype = SHOCK_INCARCERATION_UNDER_6_MONTHS

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[parole_board_permanent_decision],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    # TODO(#8961): remove this test when the ingest mappings are updated and logic that is being tested is removed
    def test_normalized_incarceration_periods_ccc_period_not_included_in_state(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="CCIS-60",
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )
        incarceration_period.custodial_authority = (
            StateCustodialAuthority.SUPERVISION_AUTHORITY
        )
        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
        )

        (
            validated_incarceration_periods,
            _,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    # TODO(#9421): This test may need to be removed if once there is a solid plan for for all community facilities
    #   if that plan says that there should be no CCC period with INTERNAL_UNKNOWN specialized_purpose_for_incarceration
    def test_normalized_incarceration_periods_internal_unknown_type(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration_raw_text="CCIS-60",
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )
        incarceration_period.custodial_authority = (
            StateCustodialAuthority.SUPERVISION_AUTHORITY
        )

        (
            validated_incarceration_periods,
            _,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[],
        )

        self.assertEqual([incarceration_period], validated_incarceration_periods)

    # TODO(#8961): Remove this test when the ingest mappings are updated to always
    #  set admission reason to INTERNAL_UNKNOWN.
    def test_normalized_incarceration_periods_ccc_period_transfer_admission_reason(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration_raw_text="CCIS-60",
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )
        incarceration_period.custodial_authority = (
            StateCustodialAuthority.SUPERVISION_AUTHORITY
        )

        (
            validated_incarceration_periods,
            _,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[],
        )

        self.assertEqual([incarceration_period], validated_incarceration_periods)

    def test_normalized_incarceration_periods_shock_incarceration_RESCR6(
        self,
    ) -> None:
        parole_board_decision_entry = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_6_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2018, month=5, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry],
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            # Program 46 indicates a commitment to a 6, 9 or 12 month stay
            specialized_purpose_for_incarceration_raw_text="CCIS-46",
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_pfi_subtype = SHOCK_INCARCERATION_6_MONTHS

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[parole_board_permanent_decision],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_shock_incarceration_RESCR9(
        self,
    ) -> None:
        parole_board_decision_entry = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_9_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2018, month=5, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry],
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            # Program 46 indicates a commitment to a 6, 9 or 12 month stay
            specialized_purpose_for_incarceration_raw_text="CCIS-46",
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_pfi_subtype = SHOCK_INCARCERATION_9_MONTHS

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[parole_board_permanent_decision],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_shock_incarceration_RESCR12(
        self,
    ) -> None:
        parole_board_decision_entry = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_12_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2018, month=5, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry],
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            # Program 46 indicates a commitment to a 6, 9 or 12 month stay
            specialized_purpose_for_incarceration_raw_text="CCIS-46",
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_pfi_subtype = SHOCK_INCARCERATION_12_MONTHS

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[parole_board_permanent_decision],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_shock_incarceration_no_set_subtype(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            # Program 46 indicates a commitment to a 6, 9 or 12 month stay
            specialized_purpose_for_incarceration_raw_text="CCIS-46",
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_pfi_subtype = SHOCK_INCARCERATION_UNDER_6_MONTHS

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_shock_incarceration_sci_no_set_subtype(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_pfi_subtype = SHOCK_INCARCERATION_UNDER_6_MONTHS

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_shock_incarceration_sci_with_board_actions(
        self,
    ) -> None:
        parole_board_decision_entry = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_12_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2018, month=5, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry],
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_pfi_subtype = SHOCK_INCARCERATION_12_MONTHS

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[parole_board_permanent_decision],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_reincarceration(self) -> None:
        parole_board_decision_entry = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text="XXX",
            )
        )

        parole_board_permanent_decision = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2018, month=5, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry],
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
        )

        expected_pfi_subtype = None

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[parole_board_permanent_decision],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_reincarceration_no_board_actions(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
        )

        expected_pfi_subtype = None

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_PVC(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            # Program 26 indicates a stay in a PVC
            specialized_purpose_for_incarceration_raw_text=PURPOSE_FOR_INCARCERATION_PVC,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        expected_pfi_subtype = SHOCK_INCARCERATION_PVC

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_treatment(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_pfi_subtype = None

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_treatment_51(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            specialized_purpose_for_incarceration_raw_text="CCIS-51",
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_pfi_subtype = None

        (
            validated_incarceration_periods,
            ip_id_to_pfi_subtype,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[incarceration_period],
            violation_responses=[],
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
        self.assertEqual(
            expected_pfi_subtype,
            ip_id_to_pfi_subtype[222],
        )

    def test_normalized_incarceration_periods_revocation_admission_v1(
        self,
    ) -> None:
        """Tests that admission_reason_raw_text values from the v1 version of the
        sci_incarceration_period view parse in IP pre-processing."""
        state_code = "US_PA"
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="TPV-TRUE-APV",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        period_copy = attr.evolve(incarceration_period)

        incarceration_periods = [
            incarceration_period,
        ]

        (
            validated_incarceration_periods,
            _,
        ) = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods,
            collapse_transfers=False,
        )

        self.assertEqual([period_copy], validated_incarceration_periods)


# pylint: disable=protected-access
class TestPurposeForIncarcerationTypeSubtypeFromParoleDecisions(unittest.TestCase):
    """Tests the _purpose_for_incarceration_subtype function."""

    def test_purpose_for_incarceration_subtype(self) -> None:
        parole_board_decision_entry_old = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_UNDER_6_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision_outside_window = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2018, month=5, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry_old],
        )

        parole_board_decision_entry_new = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_12_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision_in_window = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2019, month=12, day=30),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry_new],
        )

        commitment_admission_date = date(2020, 1, 1)
        specialized_purpose_for_incarceration_raw_text = "CCIS-46"

        purpose_for_incarceration_subtype = us_pa_incarceration_period_normalization_delegate._purpose_for_incarceration_subtype(
            commitment_admission_date,
            specialized_purpose_for_incarceration_raw_text,
            [
                parole_board_permanent_decision_outside_window,
                parole_board_permanent_decision_in_window,
            ],
        )

        self.assertEqual(
            SHOCK_INCARCERATION_12_MONTHS, purpose_for_incarceration_subtype
        )

    def test_purpose_for_incarceration_subtype_pvc(self) -> None:
        commitment_admission_date = date(2020, 1, 1)
        specialized_purpose_for_incarceration_raw_text = PURPOSE_FOR_INCARCERATION_PVC

        purpose_for_incarceration_subtype = us_pa_incarceration_period_normalization_delegate._purpose_for_incarceration_subtype(
            commitment_admission_date,
            specialized_purpose_for_incarceration_raw_text,
            [],
        )

        self.assertEqual(SHOCK_INCARCERATION_PVC, purpose_for_incarceration_subtype)

    def test_purpose_for_incarceration_subtype_no_parole_decisions(self) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2018, month=5, day=16),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
        )

        commitment_admission_date = date(2020, 1, 1)

        purpose_for_incarceration_subtype = us_pa_incarceration_period_normalization_delegate._purpose_for_incarceration_subtype(
            commitment_admission_date, None, [violation_response]
        )

        self.assertIsNone(purpose_for_incarceration_subtype)

    def test_purpose_for_incarceration_subtype_after_revocations(self) -> None:
        parole_board_decision_entry = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_UNDER_6_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision_outside_window = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2020, month=5, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry],
        )

        commitment_admission_date = date(2020, 1, 1)

        purpose_for_incarceration_subtype = us_pa_incarceration_period_normalization_delegate._purpose_for_incarceration_subtype(
            commitment_admission_date,
            None,
            [parole_board_permanent_decision_outside_window],
        )

        self.assertIsNone(purpose_for_incarceration_subtype)

    def test_purpose_for_incarceration_subtype_two_same_day(self) -> None:
        """Tests that the longer shock incarceration length is taken from two parole board actions that happened on
        the same day."""
        parole_board_decision_entry_1 = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_9_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision_1 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2019, month=12, day=30),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry_1],
        )

        parole_board_decision_entry_2 = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_6_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision_2 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2019, month=12, day=30),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry_2],
        )

        commitment_admission_date = date(2020, 1, 1)

        purpose_for_incarceration_subtype = us_pa_incarceration_period_normalization_delegate._purpose_for_incarceration_subtype(
            commitment_admission_date,
            None,
            [parole_board_permanent_decision_1, parole_board_permanent_decision_2],
        )

        self.assertEqual(
            SHOCK_INCARCERATION_9_MONTHS, purpose_for_incarceration_subtype
        )

        purpose_for_incarceration_subtype = us_pa_incarceration_period_normalization_delegate._purpose_for_incarceration_subtype(
            commitment_admission_date,
            None,
            [parole_board_permanent_decision_2, parole_board_permanent_decision_1],
        )

        self.assertEqual(
            SHOCK_INCARCERATION_9_MONTHS, purpose_for_incarceration_subtype
        )

    def test_purpose_for_incarceration_subtype_no_responses(self) -> None:
        commitment_admission_date = date(2020, 1, 1)

        purpose_for_incarceration_subtype = us_pa_incarceration_period_normalization_delegate._purpose_for_incarceration_subtype(
            commitment_admission_date, None, []
        )

        self.assertIsNone(purpose_for_incarceration_subtype)


# pylint: disable=protected-access
class TestMostSevereRevocationTypeSubtype(unittest.TestCase):
    """Tests the _most_severe_purpose_for_incarceration_subtype function."""

    def test_most_severe_purpose_for_incarceration_subtype(self) -> None:
        parole_board_decision_entry_1 = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_9_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision_1 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2019, month=12, day=30),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry_1],
        )

        parole_board_decision_entry_2 = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_6_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision_2 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2019, month=12, day=30),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry_2],
        )

        (
            purpose_for_incarceration_subtype
        ) = us_pa_incarceration_period_normalization_delegate._most_severe_purpose_for_incarceration_subtype(
            [parole_board_permanent_decision_1, parole_board_permanent_decision_2]
        )

        self.assertEqual(
            SHOCK_INCARCERATION_9_MONTHS, purpose_for_incarceration_subtype
        )

    def test_most_severe_purpose_for_incarceration_subtype_invalid_type(self) -> None:
        parole_board_decision_entry_1 = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text="XXX",
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision_1 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2019, month=12, day=30),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry_1],
        )

        parole_board_decision_entry_2 = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=STATE_CODE,
                decision_raw_text=SHOCK_INCARCERATION_6_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
        )

        parole_board_permanent_decision_2 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            response_date=date(year=2019, month=12, day=30),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry_2],
        )

        purpose_for_incarceration_subtype = us_pa_incarceration_period_normalization_delegate._most_severe_purpose_for_incarceration_subtype(
            [parole_board_permanent_decision_1, parole_board_permanent_decision_2]
        )

        self.assertEqual(
            SHOCK_INCARCERATION_6_MONTHS, purpose_for_incarceration_subtype
        )

    def test_most_severe_purpose_for_incarceration_subtype_no_responses(self) -> None:
        purpose_for_incarceration_subtype = us_pa_incarceration_period_normalization_delegate._most_severe_purpose_for_incarceration_subtype(
            []
        )

        self.assertIsNone(purpose_for_incarceration_subtype)
