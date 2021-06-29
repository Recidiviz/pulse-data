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
"""Tests the functions in the state_calculation_config_manager file."""
import unittest
from datetime import date
from typing import List, Optional, Tuple

import attr

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import (
    RevocationReturnSupervisionTimeBucket,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils import (
    state_calculation_config_manager,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_supervising_officer_and_location_info_from_supervision_period,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason as ReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)


class TestRevokedSupervisionPeriodsIfRevocationOccurred(unittest.TestCase):
    """Tests the state-specific pre_commitment_supervision_period_if_commitment function."""

    @staticmethod
    def _pre_commitment_supervision_period_if_commitment(
        incarceration_period: StateIncarcerationPeriod,
        sorted_incarceration_periods: Optional[List[StateIncarcerationPeriod]] = None,
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
    ) -> Tuple[bool, Optional[StateSupervisionPeriod]]:
        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods or []
        )
        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            sorted_incarceration_periods or [incarceration_period]
        )

        return state_calculation_config_manager.pre_commitment_supervision_period_if_commitment(
            state_code=incarceration_period.state_code,
            incarceration_period=incarceration_period,
            supervision_period_index=supervision_period_index,
            incarceration_period_index=incarceration_period_index,
        )

    def test_pre_commitment_supervision_period_if_commitment(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_XX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2019, 5, 29),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        (
            admission_is_commitment_from_supervision,
            pre_commitment_supervision_period,
        ) = self._pre_commitment_supervision_period_if_commitment(incarceration_period)

        self.assertTrue(admission_is_commitment_from_supervision)
        self.assertIsNone(pre_commitment_supervision_period)

    def test_pre_commitment_supervision_period_if_commitment_US_ID_FilterSupervision(
        self,
    ) -> None:
        """Tests that supervision periods without set
        supervision_period_supervision_type values get filtered out from being
        identified as pre_commitment supervision periods."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=None,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2020, 1, 9),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        (
            admission_is_commitment_from_supervision,
            pre_commitment_supervision_period,
        ) = self._pre_commitment_supervision_period_if_commitment(
            incarceration_period, supervision_periods=[supervision_period]
        )

        self.assertTrue(admission_is_commitment_from_supervision)
        self.assertIsNone(pre_commitment_supervision_period)

    def test_pre_commitment_supervision_period_if_commitment_US_ND_NewAdmissionNotAfterProbation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        (
            admission_is_commitment_from_supervision,
            pre_commitment_supervision_period,
        ) = self._pre_commitment_supervision_period_if_commitment(
            incarceration_period, supervision_periods=[supervision_period]
        )

        self.assertFalse(admission_is_commitment_from_supervision)
        self.assertIsNone(pre_commitment_supervision_period)

    def test_pre_commitment_supervision_period_if_commitment_US_ND_NewAdmissionNotAfterRevocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        (
            admission_is_commitment_from_supervision,
            pre_commitment_supervision_period,
        ) = self._pre_commitment_supervision_period_if_commitment(
            incarceration_period, supervision_periods=[supervision_period]
        )

        self.assertFalse(admission_is_commitment_from_supervision)
        self.assertIsNone(pre_commitment_supervision_period)

    def test_pre_commitment_supervision_period_if_commitment_US_ND_NewAdmissionAfterProbationRevocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        (
            admission_is_commitment_from_supervision,
            pre_commitment_supervision_period,
        ) = self._pre_commitment_supervision_period_if_commitment(
            incarceration_period, supervision_periods=[supervision_period]
        )

        self.assertTrue(admission_is_commitment_from_supervision)
        self.assertEqual(supervision_period, pre_commitment_supervision_period)

    def test_pre_commitment_supervision_period_if_commitment_US_ND_RevocationAdmission(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        )

        (
            admission_is_commitment_from_supervision,
            pre_commitment_supervision_period,
        ) = self._pre_commitment_supervision_period_if_commitment(
            incarceration_period, supervision_periods=[supervision_period]
        )

        self.assertTrue(admission_is_commitment_from_supervision)
        self.assertEqual(supervision_period, pre_commitment_supervision_period)

    def test_pre_commitment_supervision_period_if_commitment_US_ND_RevocationAdmissionNoSupervisionPeriod(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        )

        (
            admission_is_commitment_from_supervision,
            pre_commitment_supervision_period,
        ) = self._pre_commitment_supervision_period_if_commitment(
            incarceration_period,
        )

        self.assertTrue(admission_is_commitment_from_supervision)
        self.assertIsNone(pre_commitment_supervision_period)

    def test_pre_commitment_supervision_period_if_commitment_transfer_US_PA(
        self,
    ) -> None:
        """It's common for people on parole in US_PA to be transferred to a new
        supervision period on the date of their sanction admission to incarceration
        for shock incarceration. This tests that the period that ended on the
        admission_date is chose, and not the one that started on the admission_date."""
        state_code = "US_PA"

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=state_code,
            incarceration_period_id=111,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            admission_date=date(2020, 1, 1),
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        revoked_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code=state_code,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=date(2010, 12, 1),
            termination_date=incarceration_period.admission_date,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_while_in_prison = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            state_code=state_code,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=incarceration_period.admission_date,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        (
            admission_is_commitment_from_supervision,
            pre_commitment_supervision_period,
        ) = self._pre_commitment_supervision_period_if_commitment(
            incarceration_period,
            supervision_periods=[
                revoked_supervision_period,
                supervision_period_while_in_prison,
            ],
        )

        self.assertTrue(admission_is_commitment_from_supervision)
        self.assertEqual(revoked_supervision_period, pre_commitment_supervision_period)


class TestGetSupervisingOfficerAndLocationInfoFromSupervisionPeriod(unittest.TestCase):
    """Tests the state-specific get_supervising_officer_and_location_info_from_supervision_period function."""

    DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE: StateSupervisionPeriod = StateSupervisionPeriod.new_with_defaults(
        supervision_period_id=111,
        external_id="sp1",
        status=StateSupervisionPeriodStatus.TERMINATED,
        state_code="US_XX",
        start_date=date(2017, 3, 5),
        termination_date=date(2017, 5, 9),
        supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
    )

    DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS = {
        111: {
            "agent_id": 123,
            "agent_external_id": "agent_external_id_1",
            "supervision_period_id": 111,
        }
    }

    def test_get_supervising_officer_and_location_info_from_supervision_period(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE, supervision_site="1"
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("1", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_no_site(
        self,
    ) -> None:
        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_no_agent(self) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            supervision_period_id=666,  # No mapping for this ID
            supervision_site="1",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(None, supervising_officer_external_id)
        self.assertEqual("1", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_ID(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site="DISTRICT OFFICE 6, POCATELLO|UNKNOWN",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("UNKNOWN", level_1_supervision_location)
        self.assertEqual("DISTRICT OFFICE 6, POCATELLO", level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site="PAROLE COMMISSION OFFICE|DEPORTED",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("DEPORTED", level_1_supervision_location)
        self.assertEqual("PAROLE COMMISSION OFFICE", level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site="DISTRICT OFFICE 4, BOISE|",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual("DISTRICT OFFICE 4, BOISE", level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ID",
            supervision_site=None,
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_PA(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_PA",
            # TODO(#6314): Change this back to "CO|CO - CENTRAL OFFICE|9110"
            supervision_site=None,
        )

        # TODO(#6314): Delete this and send in the DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        #  instead once we've removed the hack
        temporary_sp_agent_associations = {
            111: {
                "agent_id": 123,
                "agent_external_id": "CO|CO - CENTRAL OFFICE|9110#999: JOHN SNOW",
                "supervision_period_id": 111,
            }
        }

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, temporary_sp_agent_associations
        )

        self.assertEqual("999: JOHN SNOW", supervising_officer_external_id)
        self.assertEqual("CO - CENTRAL OFFICE", level_1_supervision_location)
        self.assertEqual("CO", level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_PA",
            supervision_site=None,
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        # TODO(#6314): Return to asserting that the officer is "agent_external_id_1"
        self.assertEqual(None, supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

        # TODO(#6314): Delete the rest of this test, since it's only testing the hack
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_PA",
            supervision_site=None,
        )

        temporary_sp_agent_associations = {
            111: {
                "agent_id": 123,
                # Only agent info, no supervision location info
                "agent_external_id": "#999: JOHN SNOW",
                "supervision_period_id": 111,
            }
        }

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, temporary_sp_agent_associations
        )

        self.assertEqual("999: JOHN SNOW", supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

        temporary_sp_agent_associations = {
            111: {
                "agent_id": 123,
                # Only supervision location info, no agent info
                "agent_external_id": "CO|CO - CENTRAL OFFICE|9110#",
                "supervision_period_id": 111,
            }
        }

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, temporary_sp_agent_associations
        )

        self.assertEqual(None, supervising_officer_external_id)
        self.assertEqual("CO - CENTRAL OFFICE", level_1_supervision_location)
        self.assertEqual("CO", level_2_supervision_location)

        temporary_sp_agent_associations = {
            111: {
                "agent_id": 123,
                # Only level_2 location info
                "agent_external_id": "07||#",
                "supervision_period_id": 111,
            }
        }

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, temporary_sp_agent_associations
        )

        self.assertEqual(None, supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual("07", level_2_supervision_location)

        temporary_sp_agent_associations = {
            111: {
                "agent_id": 123,
                # Only level_2 location info, officer name has whitespace
                "agent_external_id": "07||#  ",
                "supervision_period_id": 111,
            }
        }

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, temporary_sp_agent_associations
        )

        self.assertEqual(None, supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual("07", level_2_supervision_location)

        temporary_sp_agent_associations = {
            111: {
                "agent_id": 123,
                # Empty info string
                "agent_external_id": "||#",
                "supervision_period_id": 111,
            }
        }

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, temporary_sp_agent_associations
        )

        self.assertEqual(None, supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_ND(
        self,
    ) -> None:
        nd_supervision_period_agent_associations = {
            111: {
                "agent_id": 123,
                "agent_external_id": "agent_external_id_1",
                "supervision_period_id": 111,
            }
        }

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_ND",
            supervision_site="SITE_1",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, nd_supervision_period_agent_associations
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("SITE_1", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_MO(
        self,
    ) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code="US_MO",
            supervision_site="04C",
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location,
            level_2_supervision_location,
        ) = get_supervising_officer_and_location_info_from_supervision_period(
            supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual("agent_external_id_1", supervising_officer_external_id)
        self.assertEqual("04C", level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)


class TestSupervisionPeriodIsOutOfState(unittest.TestCase):
    """Tests the state-specific supervision_period_is_out_of_state function."""

    def test_supervision_period_is_out_of_state_us_id_with_identifier(self) -> None:
        self.assertTrue(
            state_calculation_config_manager.supervision_period_is_out_of_state(
                self.create_time_bucket(
                    "US_ID", "INTERSTATE PROBATION - remainder of identifier"
                )
            )
        )

    def test_supervision_period_is_out_of_state_us_id_with_incorrect_identifier(
        self,
    ) -> None:
        self.assertFalse(
            state_calculation_config_manager.supervision_period_is_out_of_state(
                self.create_time_bucket("US_ID", "Incorrect - remainder of identifier")
            )
        )

    def test_supervision_period_is_out_of_state_us_id_with_empty_identifier(
        self,
    ) -> None:
        self.assertFalse(
            state_calculation_config_manager.supervision_period_is_out_of_state(
                self.create_time_bucket("US_ID", None)
            )
        )

    def test_supervision_period_is_out_of_state_us_mo_with_identifier(self) -> None:
        self.assertFalse(
            state_calculation_config_manager.supervision_period_is_out_of_state(
                self.create_time_bucket(
                    "US_MO", "INTERSTATE PROBATION - remainder of identifier"
                )
            )
        )

    def test_supervision_period_is_out_of_state_us_mo_with_incorrect_identifier(
        self,
    ) -> None:
        self.assertFalse(
            state_calculation_config_manager.supervision_period_is_out_of_state(
                self.create_time_bucket("US_MO", "Incorrect - remainder of identifier")
            )
        )

    @staticmethod
    def create_time_bucket(
        state_code: str, supervising_district_external_id: Optional[str]
    ) -> RevocationReturnSupervisionTimeBucket:
        return RevocationReturnSupervisionTimeBucket(
            state_code=state_code,
            year=2010,
            month=1,
            event_date=date(2010, 1, 1),
            is_on_supervision_last_day_of_month=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id=supervising_district_external_id,
            projected_end_date=None,
        )
