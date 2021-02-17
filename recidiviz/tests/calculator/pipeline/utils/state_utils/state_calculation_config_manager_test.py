# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
from datetime import date
from typing import List, Optional

import unittest
import attr

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import RevocationReturnSupervisionTimeBucket
from recidiviz.calculator.pipeline.utils.state_utils import state_calculation_config_manager
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    get_supervising_officer_and_location_info_from_supervision_period
from recidiviz.calculator.pipeline.utils.supervision_period_utils import SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason, \
    StateIncarcerationPeriodReleaseReason as ReleaseReason, \
    StateIncarcerationPeriodStatus, StateSpecializedPurposeForIncarceration, StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus, StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod, StateIncarcerationPeriod


class TestRevokedSupervisionPeriodsIfRevocationOccurred(unittest.TestCase):
    """Tests the state-specific revoked_supervision_periods_if_revocation_occurred function."""
    def test_revoked_supervision_periods_if_revocation_occurred(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_XX',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2019, 5, 29),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods: List[StateSupervisionPeriod] = []

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period.state_code, incarceration_period, supervision_periods, None
            )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_with_general_purpose_US_ID(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 5, 29),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods: List[StateSupervisionPeriod] = []

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period.state_code, incarceration_period, supervision_periods, None
            )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_with_treatment_US_ID(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=AdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 5, 29),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
        )

        supervision_periods: List[StateSupervisionPeriod] = []

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period.state_code, incarceration_period, supervision_periods, None
            )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_did_not_occur_with_treatment_transfer_US_ID(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=AdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 29),
            admission_reason=AdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period.state_code, incarceration_period, supervision_periods, treatment_period
            )

        self.assertFalse(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ID_NoRecentSupervision(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        # Incarceration period that occurred more than SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT months after
        # the most recent supervision period ended

        supervision_termination_date = supervision_period.termination_date
        if not supervision_termination_date:
            self.fail('Found None supervision period termination date.')
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=
            supervision_termination_date + relativedelta(months=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT + 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period.state_code, incarceration_period, supervision_periods, None
            )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ID_InvestigationSupervision(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 9),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_periods = \
            state_calculation_config_manager.revoked_supervision_periods_if_revocation_occurred(
                incarceration_period.state_code, incarceration_period, supervision_periods, None
            )

        self.assertFalse(admission_is_revocation)
        self.assertEqual([], revoked_periods)


class TestGetSupervisingOfficerAndLocationInfoFromSupervisionPeriod(unittest.TestCase):
    """Tests the state-specific get_supervising_officer_and_location_info_from_supervision_period function."""

    DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE: StateSupervisionPeriod = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_XX',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

    DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS = {
        111: {
            'agent_id': 123,
            'agent_external_id': 'agent_external_id_1',
            'supervision_period_id': 111
        }
    }

    def test_get_supervising_officer_and_location_info_from_supervision_period(self) -> None:
        supervision_period = attr.evolve(self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE, supervision_site='1')

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual('1', level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_no_site(self) -> None:
        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_no_agent(self) -> None:
        supervision_period = attr.evolve(self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
                                         supervision_period_id=666,  # No mapping for this ID
                                         supervision_site='1')

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual(None, supervising_officer_external_id)
        self.assertEqual('1', level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_ID(self) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code='US_ID', supervision_site='DISTRICT OFFICE 6, POCATELLO|UNKNOWN')

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual('UNKNOWN', level_1_supervision_location)
        self.assertEqual('DISTRICT OFFICE 6, POCATELLO', level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code='US_ID', supervision_site='PAROLE COMMISSION OFFICE|DEPORTED')

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual('DEPORTED', level_1_supervision_location)
        self.assertEqual('PAROLE COMMISSION OFFICE', level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code='US_ID', supervision_site='DISTRICT OFFICE 4, BOISE|')

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual('DISTRICT OFFICE 4, BOISE', level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code='US_ID', supervision_site=None)

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_PA(self) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code='US_PA', supervision_site='CO|CO - CENTRAL OFFICE|9110')

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual('CO - CENTRAL OFFICE', level_1_supervision_location)
        self.assertEqual('CO', level_2_supervision_location)

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code='US_PA', supervision_site=None)

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual(None, level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    # TODO(#5575): Delete this test once support for legacy, two-part supervision sites has been removed
    def test_get_supervising_officer_and_location_info_from_supervision_period_US_PA_legacy(self) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code='US_PA', supervision_site='CO|9110')

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual('9110', level_1_supervision_location)
        self.assertEqual('CO', level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_ND(self) -> None:
        nd_supervision_period_agent_associations = {
            111: {
                'agent_id': 123,
                'agent_external_id': 'agent_external_id_1',
                'supervision_period_id': 111
            }
        }

        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code='US_ND', supervision_site='SITE_1')

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, nd_supervision_period_agent_associations)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual('SITE_1', level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)

    def test_get_supervising_officer_and_location_info_from_supervision_period_US_MO(self) -> None:
        supervision_period = attr.evolve(
            self.DEFAULT_SUPERVISION_PERIOD_NO_SUPERVISION_SITE,
            state_code='US_MO', supervision_site='04C')

        supervising_officer_external_id, level_1_supervision_location, level_2_supervision_location = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, self.DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual('agent_external_id_1', supervising_officer_external_id)
        self.assertEqual('04C', level_1_supervision_location)
        self.assertEqual(None, level_2_supervision_location)


class TestSupervisionPeriodIsOutOfState(unittest.TestCase):
    """Tests the state-specific supervision_period_is_out_of_state function."""

    def test_supervision_period_is_out_of_state_us_id_with_identifier(self):
        self.assertTrue(state_calculation_config_manager.supervision_period_is_out_of_state(
            self.create_time_bucket("US_ID", "INTERSTATE PROBATION - remainder of identifier")))

    def test_supervision_period_is_out_of_state_us_id_with_incorrect_identifier(self):
        self.assertFalse(state_calculation_config_manager.supervision_period_is_out_of_state(
            self.create_time_bucket("US_ID", "Incorrect - remainder of identifier")))

    def test_supervision_period_is_out_of_state_us_id_with_empty_identifier(self):
        self.assertFalse(state_calculation_config_manager.supervision_period_is_out_of_state(
            self.create_time_bucket("US_ID", None)))

    def test_supervision_period_is_out_of_state_us_mo_with_identifier(self):
        self.assertFalse(state_calculation_config_manager.supervision_period_is_out_of_state(
            self.create_time_bucket("US_MO", "INTERSTATE PROBATION - remainder of identifier")))

    def test_supervision_period_is_out_of_state_us_mo_with_incorrect_identifier(self):
        self.assertFalse(state_calculation_config_manager.supervision_period_is_out_of_state(
            self.create_time_bucket("US_MO", "Incorrect - remainder of identifier")))

    @staticmethod
    def create_time_bucket(state_code: str, supervising_district_external_id: Optional[str]):
        return RevocationReturnSupervisionTimeBucket(
            state_code=state_code,
            year=2010,
            month=1,
            event_date=date(2010, 1, 1),
            is_on_supervision_last_day_of_month=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id=supervising_district_external_id,
            is_past_projected_end_date=False
        )
