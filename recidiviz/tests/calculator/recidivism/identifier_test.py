# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

# pylint: disable=unused-import,wrong-import-order

"""Tests for recidivism/identifier.py."""

from datetime import date

from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, \
    StateIncarcerationFacilitySecurityLevel

from recidiviz.calculator.recidivism import identifier, RecidivismEvent
from recidiviz.calculator.recidivism.recidivism_event import \
    IncarcerationReturnType


class TestFindRecidivism:
    """Tests for the find_recidivism function."""

    def test_find_recidivism(self):
        """Tests the find_recidivism function path where the person did
        recidivate."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        subsequent_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='TX',
                admission_date=date(2017, 1, 4),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 subsequent_reincarceration_period]

        recidivism_events_by_cohort = \
            identifier.find_recidivism(incarceration_periods)

        assert len(recidivism_events_by_cohort) == 2

        assert recidivism_events_by_cohort[2010] == [RecidivismEvent(
            recidivated=True,
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.RECONVICTION)]

        assert recidivism_events_by_cohort[2014] == [RecidivismEvent(
            recidivated=True,
            original_admission_date=first_reincarceration_period.admission_date,
            release_date=first_reincarceration_period.release_date,
            release_facility=None,
            reincarceration_date=subsequent_reincarceration_period.
            admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.RECONVICTION)]

    def test_find_recidivism_no_incarcerations_at_all(self):
        """Tests the find_recidivism function when the person has no
        StateIncarcerationPeriods."""
        recidivism_events_by_cohort = identifier.find_recidivism([])

        assert not recidivism_events_by_cohort

    def test_find_recidivism_no_recidivism_after_first(self):
        """Tests the find_recidivism function when the person does not have
        any StateIncarcerationPeriods after their first."""
        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2000, 1, 9),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2003, 12, 8),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        recidivism_events_by_cohort = identifier.find_recidivism(
            [only_incarceration_period])

        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2003] == [
            RecidivismEvent.non_recidivism_event(
                original_admission_date=only_incarceration_period.
                admission_date,
                release_date=only_incarceration_period.release_date,
                release_facility=None)]

    def test_find_recidivism_still_incarcerated_on_first(self):
        """Tests the find_recidivism function where the person is still
        incarcerated on their very first StateIncarcerationPeriod."""
        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION)

        recidivism_events_by_cohort = identifier.find_recidivism(
            [only_incarceration_period])

        assert not recidivism_events_by_cohort

    def test_find_recidivism_no_recidivism_conditional_release(self):
        """Tests the find_recidivism function when the person does not have
                any StateIncarcerationPeriods after their first, and they were
                released conditionally."""
        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2000, 1, 9),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2003, 12, 8),
                release_reason=StateIncarcerationPeriodReleaseReason.
                CONDITIONAL_RELEASE)

        recidivism_events_by_cohort = identifier.find_recidivism(
            [only_incarceration_period])

        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2003] == [
            RecidivismEvent.non_recidivism_event(
                original_admission_date=only_incarceration_period.
                admission_date,
                release_date=only_incarceration_period.release_date,
                release_facility=None)]

    def test_find_recidivism_parole_revocation(self):
        """Tests the find_recidivism function path where the person was
        conditionally released on parole and returned for a parole
        revocation."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                CONDITIONAL_RELEASE)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                PAROLE_REVOCATION,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        recidivism_events_by_cohort = identifier.find_recidivism(
            incarceration_periods=incarceration_periods,
            include_revocation_returns=True)

        assert len(recidivism_events_by_cohort) == 2

        assert recidivism_events_by_cohort[2010] == [RecidivismEvent(
            recidivated=True,
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.PAROLE_REVOCATION)]

        assert recidivism_events_by_cohort[2014] == [
            RecidivismEvent.non_recidivism_event(
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]

    def test_find_recidivism_probation_revocation(self):
        """Tests the find_recidivism function path where the person was
        conditionally released and returned for a probation revocation."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                CONDITIONAL_RELEASE)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                PROBATION_REVOCATION,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        recidivism_events_by_cohort = identifier.find_recidivism(
            incarceration_periods=incarceration_periods,
            include_revocation_returns=True)

        assert len(recidivism_events_by_cohort) == 2

        assert recidivism_events_by_cohort[2010] == [RecidivismEvent(
            recidivated=True,
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.PROBATION_REVOCATION)]

        assert recidivism_events_by_cohort[2014] == [
            RecidivismEvent.non_recidivism_event(
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]

    def test_find_recidivism_conditional_release_new_admission(self):
        """Tests the find_recidivism function path where the person was
        conditionally released on parole but returned as a new admission."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                CONDITIONAL_RELEASE)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        recidivism_events_by_cohort = identifier.find_recidivism(
            incarceration_periods=incarceration_periods,
            include_revocation_returns=True)

        assert len(recidivism_events_by_cohort) == 2

        assert recidivism_events_by_cohort[2010] == [RecidivismEvent(
            recidivated=True,
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.RECONVICTION)]

        assert recidivism_events_by_cohort[2014] == [
            RecidivismEvent.non_recidivism_event(
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]

    def test_find_recidivism_sentence_served_probation_revocation(self):
        """Tests the find_recidivism function path where the person served their
         first sentence, then later returned on a probation revocation."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                PROBATION_REVOCATION,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        recidivism_events_by_cohort = identifier.find_recidivism(
            incarceration_periods=incarceration_periods,
            include_revocation_returns=True)

        assert len(recidivism_events_by_cohort) == 2

        assert recidivism_events_by_cohort[2010] == [RecidivismEvent(
            recidivated=True,
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.PROBATION_REVOCATION)]

        assert recidivism_events_by_cohort[2014] == [
            RecidivismEvent.non_recidivism_event(
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]

    def test_find_recidivism_transfer_no_recidivism(self):
        """Tests the find_recidivism function path where the person was
        transferred between two incarceration periods."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2010, 12, 4),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        recidivism_events_by_cohort = identifier.find_recidivism(
            incarceration_periods=incarceration_periods,
            include_revocation_returns=True)

        assert len(recidivism_events_by_cohort) == 1

        assert recidivism_events_by_cohort[2014] == [
            RecidivismEvent.non_recidivism_event(
                original_admission_date=initial_incarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]

    def test_find_recidivism_transfer_out_but_recidivism(self):
        """Tests the find_recidivism function path where the person was
        transferred out of state, then later returned on a new admission."""
        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 4, 5),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        recidivism_events_by_cohort = identifier.find_recidivism(
            incarceration_periods=incarceration_periods,
            include_revocation_returns=True)

        assert len(recidivism_events_by_cohort) == 2

        assert recidivism_events_by_cohort[2010] == [RecidivismEvent(
            recidivated=True,
            original_admission_date=initial_incarceration_period.admission_date,
            release_date=initial_incarceration_period.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration_period.admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.RECONVICTION)]

        assert recidivism_events_by_cohort[2014] == [
            RecidivismEvent.non_recidivism_event(
                original_admission_date=first_reincarceration_period.
                admission_date,
                release_date=first_reincarceration_period.release_date,
                release_facility=None)]


class TestCollapseIncarcerationPeriods:
    """Tests the collapse_incarceration_periods function."""

    def test_collapse_incarceration_periods(self):
        """Tests collapse_incarceration_periods for two incarceration
        periods linked by a transfer."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2010, 12, 4),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 1

        assert collapsed_incarceration_periods == [
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=initial_incarceration_period.
                incarceration_period_id,
                status=first_reincarceration_period.status,
                state_code=initial_incarceration_period.state_code,
                admission_date=initial_incarceration_period.admission_date,
                admission_reason=initial_incarceration_period.admission_reason,
                release_date=first_reincarceration_period.release_date,
                release_reason=first_reincarceration_period.release_reason)
        ]

    def test_collapse_incarceration_periods_no_transfers(self):
        """Tests collapse_incarceration_periods for two incarceration
        periods not linked by a transfer."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2010, 12, 4),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 2

        assert collapsed_incarceration_periods == incarceration_periods

    def test_collapse_incarceration_periods_multiple_transfers(self):
        """Tests collapse_incarceration_periods for a person who was repeatedly
        transferred between facilities. All of these incarceration periods
        should collapse into a single incarceration period."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 2),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                TRANSFER,
                release_date=date(2012, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 2, 4),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                TRANSFER)

        third_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=4444,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2016, 6, 2),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                TRANSFER,
                release_date=date(2017, 3, 1),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period,
                                 third_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 1

        assert collapsed_incarceration_periods == [
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=initial_incarceration_period.
                incarceration_period_id,
                status=third_reincarceration_period.status,
                state_code=initial_incarceration_period.state_code,
                admission_date=initial_incarceration_period.admission_date,
                admission_reason=initial_incarceration_period.admission_reason,
                release_date=third_reincarceration_period.release_date,
                release_reason=third_reincarceration_period.release_reason),
        ]

    def test_collapse_incarceration_periods_between_periods(self):
        """Tests collapse_incarceration_periods for two incarceration
        periods linked by a transfer preceded and followed by regular
        incarceration periods."""

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 2),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 2, 4),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        third_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=4444,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2016, 6, 2),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2017, 3, 1),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period,
                                 third_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 3

        assert collapsed_incarceration_periods == [
            initial_incarceration_period,
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=first_reincarceration_period.
                incarceration_period_id,
                status=second_reincarceration_period.status,
                state_code=first_reincarceration_period.state_code,
                admission_date=first_reincarceration_period.admission_date,
                admission_reason=first_reincarceration_period.admission_reason,
                release_date=second_reincarceration_period.release_date,
                release_reason=second_reincarceration_period.release_reason),
            third_reincarceration_period
        ]

    def test_collapse_incarceration_periods_no_incarcerations(self):
        """Tests collapse_incarceration_periods for an empty list of
        incarceration periods."""

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods([])

        assert not collapsed_incarceration_periods

    def test_collapse_incarceration_periods_one_incarceration(self):
        """Tests collapse_incarceration_periods for a person with only
        one incarceration period that ended with a sentence served."""

        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [only_incarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert collapsed_incarceration_periods == incarceration_periods

    def test_collapse_incarceration_periods_one_incarceration_transferred(self):
        """Tests collapse_incarceration_periods for a person with only
        one incarceration period that ended with a transfer out of state
        prison."""

        only_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        incarceration_periods = [only_incarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert collapsed_incarceration_periods == incarceration_periods

    def test_collapse_incarceration_periods_transfer_out_then_in(self):
        """Tests collapse_incarceration_periods for a person who was
        transferred elsewhere, potentially out of state, and then reappeared
        in the state later as a new admission. These two periods should not
        be collapsed, because it's possible that this person was transferred
        to another state or to a federal prison, was completely released from
        that facility, and then has a new crime admission back into this state's
        facility.

        Then, this person was conditionally released, and returned later due to
        a transfer. These two period should also not be collapsed, because we
        don't have enough knowledge to make a safe assumption that this last
        incarceration period is connected to any of the previous ones.
        """

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 2),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                CONDITIONAL_RELEASE)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2013, 12, 4),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 3

        assert collapsed_incarceration_periods == incarceration_periods

    def test_collapse_incarceration_periods_transfer_back_then_transfer(self):
        """Tests collapse_incarceration_periods for a person who was transferred
        elsewhere, perhaps out of state, and then later reappears in the
        system as a new admission. These two incarceration periods should not
        be collapsed, because it's possible that this person was transferred
        to another state or to a federal prison, was completely released from
        that facility, and then has a new crime admission back into this state's
        facility.

        Then, this person was transferred out of this period and into a new
        incarceration period. These two periods should be collapsed because
        they are connected by a transfer.
        """

        initial_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2008, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2010, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        first_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=2222,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2011, 3, 2),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                release_date=date(2012, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                TRANSFER)

        second_reincarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2012, 12, 4),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                TRANSFER,
                release_date=date(2014, 4, 14),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED)

        incarceration_periods = [initial_incarceration_period,
                                 first_reincarceration_period,
                                 second_reincarceration_period]

        collapsed_incarceration_periods = \
            identifier.collapse_incarceration_periods(incarceration_periods)

        assert len(collapsed_incarceration_periods) == 2

        assert collapsed_incarceration_periods == [
            initial_incarceration_period,
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=first_reincarceration_period.
                incarceration_period_id,
                status=second_reincarceration_period.status,
                state_code=first_reincarceration_period.state_code,
                admission_date=first_reincarceration_period.admission_date,
                admission_reason=first_reincarceration_period.admission_reason,
                release_date=second_reincarceration_period.release_date,
                release_reason=second_reincarceration_period.release_reason),
        ]


class TestCombineIncarcerationPeriods:
    """Tests for combine_incarceration_periods function."""

    def test_combine_incarceration_periods(self):
        """Tests for combining two incarceration periods connected by a
        transfer."""

        start_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            facility='Green',
            housing_unit='House19',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MEDIUM,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        end_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            facility='Jones',
            housing_unit='HouseUnit3',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 10),
            release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE)

        combined_incarceration_period = \
            identifier.combine_incarceration_periods(
                start_incarceration_period, end_incarceration_period
            )

        assert combined_incarceration_period == \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=start_incarceration_period.
                incarceration_period_id,
                status=end_incarceration_period.status,
                state_code=start_incarceration_period.state_code,
                facility=end_incarceration_period.facility,
                housing_unit=end_incarceration_period.housing_unit,
                facility_security_level=end_incarceration_period.
                facility_security_level,
                projected_release_reason=end_incarceration_period.
                projected_release_reason,
                admission_date=start_incarceration_period.admission_date,
                admission_reason=start_incarceration_period.admission_reason,
                release_date=end_incarceration_period.release_date,
                release_reason=end_incarceration_period.release_reason

            )
