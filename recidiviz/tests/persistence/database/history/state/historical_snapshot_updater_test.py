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
"""Tests for StateHistoricalSnapshotUpdater"""

import datetime

from more_itertools import one

from recidiviz import Session
from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import Ethnicity, Race
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_parole_decision import \
    StateParoleDecisionOutcome
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.tests.persistence.database.history.\
    base_historical_snapshot_updater_test import (
        BaseHistoricalSnapshotUpdaterTest
    )


class TestStateHistoricalSnapshotUpdater(BaseHistoricalSnapshotUpdaterTest):
    """Tests for StateHistoricalSnapshotUpdater"""

    def generate_schema_state_person_obj_tree(self) -> state_schema.StatePerson:
        """Test util for generating a StatePerson schema object that has at
         least one child of each possible schema object type defined on
         state/schema.py.

        Returns:
            A test instance of a StatePerson schema object.
        """
        person_id = 143

        supervision_violation_response = \
            state_schema.StateSupervisionViolationResponse(
                supervision_violation_response_id=456,
                state_code='us_ca',
                person_id=person_id,
            )

        supervision_violation = state_schema.StateSupervisionViolation(
            supervision_violation_id=321,
            state_code='us_ca',
            person_id=person_id,
            supervision_violation_responses=[supervision_violation_response]
        )

        supervision_period = state_schema.StateSupervisionPeriod(
            supervision_period_id=4444,
            status=StateSupervisionPeriodStatus.EXTERNAL_UNKNOWN.value,
            state_code='us_ca',
            person_id=person_id,
            supervision_violations=[supervision_violation],
        )

        incarceration_incident = state_schema.StateIncarcerationIncident(
            incarceration_incident_id=321,
            state_code='us_ca',
            person_id=person_id,
        )

        parole_decision = state_schema.StateParoleDecision(
            parole_decision_id=789,
            state_code='us_ca',
            decision_outcome=StateParoleDecisionOutcome.PAROLE_DENIED.value,
            person_id=person_id,
        )

        incarceration_period = state_schema.StateIncarcerationPeriod(
            incarceration_period_id=5555,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
            state_code='us_ca',
            person_id=person_id,
            incarceration_incidents=[incarceration_incident],
            parole_decisions=[parole_decision],
        )
        court_case = state_schema.StateCourtCase(
            court_case_id=8888,
            state_code='us_ca',
            person_id=person_id,
        )

        bond = state_schema.StateBond(
            bond_id=9999,
            person_id=person_id,
            status=BondStatus.PENDING.value,
            status_raw_text='PENDING',
            bond_type=BondType.CASH.value,
            bond_type_raw_text='CASH',
            date_paid=None,
            state_code='us_ca',
            county_code='us_ca_san_francisco',
            amount_dollars=1000,
            bond_agent=None,
        )

        charge1 = state_schema.StateCharge(
            charge_id=6666,
            person_id=person_id,
            status=ChargeStatus.PENDING.value,
            state_code='us_ca',
            court_case=court_case,
            bond=bond,
        )

        charge2 = state_schema.StateCharge(
            charge_id=7777,
            person_id=person_id,
            status=ChargeStatus.PENDING.value,
            state_code='us_ca',
            court_case=court_case,
        )

        supervision_sentence = state_schema.StateSupervisionSentence(
            supervision_sentence_id=1111,
            status=StateSentenceStatus.SERVING.value,
            state_code='us_ca',
            person_id=person_id,
            charges=[charge1, charge2],
        )

        incarceration_sentence = state_schema.StateIncarcerationSentence(
            incarceration_sentence_id=2222,
            status=StateSentenceStatus.SUSPENDED.value,
            state_code='us_ca',
            person_id=person_id,
            charges=[charge1, charge2],
        )

        fine = state_schema.StateFine(
            fine_id=3333,
            status=StateFineStatus.PAID.value,
            state_code='us_ca',
            person_id=person_id,
        )

        sentence_group = state_schema.StateSentenceGroup(
            sentence_group_id=567,
            status=StateSentenceStatus.SUSPENDED.value,
            state_code='us_ca',
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[incarceration_sentence],
            fines=[fine],
        )

        assessment_agent = state_schema.StateAgent(
            agent_id=1010,
            external_id='ASSAGENT1234',
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
            state_code='us_ca',
            full_name='JOHN SMITH',
        )

        person = state_schema.StatePerson(
            person_id=person_id,
            full_name='name',
            birthdate=datetime.date(1980, 1, 5),
            birthdate_inferred_from_age=False,
            external_ids=[
                state_schema.StatePersonExternalId(
                    person_external_id_id=234,
                    external_id='person_external_id',
                    state_code='us_ny',
                    person_id=person_id,
                )
            ],
            aliases=[
                state_schema.StatePersonAlias(
                    person_alias_id=1456,
                    state_code='us_ca',
                    full_name='name',
                    person_id=person_id,
                )
            ],
            races=[
                state_schema.StatePersonRace(
                    person_race_id=345,
                    state_code='us_ca',
                    race=Race.BLACK.value,
                    race_raw_text='BLK',
                    person_id=person_id,
                )
            ],
            ethnicities=[
                state_schema.StatePersonEthnicity(
                    person_ethnicity_id=345,
                    state_code='us_ca',
                    ethnicity=Ethnicity.NOT_HISPANIC.value,
                    ethnicity_raw_text='HISP',
                    person_id=person_id,
                )
            ],
            sentence_groups=[sentence_group],
            assessments=[
                state_schema.StateAssessment(
                    assessment_id=456,
                    person_id=person_id,
                    state_code='us_ca',
                    incarceration_period=incarceration_period,
                    conducting_agent=assessment_agent,
                ),
                state_schema.StateAssessment(
                    assessment_id=4567,
                    person_id=person_id,
                    state_code='us_ca',
                    supervision_period=supervision_period,
                    conducting_agent=assessment_agent,
                )
            ],
        )
        sentence_group.person = person
        return person

    def testStateRecordTreeSnapshotUpdate(self):
        person = self.generate_schema_state_person_obj_tree()

        ingest_time_1 = datetime.datetime(2018, 7, 30)
        self._commit_person(person, SystemLevel.STATE, ingest_time_1)

        all_schema_objects = self._get_all_schema_objects_in_db(
            state_schema.StatePerson, state_schema, [])
        for schema_object in all_schema_objects:
            self._assert_expected_snapshots_for_schema_object(
                schema_object, [ingest_time_1])

        # Commit an update to the StatePerson
        update_session = Session()
        person = one(update_session.query(state_schema.StatePerson).all())
        person.full_name = 'new name'
        ingest_time_2 = datetime.datetime(2018, 7, 31)
        self._commit_person(person, SystemLevel.STATE, ingest_time_2)
        update_session.close()

        # Check that StatePerson had a new history table row written, but not
        # its child SentenceGroup.
        assert_session = Session()
        person = one(assert_session.query(state_schema.StatePerson).all())
        sentence_group = \
            one(assert_session.query(state_schema.StateSentenceGroup).all())

        self._assert_expected_snapshots_for_schema_object(person,
                                                          [ingest_time_1,
                                                           ingest_time_2])

        self._assert_expected_snapshots_for_schema_object(sentence_group,
                                                          [ingest_time_1])
        assert_session.close()
