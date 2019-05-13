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
from recidiviz.common.constants.person_characteristics import Ethnicity
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.tests.persistence.database.history.\
    base_historical_snapshot_updater_test import (
        BaseHistoricalSnapshotUpdaterTest
    )


class TestStateHistoricalSnapshotUpdater(BaseHistoricalSnapshotUpdaterTest):
    """Tests for StateHistoricalSnapshotUpdater"""

    def testConvertStateRecordTree(self):
        person_id = 143

        person = state_schema.Person(
            person_id=person_id,
            full_name='name',
            birthdate=datetime.date(1980, 1, 5),
            birthdate_inferred_from_age=False,
            external_ids=[
                state_schema.PersonExternalId(
                    person_external_id_id=234,
                    external_id='person_external_id',
                    state_code='us_ny',
                    person_id=person_id,
                )
            ],
            races=[
                state_schema.PersonRace(
                    person_race_id=345,
                    race=None,
                    race_raw_text='BLK',
                    person_id=person_id,
                )
            ],
            ethnicities=[
                state_schema.PersonEthnicity(
                    person_ethnicity_id=345,
                    ethnicity=Ethnicity.NOT_HISPANIC.value,
                    ethnicity_raw_text='HISP',
                    person_id=person_id,
                )
            ],
            assessments=[
                state_schema.Assessment(
                    assessment_id=456,
                    person_id=person_id,
                )
            ],
            sentence_groups=[
                state_schema.SentenceGroup(
                    sentence_group_id=567,
                    person_id=person_id,
                )
            ],
        )

        self._check_person_has_relationships_to_all_schema_object_types(
            person, state_schema, [])

        ingest_time_1 = datetime.datetime(2018, 7, 30)
        self._commit_person(person, SystemLevel.STATE, ingest_time_1)

        assert_session = Session()
        history_snapshots = assert_session.query(
            state_schema.PersonHistory
        ).filter(
            state_schema.PersonHistory.person_id == person_id
        ).all()

        person_snapshot = one(history_snapshots)

        self._assert_entity_and_snapshot_match(person, person_snapshot)

        # TODO(1625): Add more detailed checking for objects in the subtree

        assert_session.commit()
        assert_session.close()

        person.full_name = 'new name'
        ingest_time_2 = datetime.datetime(2018, 7, 31)
        self._commit_person(person, SystemLevel.STATE, ingest_time_2)

        assert_session = Session()

        history_snapshots = assert_session.query(
            state_schema.PersonHistory
        ).filter(
            state_schema.PersonHistory.person_id == person_id
        ).all()

        assert len(history_snapshots) == 2

        history_snapshots = sorted(history_snapshots,
                                   key=lambda snapshot: snapshot.valid_from)

        assert history_snapshots[0].valid_from == ingest_time_1
        assert history_snapshots[1].valid_from == ingest_time_2

        assert history_snapshots[0].valid_to == ingest_time_2
        assert history_snapshots[1].valid_to is None

        self._assert_entity_and_snapshot_match(person, history_snapshots[1])

        assert_session.commit()
        assert_session.close()
