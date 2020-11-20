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
from typing import Optional

from more_itertools import one

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.tests.persistence.database.history.\
    base_historical_snapshot_updater_test import (
        BaseHistoricalSnapshotUpdaterTest
    )
from recidiviz.tests.persistence.database.database_test_utils import \
    generate_schema_state_person_obj_tree
from recidiviz.tools.postgres import local_postgres_helpers


class TestStateHistoricalSnapshotUpdater(BaseHistoricalSnapshotUpdaterTest):
    """Tests for StateHistoricalSnapshotUpdater"""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database(create_temporary_db=True)

    def setUp(self) -> None:
        local_postgres_helpers.use_on_disk_postgresql_database(StateBase)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(StateBase)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(cls.temp_db_dir)

    def testStateRecordTreeSnapshotUpdate(self):
        person = generate_schema_state_person_obj_tree()

        ingest_time_1 = datetime.datetime(2018, 7, 30)
        self._commit_person(person, SystemLevel.STATE, ingest_time_1)

        all_schema_objects = self._get_all_schema_objects_in_db(
            state_schema.StatePerson, state_schema, [])
        for schema_object in all_schema_objects:
            self._assert_expected_snapshots_for_schema_object(
                schema_object, [ingest_time_1])

        # Commit an update to the StatePerson
        update_session = SessionFactory.for_schema_base(StateBase)
        person = one(update_session.query(state_schema.StatePerson).all())
        person.full_name = 'new name'
        ingest_time_2 = datetime.datetime(2018, 7, 31)
        self._commit_person(person, SystemLevel.STATE, ingest_time_2)
        update_session.close()

        # Check that StatePerson had a new history table row written, but not
        # its child SentenceGroup.
        assert_session = SessionFactory.for_schema_base(StateBase)
        person = one(assert_session.query(state_schema.StatePerson).all())
        sentence_group = \
            one(assert_session.query(state_schema.StateSentenceGroup).all())

        self._assert_expected_snapshots_for_schema_object(person,
                                                          [ingest_time_1,
                                                           ingest_time_2])

        self._assert_expected_snapshots_for_schema_object(sentence_group,
                                                          [ingest_time_1])
        assert_session.close()
