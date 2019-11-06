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
"""Tests for the BaseStateDirectIngestController."""
import abc
import os
import unittest
from typing import List, Type

from freezegun import freeze_time
from mock import patch, Mock, create_autospec

from recidiviz import IngestInfo
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema_entity_converter.state.\
    schema_entity_converter import StateSchemaToEntityConverter
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.entity.entity_utils import print_entity_tree
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    build_gcsfs_controller_for_tests, ingest_args_for_fixture_file, \
    FakeDirectIngestGCSFileSystem, run_task_queues_to_empty, \
    path_for_fixture_file
from recidiviz.tests.persistence.entity.state.entities_test_utils import \
    clear_db_ids
from recidiviz.tests.utils import fakes
from recidiviz.utils.regions import Region


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
@freeze_time('2019-09-27')
class BaseStateDirectIngestControllerTests(unittest.TestCase):
    """Tests for the BaseStateDirectIngestController."""

    @classmethod
    @abc.abstractmethod
    def state_code(cls) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        pass

    def setUp(self) -> None:
        self.maxDiff = 250000
        fakes.use_in_memory_sqlite_database(StateBase)

        self.controller = build_gcsfs_controller_for_tests(
            self.controller_cls(),
            self.fixture_path_prefix(),
            run_async=False,
            max_delay_sec_between_files=0)

    @classmethod
    def fixture_path_prefix(cls):
        return os.path.join('direct', 'regions', cls.state_code().lower())

    def run_parse_file_test(self, expected: IngestInfo, file_tag: str) -> None:
        args = ingest_args_for_fixture_file(self.controller, f'{file_tag}.csv')

        if not isinstance(self.controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(self.controller.fs)}]")
        self.controller.fs.test_add_path(args.file_path)

        # pylint:disable=protected-access
        fixture_contents = self.controller._read_contents(args)

        if fixture_contents is None:
            raise ValueError('Contents should not be None')

        final_info = self.controller._parse(args, fixture_contents)

        print("FINAL")
        print(final_info)
        print("\n\n\n")
        print("EXPECTED")
        print(expected)

        self.assertEqual(final_info, expected)

    def _run_ingest_job_for_filename(self, filename: str) -> None:
        get_region_patcher = patch(
            "recidiviz.persistence.entity_matching.state."
            "base_state_matching_delegate.get_region")
        mock_get_region = get_region_patcher.start()
        mock_get_region.return_value = self._fake_region()

        environ_patcher = patch.dict(
            'os.environ', {'PERSIST_LOCALLY': 'true'}
        )
        environ_patcher.start()

        file_path = path_for_fixture_file(self.controller,
                                          filename,
                                          should_normalize=False)

        if not isinstance(self.controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(self.controller.fs)}]")

        self.controller.fs.test_add_path(file_path)

        run_task_queues_to_empty(self.controller)

        get_region_patcher.stop()
        environ_patcher.stop()

    @staticmethod
    def convert_and_clear_db_ids(db_entities: List[StateBase]):
        converted = StateSchemaToEntityConverter().convert_all(
            db_entities, populate_back_edges=True)
        clear_db_ids(converted)
        return converted

    def assert_expected_db_people(
            self, expected_db_people: List[StatePerson]) -> None:
        print('\n\n************** ASSERTING *************')
        session = SessionFactory.for_schema_base(StateBase)
        found_people_from_db = dao.read_people(session)
        found_people = self.convert_and_clear_db_ids(found_people_from_db)

        print("\n\nFINAL")
        for p in found_people:
            print_entity_tree(p)
        print("\n\nEXPECTED")
        for p in expected_db_people:
            print_entity_tree(p)

        self.assertCountEqual(found_people, expected_db_people)

    def _fake_region(self):
        fake_region = create_autospec(Region)
        fake_region.get_enum_overrides.return_value = \
            self.controller.get_enum_overrides()
        return fake_region
