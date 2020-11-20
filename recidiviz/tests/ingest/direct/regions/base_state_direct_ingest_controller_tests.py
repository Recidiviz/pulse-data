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
"""Class with basic functionality for tests of all state
GcsfsDirectIngestControllers.
"""
import abc
import datetime
import os
from typing import List, Type, Optional, cast

from freezegun import freeze_time
from mock import patch, Mock, create_autospec
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsDirectIngestFileType, \
    GcsfsIngestViewExportArgs
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema_entity_converter.state.\
    schema_entity_converter import StateSchemaToEntityConverter
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.entity.entity_utils import person_has_id, \
    print_entity_trees, prune_dangling_placeholders_from_tree
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.persistence.persistence import OVERALL_THRESHOLD, ENUM_THRESHOLD, ENTITY_MATCHING_THRESHOLD, \
    DATABASE_INVARIANT_THRESHOLD
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    run_task_queues_to_empty, \
    path_for_fixture_file
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests \
    import BaseDirectIngestControllerTests
from recidiviz.tests.persistence.entity.state.entities_test_utils import \
    clear_db_ids, assert_no_unexpected_entities_in_db
from recidiviz.tests.utils.test_utils import print_visible_header_label, \
    is_running_in_ci
from recidiviz.utils.regions import Region


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
@freeze_time('2019-09-27')
class BaseStateDirectIngestControllerTests(BaseDirectIngestControllerTests):
    """Class with basic functionality for tests of all state
    GcsfsDirectIngestControllers.
    """
    @classmethod
    @abc.abstractmethod
    def region_code(cls) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        pass

    @classmethod
    def schema_base(cls) -> DeclarativeMeta:
        return StateBase

    def setUp(self) -> None:
        super().setUp()

        # Set entity matching error threshold to a diminishingly small number
        # for tests. We cannot set it to 0 because we throw when errors *equal*
        # the error threshold.
        self.entity_matching_error_threshold_patcher = patch.dict(
            'recidiviz.persistence.persistence.SYSTEM_TYPE_TO_ERROR_THRESHOLD',
            {SystemLevel.STATE: {
                OVERALL_THRESHOLD: 0,
                ENUM_THRESHOLD: 0,
                ENTITY_MATCHING_THRESHOLD: 0,
                DATABASE_INVARIANT_THRESHOLD: 0,
            }})

        self.entity_matching_error_threshold_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()

        self.entity_matching_error_threshold_patcher.stop()

    def _run_ingest_job_for_filename(self, filename: str) -> None:
        """Runs ingest for a the ingest view file with the given unnormalized file name."""
        get_region_patcher = patch(
            "recidiviz.persistence.entity_matching.state."
            "base_state_matching_delegate.get_region")
        mock_get_region = get_region_patcher.start()
        mock_get_region.return_value = self._fake_region()

        environ_patcher = patch.dict(
            'os.environ', {'PERSIST_LOCALLY': 'true'}
        )
        environ_patcher.start()

        file_type = GcsfsDirectIngestFileType.INGEST_VIEW \
            if self.controller.region.is_raw_vs_ingest_file_name_detection_enabled() else None

        if not isinstance(self.controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeGCSFileSystem. Found instead "
                             f"type [{type(self.controller.fs.gcs_file_system)}]")

        if self.controller.region.are_ingest_view_exports_enabled_in_env():
            now = datetime.datetime.utcnow()
            yesterday = now - datetime.timedelta(days=1)
            ingest_file_export_job_args = GcsfsIngestViewExportArgs(
                ingest_view_name=os.path.splitext(filename)[0],
                upper_bound_datetime_to_export=now,
                upper_bound_datetime_prev=yesterday,
            )

            self.controller.file_metadata_manager.register_ingest_file_export_job(ingest_file_export_job_args)
            self.controller.ingest_view_export_manager.export_view_for_args(ingest_file_export_job_args)
        else:
            file_path = path_for_fixture_file(self.controller,
                                              filename,
                                              file_type=file_type,
                                              should_normalize=True)
            self.controller.fs.gcs_file_system.test_add_path(file_path)

        run_task_queues_to_empty(self.controller)

        get_region_patcher.stop()
        environ_patcher.stop()

    def _do_ingest_job_rerun_for_tags(self, file_tags: List[str]):
        self.invalidate_ingest_view_metadata()
        for file_tag in file_tags:
            self._run_ingest_job_for_filename(f'{file_tag}.csv')

    @staticmethod
    def convert_and_clear_db_ids(db_entities: List[StateBase]):
        converted = StateSchemaToEntityConverter().convert_all(
            db_entities, populate_back_edges=True)
        clear_db_ids(converted)
        return converted

    def assert_expected_db_people(
            self,
            expected_db_people: List[StatePerson],
            debug: bool = False,
            single_person_to_debug: Optional[str] = None,
            # TODO(#2492): Once we properly clean up dangling placeholders,
            #  delete this.
            ignore_dangling_placeholders: bool = False,
            print_tree_structure_only: bool = False,
    ) -> None:
        """Asserts that the set of expected people matches all the people that currently exist in the database.

        Args:
            debug: (bool) If true, prints out both the found and expected entity trees.
            single_person_to_debug: (str) A string external_id of a person. If debug=True and this is not None, this
                will only check for equality between the people with that external_id. This should be used for debugging
                only and this function will throw if this value is set in CI.
            ignore_dangling_placeholders: (bool) If True, eliminates dangling placeholder objects (i.e. placeholders
                with no non-placeholder children) from both the result and expected trees before doing a comparison.
            print_tree_structure_only: (bool) If True and debug=True, then the printed result only shows the tree
                structure - external ids and parent-child relationships.
        """

        if debug:
            print('\n\n************** ASSERTING *************')
        session = SessionFactory.for_schema_base(StateBase)
        found_people_from_db = dao.read_people(session)
        found_people = self.convert_and_clear_db_ids(found_people_from_db)

        if ignore_dangling_placeholders:
            pruned_found_people = []
            for person in found_people:
                pruned_person = prune_dangling_placeholders_from_tree(person)
                if pruned_person is not None:
                    pruned_found_people.append(pruned_person)
            found_people = pruned_found_people

            pruned_expected_people: List[StatePerson] = []
            for person in expected_db_people:
                pruned_expected_person = cast(StatePerson, prune_dangling_placeholders_from_tree(person))
                if pruned_expected_person is not None:
                    pruned_expected_people.append(pruned_expected_person)
            expected_db_people = pruned_expected_people

        if debug:
            if single_person_to_debug is not None:
                if is_running_in_ci():
                    self.fail('The |single_person_to_debug| flag should only be used for local debugging.')

                found_people = [p for p in found_people if person_has_id(p, single_person_to_debug)]
                expected_db_people = [p for p in expected_db_people if person_has_id(p, single_person_to_debug)]

            print_visible_header_label('FINAL')
            print_entity_trees(found_people, print_tree_structure_only=print_tree_structure_only)

            print_visible_header_label('EXPECTED')
            print_entity_trees(expected_db_people, print_tree_structure_only=print_tree_structure_only)

        self.assertCountEqual(found_people, expected_db_people)

        assert_no_unexpected_entities_in_db(found_people_from_db, session)

    def _fake_region(self):
        fake_region = create_autospec(Region)
        fake_region.get_enum_overrides.return_value = \
            self.controller.get_enum_overrides()
        return fake_region
