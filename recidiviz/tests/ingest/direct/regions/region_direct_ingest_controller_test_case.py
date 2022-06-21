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
"""Class with basic functionality for tests of all region-specific
BaseDirectIngestControllers.
"""

import abc
import datetime
import os
import unittest
from typing import List, Optional, Type, cast

import pytest
import pytz
from freezegun import freeze_time
from mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.legacy_ingest_view_processor import (
    LegacyIngestViewProcessor,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_pause_status_manager import (
    DirectIngestInstancePauseStatusManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    ExtractAndMergeArgs,
    IngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.operations import schema as operations_schema
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema_entity_converter.state.schema_entity_converter import (
    StateSchemaToEntityConverter,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    person_has_id,
    print_entity_trees,
    prune_dangling_placeholders_from_tree,
)
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.persistence.persistence import (
    DATABASE_INVARIANT_THRESHOLD,
    ENTITY_MATCHING_THRESHOLD,
    ENUM_THRESHOLD,
    OVERALL_THRESHOLD,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct.direct_ingest_test_util import (
    run_task_queues_to_empty,
)
from recidiviz.tests.ingest.direct.fakes.fake_direct_ingest_controller import (
    build_fake_direct_ingest_controller,
)
from recidiviz.tests.ingest.direct.fakes.fake_instance_ingest_view_contents import (
    FakeInstanceIngestViewContents,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    assert_no_unexpected_entities_in_db,
    clear_db_ids,
)
from recidiviz.tests.utils.test_utils import print_visible_header_label
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.environment import in_ci
from recidiviz.utils.types import assert_type

FULL_INTEGRATION_TEST_NAME = "test_run_full_ingest_all_files_specific_order"


@pytest.mark.uses_db
@freeze_time("2019-09-27")
class RegionDirectIngestControllerTestCase(unittest.TestCase):
    """Class with basic functionality for tests of all region-specific
    BaseDirectIngestControllers.
    """

    @classmethod
    @abc.abstractmethod
    def region_code(cls) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def controller_cls(cls) -> Type[BaseDirectIngestController]:
        pass

    @classmethod
    @abc.abstractmethod
    def schema_type(cls) -> SchemaType:
        pass

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    @classmethod
    def _main_ingest_instance(cls) -> DirectIngestInstance:
        # We assume we're ingesting into the SECONDARY ingest instance, which
        # should always have the latest ingest logic updates released to it.
        return DirectIngestInstance.SECONDARY

    @classmethod
    def _main_database_key(cls) -> "SQLAlchemyDatabaseKey":
        if cls.schema_type() == SchemaType.STATE:
            state_code = StateCode(cls.region_code().upper())
            return cls._main_ingest_instance().database_key_for_state(
                state_code,
            )
        return SQLAlchemyDatabaseKey.for_schema(cls.schema_type())

    def setUp(self) -> None:
        self.maxDiff = None

        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-staging"

        self.main_database_key = self._main_database_key()
        self.operations_database_key = SQLAlchemyDatabaseKey.for_schema(
            SchemaType.OPERATIONS
        )
        local_postgres_helpers.use_on_disk_postgresql_database(self.main_database_key)
        local_postgres_helpers.use_on_disk_postgresql_database(
            self.operations_database_key
        )

        self.controller = build_fake_direct_ingest_controller(
            self.controller_cls(),
            ingest_instance=self._main_ingest_instance(),
            run_async=False,
            regions_module=regions,
        )

        # Set entity matching error threshold to a diminishingly small number
        # for tests. We cannot set it to 0 because we throw when errors *equal*
        # the error threshold.
        self.entity_matching_error_threshold_patcher = patch.dict(
            "recidiviz.persistence.persistence.SYSTEM_TYPE_TO_ERROR_THRESHOLD",
            {
                SystemLevel.STATE: {
                    OVERALL_THRESHOLD: 0,
                    ENUM_THRESHOLD: 0,
                    ENTITY_MATCHING_THRESHOLD: 0,
                    DATABASE_INVARIANT_THRESHOLD: 0,
                }
            },
        )

        self.entity_matching_error_threshold_patcher.start()

        for instance in DirectIngestInstance:
            DirectIngestInstancePauseStatusManager.add_instance(
                self.region_code(), instance, (instance != self._main_ingest_instance())
            )

        self.file_tags_processed: List[str] = []
        self.did_rerun_for_idempotence = False

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(
            self.operations_database_key
        )
        local_postgres_helpers.teardown_on_disk_postgresql_database(
            self.main_database_key
        )
        self.metadata_patcher.stop()
        self.entity_matching_error_threshold_patcher.stop()

        self._validate_integration_test()

    def _validate_integration_test(self) -> None:
        """If this test is the main integration test, validates that all expected files
        were processed during this test.
        """
        if FULL_INTEGRATION_TEST_NAME not in dir(self):
            raise ValueError(
                f"Must define integration test with name "
                f"[{FULL_INTEGRATION_TEST_NAME}] in this test class."
            )

        if (
            self._outcome.errors  # type: ignore
            or self._testMethodName != FULL_INTEGRATION_TEST_NAME
        ):
            # If test fails or this is not the integration test, do not validate
            return

        expected_tags = self.controller.get_ingest_view_rank_list()

        expected_tags_set = set(expected_tags)
        processed_tags_set = set(self.file_tags_processed)

        if skipped := expected_tags_set.difference(processed_tags_set):
            self.fail(f"Failed to run test for ingest view files: {skipped}")

        if extra := processed_tags_set.difference(expected_tags_set):
            self.fail(f"Found test for extra, unexpected ingest view files: {extra}")

        self.assertEqual(
            expected_tags,
            self.file_tags_processed,
            "Expected and processed tags do not match.",
        )

        self.assertTrue(
            self.did_rerun_for_idempotence,
            "No rerun for idempotence. Make sure the integration test calls "
            "_do_ingest_job_rerun_for_tags().",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    # TODO(#8905): Delete this function once we have migrated all states to use the new
    #   version of ingest mappings that skip ingest info entirely.
    def run_legacy_parse_file_test(
        self, expected: IngestInfo, ingest_view_name: str
    ) -> IngestInfo:
        """Runs a test that reads and parses a given fixture file. Returns the
        parsed IngestInfo object for tests to run further validations."""

        if not self.controller.region.is_ingest_launched_in_env():
            raise ValueError(
                "This function is only for use in legacy states where ingest is fully launched."
            )

        if not isinstance(self.controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(self.controller.fs.gcs_file_system)}]"
            )

        now = datetime.datetime.now()
        yesterday = now - datetime.timedelta(days=1)

        materialization_job_args = self._register_materialization_job(
            controller=self.controller,
            ingest_view_name=ingest_view_name,
            upper_bound_datetime=now,
            lower_bound_datetime=yesterday,
        )

        ingest_view_contents = assert_type(
            self.controller.ingest_view_contents, FakeInstanceIngestViewContents
        )
        # Make batch size large so all fixture data is processed in one batch
        ingest_view_contents.batch_size = 10000

        self.controller.ingest_view_materializer.materialize_view_for_args(
            materialization_job_args
        )

        extract_and_merge_args = assert_type(
            self.controller.job_prioritizer.get_next_job_args(),
            ExtractAndMergeArgs,
        )

        # pylint:disable=protected-access
        fixture_contents_handle = self.controller._get_contents_handle(
            extract_and_merge_args
        )

        if fixture_contents_handle is None:
            self.fail("fixture_contents_handle should not be None")
        processor = self.controller.get_ingest_view_processor(extract_and_merge_args)

        if not isinstance(processor, LegacyIngestViewProcessor):
            raise ValueError(f"Unexpected processor type: {type(processor)}")

        # pylint:disable=protected-access
        final_info = processor._parse_ingest_info(
            extract_and_merge_args,
            fixture_contents_handle,
            self.controller._get_ingest_metadata(extract_and_merge_args),
        )

        print_visible_header_label("FINAL")
        print(final_info)

        print_visible_header_label("EXPECTED")
        print(expected)

        self.assertEqual(expected, final_info)

        return final_info

    def invalidate_ingest_view_metadata(self) -> None:
        with SessionFactory.using_database(self.operations_database_key) as session:
            session.query(
                operations_schema.DirectIngestViewMaterializationMetadata
            ).update(
                {
                    operations_schema.DirectIngestViewMaterializationMetadata.is_invalidated: True
                }
            )

    def _run_ingest_job_for_filename(
        self, filename: str, is_rerun: bool = False
    ) -> None:
        """Runs ingest for the ingest view file with the given unnormalized file name."""

        environ_patcher = patch.dict("os.environ", {"PERSIST_LOCALLY": "true"})
        environ_patcher.start()

        if not isinstance(self.controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(self.controller.fs.gcs_file_system)}]"
            )

        now = datetime.datetime.now(tz=pytz.UTC)
        yesterday = now - datetime.timedelta(days=1)
        file_tag, _ext = os.path.splitext(filename)

        if file_tag not in self.controller.get_ingest_view_rank_list():
            raise ValueError(
                f"Cannot run test for tag [{file_tag}] which is not returned by the "
                f"controller's get_ingest_view_rank_list() function."
            )

        if not is_rerun:
            self.file_tags_processed.append(file_tag)

        materialization_job_args = self._register_materialization_job(
            controller=self.controller,
            ingest_view_name=file_tag,
            upper_bound_datetime=now,
            lower_bound_datetime=yesterday,
        )

        self.controller.do_ingest_view_materialization(materialization_job_args)

        run_task_queues_to_empty(self.controller)

        environ_patcher.stop()

    @staticmethod
    def _register_materialization_job(
        controller: BaseDirectIngestController,
        ingest_view_name: str,
        upper_bound_datetime: datetime.datetime,
        lower_bound_datetime: Optional[datetime.datetime],
    ) -> IngestViewMaterializationArgs:
        metadata_manager = controller.view_materialization_metadata_manager
        args = IngestViewMaterializationArgs(
            ingest_view_name=ingest_view_name,
            lower_bound_datetime_exclusive=lower_bound_datetime,
            upper_bound_datetime_inclusive=upper_bound_datetime,
            ingest_instance=metadata_manager.ingest_instance,
        )
        metadata_manager.register_ingest_materialization_job(args)
        return args

    def _do_ingest_job_rerun_for_tags(self, file_tags: List[str]) -> None:
        self.invalidate_ingest_view_metadata()
        ingest_view_contents = assert_type(
            self.controller.ingest_view_contents, FakeInstanceIngestViewContents
        )
        ingest_view_contents.test_clear_data()
        for file_tag in file_tags:
            self._run_ingest_job_for_filename(f"{file_tag}.csv", is_rerun=True)
        self.did_rerun_for_idempotence = True

    @staticmethod
    def convert_and_clear_db_ids(db_entities: List[StateBase]) -> List[Entity]:
        converted = StateSchemaToEntityConverter().convert_all(
            db_entities, populate_back_edges=True
        )
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
        # Default arg caches across calls to this function
        field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
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
            print("\n\n************** ASSERTING *************")

        if not self.schema_type() == SchemaType.STATE:
            raise ValueError(f"Unsupported schema type [{self.schema_type()}]")

        with SessionFactory.using_database(
            self.main_database_key, autocommit=False
        ) as session:
            found_people_from_db = dao.read_people(session)
            found_people = cast(
                List[StatePerson], self.convert_and_clear_db_ids(found_people_from_db)
            )

        if ignore_dangling_placeholders:
            pruned_found_people = []
            for person in found_people:
                pruned_person = cast(
                    StatePerson,
                    prune_dangling_placeholders_from_tree(
                        person, field_index=field_index
                    ),
                )
                if pruned_person is not None:
                    pruned_found_people.append(pruned_person)
            found_people = pruned_found_people

            pruned_expected_people: List[StatePerson] = []
            for person in expected_db_people:
                pruned_expected_person = cast(
                    StatePerson,
                    prune_dangling_placeholders_from_tree(
                        person, field_index=field_index
                    ),
                )
                if pruned_expected_person is not None:
                    pruned_expected_people.append(pruned_expected_person)
            expected_db_people = pruned_expected_people

        if debug:
            if in_ci():
                self.fail("The |debug| flag should only be used for local debugging.")
            if single_person_to_debug is not None:
                found_people = [
                    p for p in found_people if person_has_id(p, single_person_to_debug)
                ]
                expected_db_people = [
                    p
                    for p in expected_db_people
                    if person_has_id(p, single_person_to_debug)
                ]

            print_visible_header_label("FINAL")
            print_entity_trees(
                found_people,
                print_tree_structure_only=print_tree_structure_only,
                field_index=field_index,
            )

            print_visible_header_label("EXPECTED")
            print_entity_trees(
                expected_db_people,
                print_tree_structure_only=print_tree_structure_only,
                field_index=field_index,
            )

        self.assertCountEqual(found_people, expected_db_people)

        assert_no_unexpected_entities_in_db(found_people_from_db, session)
