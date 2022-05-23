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
"""Tests for BaseDirectIngestController.
TODO(#11424): Delete this file once we have migrated all regions to BQ materialization.
"""
import abc
import datetime
import json
import os
import time
import unittest
from collections import defaultdict
from typing import List, Optional, Set, Tuple, Type, TypeVar

import pytest
import pytz
from freezegun import freeze_time
from mock import Mock, patch

from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.io.contents_handle import ContentsHandle
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.common.serialization import (
    attr_from_json_dict,
    attr_to_json_dict,
    datetime_to_serializable,
    serializable_to_datetime,
)
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    build_scheduler_task_id,
)
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    SPLIT_FILE_STORAGE_SUBDIR,
)
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_gating_context import (
    IngestViewMaterializationGatingContext,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_pause_status_manager import (
    DirectIngestInstancePauseStatusManager,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestIngestFileMetadataManager,
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    ExtractAndMergeArgs,
    LegacyExtractAndMergeArgs,
    NewExtractAndMergeArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import DirectIngestError
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_lock_manager import (
    postgres_to_bq_lock_name_for_schema,
)
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct import fixture_util
from recidiviz.tests.ingest.direct.direct_ingest_test_util import (
    path_for_fixture_file,
    run_task_queues_to_empty,
)
from recidiviz.tests.ingest.direct.fakes.fake_direct_ingest_controller import (
    FakeDirectIngestRawFileImportManager,
    FakeDirectIngestRegionRawFileConfig,
    FakeIngestViewMaterializer,
    build_fake_direct_ingest_controller,
)
from recidiviz.tests.ingest.direct.fakes.fake_synchronous_direct_ingest_cloud_task_manager import (
    FakeSynchronousDirectIngestCloudTaskManager,
)
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.regions import Region

DirectIngestControllerT = TypeVar(
    "DirectIngestControllerT", bound=BaseDirectIngestController
)


class BaseDirectIngestControllerForTests(BaseDirectIngestController):
    """Base class for test direct ingest controllers used in this file."""

    def __init__(self, ingest_bucket_path: GcsfsBucketPath):
        super().__init__(ingest_bucket_path)
        self.local_paths: Set[str] = set()

    @abc.abstractmethod
    def get_ingest_view_rank_list(self) -> List[str]:
        pass

    def _get_contents_handle(
        self, args: ExtractAndMergeArgs
    ) -> Optional[ContentsHandle]:
        handle = super()._get_contents_handle(args)
        if handle:
            # TODO(#11424): Once ingest view file elimination complete, we should never
            #  have persisted local ingest files. Remove this check.
            if isinstance(handle, LocalFileContentsHandle):
                self.local_paths.add(handle.local_file_path)
        return handle

    def has_temp_paths_in_disk(self) -> bool:
        for path in self.local_paths:
            if os.path.exists(path):
                return True
        return False


class StateTestDirectIngestController(BaseDirectIngestControllerForTests):
    @classmethod
    def region_code(cls) -> str:
        return "us_xx"

    @property
    def region(self) -> Region:
        return fake_region(
            region_code=self.region_code(),
            environment="production",
            region_module=fake_regions_module,
        )

    def get_ingest_view_rank_list(self) -> List[str]:
        return ["tagFullyEmptyFile", "tagHeadersNoContents", "tagBasicData"]


class StagingOnlyStateTestDirectIngestController(StateTestDirectIngestController):
    @property
    def region(self) -> Region:
        return fake_region(
            region_code=self.region_code(),
            environment="staging",
            region_module=fake_regions_module,
        )


class CrashingStateTestDirectIngestController(StateTestDirectIngestController):
    def get_ingest_view_rank_list(self) -> List[str]:
        return ["tagBasicData"]

    def _run_extract_and_merge_job(self, args: ExtractAndMergeArgs) -> bool:
        raise Exception("insta-crash")


class SingleTagStateTestDirectIngestController(StateTestDirectIngestController):
    def get_ingest_view_rank_list(self) -> List[str]:
        return ["tagBasicData"]


def check_all_paths_processed(
    test_case: unittest.TestCase,
    controller: BaseDirectIngestController,
    file_tags: List[str],
    unexpected_tags: List[str],
) -> None:
    """Checks that all non-directory paths with expected tags have been
    processed and moved to storage.
    """

    if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
        raise ValueError(
            f"Controller fs must have type "
            f"FakeGCSFileSystem. Found instead "
            f"type [{type(controller.fs.gcs_file_system)}]"
        )

    file_tags_processed = set()
    for path in controller.fs.gcs_file_system.all_paths:
        if isinstance(path, GcsfsDirectoryPath):
            continue

        # TODO(#11424): Delete this check once all states have been migrated to BQ-based
        #  ingest view materialization.
        if path == IngestViewMaterializationGatingContext.gating_config_path():
            continue

        file_tag = filename_parts_from_path(path).file_tag

        if file_tag not in unexpected_tags:
            # Test all expected files have been moved to storage
            test_case.assertTrue(
                path.abs_path().startswith(
                    controller.storage_directory_path.abs_path()
                ),
                f"{path} has not been moved to correct storage directory",
            )

            file_tags_processed.add(filename_parts_from_path(path).file_tag)
        else:
            test_case.assertTrue(path.file_name.startswith("unprocessed"))

    # Test that each expected file tag has been processed
    test_case.assertEqual(
        file_tags_processed, set(file_tags).difference(set(unexpected_tags))
    )


def process_task_queues(
    test_case: unittest.TestCase,
    controller: BaseDirectIngestController,
    file_tags: List[str],
    unexpected_tags: List[str] = None,
) -> None:
    if unexpected_tags is None:
        unexpected_tags = []

    run_task_queues_to_empty(controller)
    check_all_paths_processed(test_case, controller, file_tags, unexpected_tags)


def add_paths_with_tags_and_process(
    test_case: unittest.TestCase,
    controller: BaseDirectIngestController,
    file_tags: List[str],
    pre_normalized_file_type: Optional[GcsfsDirectIngestFileType] = None,
    unexpected_tags: List[str] = None,
) -> None:
    """Runs a test that queues files for all the provided file tags, waits
    for the controller to finish processing everything, then makes sure that
    all files not in |unexpected_tags| have been moved to storage.
    """
    add_paths_with_tags(controller, file_tags, pre_normalized_file_type)
    process_task_queues(test_case, controller, file_tags, unexpected_tags)


def add_paths_with_tags(
    controller: BaseDirectIngestController,
    file_tags: List[str],
    pre_normalized_file_type: Optional[GcsfsDirectIngestFileType] = None,
) -> None:
    if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
        raise ValueError(
            f"Controller fs must have type "
            f"FakeGCSFileSystem. Found instead "
            f"type [{type(controller.fs.gcs_file_system)}]"
        )

    for file_tag in file_tags:
        file_path = path_for_fixture_file(
            controller,
            f"{file_tag}.csv",
            should_normalize=bool(pre_normalized_file_type),
            file_type=pre_normalized_file_type,
        )
        # Only get a fixture path if it is a file, if it is a directory leave it as None
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path,
            region_code=controller.region_code(),
        )
        time.sleep(0.05)


_MATERIALIZATION_CONFIG_YAML = """
states:
- US_XX:
   PRIMARY: FILE
   SECONDARY: FILE
"""


@pytest.mark.uses_db
@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging"))
@patch(
    "recidiviz.ingest.direct.views.direct_ingest_big_query_view_types"
    ".get_region_raw_file_config",
    Mock(return_value=FakeDirectIngestRegionRawFileConfig("US_XX")),
)
class FileBaseMaterializationDirectIngestControllerTest(unittest.TestCase):
    """Tests for BaseDirectIngestController that use legacy file-based ingest view
    materialization infrastructure.
    """

    # Stores the location of the postgres DB for this test run
    temp_db_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.operations_database_key = SQLAlchemyDatabaseKey.for_schema(
            SchemaType.OPERATIONS
        )
        local_postgres_helpers.use_on_disk_postgresql_database(
            self.operations_database_key
        )
        self.ingest_instance = DirectIngestInstance.PRIMARY

        # Insert test states into DirectIngestInstancePauseStatus
        for region in ["US_XX", "US_XX_YYYYY"]:
            for instance in DirectIngestInstance:
                DirectIngestInstancePauseStatusManager.add_instance(
                    region, instance, (instance != DirectIngestInstance.PRIMARY)
                )

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(
            self.operations_database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def validate_file_metadata(
        self,
        controller: BaseDirectIngestController,
        expected_raw_metadata_tags_with_is_processed: Optional[
            List[Tuple[str, bool]]
        ] = None,
        expected_ingest_metadata_tags_with_is_processed: Optional[
            List[Tuple[str, bool]]
        ] = None,
    ) -> None:
        """Validates that the file metadata was recorded as expected."""

        if not controller.region.is_ingest_launched_in_env():
            expected_raw_metadata_tags_with_is_processed = []
        elif expected_raw_metadata_tags_with_is_processed is None:
            expected_raw_metadata_tags_with_is_processed = [
                (tag, True) for tag in controller.get_ingest_view_rank_list()
            ]

        if not controller.region.is_ingest_launched_in_env():
            expected_ingest_metadata_tags_with_is_processed = []
        elif expected_ingest_metadata_tags_with_is_processed is None:
            expected_ingest_metadata_tags_with_is_processed = [
                (tag, True) for tag in controller.get_ingest_view_rank_list()
            ]

        raw_file_metadata_manager = controller.raw_file_metadata_manager
        if not isinstance(
            raw_file_metadata_manager, PostgresDirectIngestRawFileMetadataManager
        ):
            self.fail(
                f"Unexpected raw_file_metadata_manager type {raw_file_metadata_manager}"
            )

        with SessionFactory.using_database(
            self.operations_database_key, autocommit=False
        ) as session:
            raw_file_results = session.query(schema.DirectIngestRawFileMetadata).all()

            raw_file_metadata_list = [
                # pylint:disable=protected-access
                raw_file_metadata_manager._raw_file_schema_metadata_as_entity(metadata)
                for metadata in raw_file_results
            ]

            ingest_file_results = session.query(
                schema.DirectIngestIngestFileMetadata
            ).all()

            ingest_file_metadata_list = [
                # pylint:disable=protected-access
                PostgresDirectIngestIngestFileMetadataManager._ingest_file_schema_metadata_as_entity(
                    metadata
                )
                for metadata in ingest_file_results
            ]

        actual_raw_metadata_tags_with_is_processed = [
            (metadata.file_tag, bool(metadata.processed_time))
            for metadata in raw_file_metadata_list
        ]

        if not controller.region.is_ingest_launched_in_env():
            self.assertEqual([], actual_raw_metadata_tags_with_is_processed)

        self.assertEqual(
            sorted(expected_raw_metadata_tags_with_is_processed),
            sorted(actual_raw_metadata_tags_with_is_processed),
            "Raw file metadata does not match expected",
        )

        actual_ingest_metadata_tags_with_is_processed = [
            (metadata.file_tag, bool(metadata.processed_time))
            for metadata in ingest_file_metadata_list
        ]

        if not controller.region.is_ingest_launched_in_env():
            self.assertEqual([], actual_ingest_metadata_tags_with_is_processed)

        self.assertEqual(
            sorted(expected_ingest_metadata_tags_with_is_processed),
            sorted(actual_ingest_metadata_tags_with_is_processed),
            "Ingest file metadata does not match expected",
        )

    def run_async_file_order_test_for_controller_cls(
        self, controller_cls: Type[DirectIngestControllerT]
    ) -> BaseDirectIngestControllerForTests:
        """Writes all expected files to the mock fs, then kicks the controller
        and ensures that all jobs are run to completion in the proper order."""

        controller = build_fake_direct_ingest_controller(
            controller_cls,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        file_tags = list(reversed(sorted(controller.get_ingest_view_rank_list())))

        add_paths_with_tags_and_process(self, controller, file_tags)

        if not isinstance(controller, BaseDirectIngestControllerForTests):
            self.fail("Controller is not of type BaseDirectIngestControllerForTests.")
        self.assertFalse(controller.has_temp_paths_in_disk())

        self.validate_file_metadata(controller)

        return controller

    def check_tags(
        self, controller: BaseDirectIngestController, tags: List[str]
    ) -> None:
        materializer = controller.ingest_view_materializer
        if not isinstance(materializer, FakeIngestViewMaterializer):
            self.fail("Expected FakeIngestViewMaterializer but did not find one.")

        self.assertCountEqual(
            tags,
            materializer.get_materialized_ingest_views(),
        )

    def get_fake_task_manager(
        self, controller: BaseDirectIngestController
    ) -> FakeSynchronousDirectIngestCloudTaskManager:
        if not isinstance(
            controller.cloud_task_manager, FakeSynchronousDirectIngestCloudTaskManager
        ):
            self.fail("Expected FakeSynchronousDirectIngestCouldManager")
        return controller.cloud_task_manager

    def check_imported_path_count(
        self, controller: BaseDirectIngestController, expected_count: int
    ) -> None:
        if not isinstance(
            controller.raw_file_import_manager, FakeDirectIngestRawFileImportManager
        ):
            self.fail(
                f"Unexpected type for raw_file_import_manager: {controller.raw_file_import_manager}"
            )
        self.assertEqual(
            expected_count, len(controller.raw_file_import_manager.imported_paths)
        )

    def add_and_process_through_ingest_view_materialization(
        self,
        controller: BaseDirectIngestController,
        file_path: GcsfsFilePath,
        clear_scheduler_queue_after: bool = False,
    ) -> None:
        """Adds a file path and runs all ingest steps through the resulting ingest view
        materialization tasks, but does not process any new ingest view results.
        """
        task_manager = self.get_fake_task_manager(controller)
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path,
            region_code=controller.region_code(),
        )

        # Tasks for handling unnormalized file_path
        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()
        # file_path raw data import
        task_manager.test_run_next_raw_data_import_task()
        task_manager.test_pop_finished_raw_data_import_task()
        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()
        # file_path ingest view materialization
        task_manager.test_run_next_ingest_view_materialization_task()
        task_manager.test_pop_finished_ingest_view_materialization_task()

        if clear_scheduler_queue_after:
            while task_manager.scheduler_tasks:
                task_manager.test_run_next_scheduler_task()
                task_manager.test_pop_finished_scheduler_task()

    def test_state_runs_files_in_order(self) -> None:
        controller = self.run_async_file_order_test_for_controller_cls(
            StateTestDirectIngestController
        )

        self.check_imported_path_count(controller, 3)

        self.check_tags(controller, controller.get_ingest_view_rank_list())

    def test_state_doesnt_run_when_paused(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )
        controller.ingest_instance_status_manager.pause_instance()

        file_tags = list(reversed(sorted(controller.get_ingest_view_rank_list())))
        add_paths_with_tags(controller, file_tags, None)
        run_task_queues_to_empty(controller)

        # No files should be imported because we are paused
        self.check_imported_path_count(controller, 0)

    def test_state_generate_files_after_deprecation(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        file_tags = list(reversed(sorted(controller.get_ingest_view_rank_list())))
        add_paths_with_tags(
            controller,
            file_tags,
            pre_normalized_file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        run_task_queues_to_empty(controller)

        self.validate_file_metadata(controller)

        if not controller.is_bq_materialization_enabled:
            if not controller.ingest_file_metadata_manager:
                raise ValueError(
                    "Legacy ingest_file_metadata_manager is unexpectedly None."
                )
            controller.ingest_file_metadata_manager.clear_ingest_file_metadata()
        else:
            if not controller.view_materialization_metadata_manager:
                raise ValueError(
                    "The view_materialization_metadata_manager is unexpectedly None."
                )
            controller.view_materialization_metadata_manager.clear_instance_metadata()
        if not isinstance(
            controller.ingest_view_materializer, FakeIngestViewMaterializer
        ):
            raise ValueError(
                f"Unexpected materializer type [{controller.ingest_view_materializer}]"
            )
        controller.ingest_view_materializer.processed_args = []

        # We should now have no ingest metadata rows
        self.validate_file_metadata(
            controller, expected_ingest_metadata_tags_with_is_processed=[]
        )

        controller.cloud_task_manager.create_direct_ingest_handle_new_files_task(
            region=controller.region,
            ingest_bucket=controller.ingest_bucket_path,
            can_start_ingest=True,
        )

        run_task_queues_to_empty(controller)

        # Expect that files have been processed again
        self.validate_file_metadata(controller)

    @patch(
        "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
        Mock(return_value=FakeGCSFileSystem()),
    )
    def test_state_runs_files_in_order_locking(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )
        controller.region_lock_manager.lock_manager.lock(
            postgres_to_bq_lock_name_for_schema(SchemaType.STATE)
        )
        file_tags = list(reversed(sorted(controller.get_ingest_view_rank_list())))
        add_paths_with_tags(controller, file_tags)
        run_task_queues_to_empty(controller)

        if not isinstance(controller, BaseDirectIngestControllerForTests):
            self.fail("Controller is not of type BaseDirectIngestControllerForTests.")

        self.assertFalse(controller.has_temp_paths_in_disk())
        self.validate_file_metadata(
            controller,
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", False),
                ("tagHeadersNoContents", False),
                ("tagBasicData", False),
            ],
        )
        controller.region_lock_manager.lock_manager.unlock(
            postgres_to_bq_lock_name_for_schema(SchemaType.STATE)
        )
        controller.kick_scheduler(just_finished_job=False)
        run_task_queues_to_empty(controller)

        check_all_paths_processed(
            self,
            controller,
            ["tagFullyEmptyFile", "tagHeadersNoContents", "tagBasicData"],
            unexpected_tags=[],
        )
        self.check_imported_path_count(controller, 3)
        self.check_tags(controller, controller.get_ingest_view_rank_list())
        self.validate_file_metadata(
            controller,
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", True),
                ("tagHeadersNoContents", True),
                ("tagBasicData", True),
            ],
        )

    @patch(
        "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
        Mock(return_value=FakeGCSFileSystem()),
    )
    def test_state_runs_files_in_order_locking_region(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )
        with controller.region_lock_manager.using_region_lock(expiration_in_seconds=10):
            file_tags = list(reversed(sorted(controller.get_ingest_view_rank_list())))
            add_paths_with_tags(controller, file_tags)
            run_task_queues_to_empty(controller)

        if not isinstance(controller, BaseDirectIngestControllerForTests):
            self.fail("Controller is not of type BaseDirectIngestControllerForTests.")

        self.assertFalse(controller.has_temp_paths_in_disk())
        self.validate_file_metadata(
            controller,
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", False),
                ("tagHeadersNoContents", False),
                ("tagBasicData", False),
            ],
        )

        # Try again with lock no longer held
        controller.kick_scheduler(just_finished_job=False)
        run_task_queues_to_empty(controller)

        check_all_paths_processed(
            self,
            controller,
            ["tagFullyEmptyFile", "tagHeadersNoContents", "tagBasicData"],
            unexpected_tags=[],
        )
        self.check_imported_path_count(controller, 3)
        self.check_tags(controller, controller.get_ingest_view_rank_list())
        self.validate_file_metadata(
            controller,
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", True),
                ("tagHeadersNoContents", True),
                ("tagBasicData", True),
            ],
        )

    @patch(
        "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
        Mock(return_value=FakeGCSFileSystem()),
    )
    def test_crash_releases_lock(self) -> None:
        controller = build_fake_direct_ingest_controller(
            CrashingStateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )
        file_tags = list(reversed(sorted(controller.get_ingest_view_rank_list())))
        add_paths_with_tags(controller, file_tags)
        with self.assertRaises(Exception):
            run_task_queues_to_empty(controller)

        # Assert that there are no leftover locks
        self.assertTrue(
            controller.region_lock_manager.lock_manager.no_active_locks_with_prefix("")
        )

    def test_state_single_split_tag(self) -> None:
        controller = build_fake_direct_ingest_controller(
            SingleTagStateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        file_tags: List[str] = ["tagBasicData"]
        unexpected_tags: List[str] = []

        add_paths_with_tags_and_process(
            self,
            controller,
            file_tags,
            unexpected_tags=unexpected_tags,
        )

        processed_split_file_paths = defaultdict(list)
        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )
        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            if self._path_in_split_file_storage_subdir(path, controller):
                file_tag = filename_parts_from_path(path).file_tag
                processed_split_file_paths[file_tag].append(path)

        self.assertEqual(1, len(processed_split_file_paths.keys()))
        self.assertEqual(2, len(processed_split_file_paths["tagBasicData"]))

        found_suffixes = {
            filename_parts_from_path(p).filename_suffix
            for p in processed_split_file_paths["tagBasicData"]
            if isinstance(p, GcsfsFilePath)
        }
        self.assertEqual(
            found_suffixes, {"00001_file_split_size1", "00002_file_split_size1"}
        )

        self.check_imported_path_count(controller, 1)

        expected_raw_metadata_tags_with_is_processed = [("tagBasicData", True)]
        expected_ingest_metadata_tags_with_is_processed = [
            ("tagBasicData", True),
            ("tagBasicData", True),
            ("tagBasicData", True),
        ]
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed,
            expected_ingest_metadata_tags_with_is_processed=expected_ingest_metadata_tags_with_is_processed,
        )

        self.check_tags(controller, controller.get_ingest_view_rank_list())

    def test_state_unexpected_tag(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        file_tags = [
            "tagFullyEmptyFile",
            "Unexpected_Tag",
            "tagHeadersNoContents",
            "tagBasicData",
        ]
        unexpected_tags = ["Unexpected_Tag"]

        add_paths_with_tags_and_process(
            self,
            controller,
            file_tags,
            unexpected_tags=unexpected_tags,
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )
        split_paths = {
            path
            for path in controller.fs.gcs_file_system.all_paths
            if isinstance(path, GcsfsFilePath)
            and path != IngestViewMaterializationGatingContext.gating_config_path()
            and controller.fs.is_split_file(path)
        }
        self.assertFalse(split_paths)

        self.check_imported_path_count(controller, 3)

        expected_raw_metadata_tags_with_is_processed = [
            ("tagFullyEmptyFile", True),
            ("tagHeadersNoContents", True),
            ("tagBasicData", True),
            ("Unexpected_Tag", False),
        ]
        expected_ingest_metadata_tags_with_is_processed = [
            ("tagFullyEmptyFile", True),
            ("tagHeadersNoContents", True),
            ("tagBasicData", True),
        ]
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed,
            expected_ingest_metadata_tags_with_is_processed=expected_ingest_metadata_tags_with_is_processed,
        )

        self.check_tags(controller, controller.get_ingest_view_rank_list())

    def test_state_tag_we_import_but_do_not_ingest(
        self,
    ) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        file_tags = [
            "tagFullyEmptyFile",
            "tagWeDoNotIngest",
            "tagHeadersNoContents",
            "tagBasicData",
        ]

        add_paths_with_tags_and_process(self, controller, file_tags)

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )
        split_paths = {
            path
            for path in controller.fs.gcs_file_system.all_paths
            if isinstance(path, GcsfsFilePath)
            and path != IngestViewMaterializationGatingContext.gating_config_path()
            and controller.fs.is_split_file(path)
        }
        self.assertFalse(split_paths)

        self.check_imported_path_count(controller, 4)

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                (tag, True) for tag in file_tags
            ],
            expected_ingest_metadata_tags_with_is_processed=[
                (tag, True)
                for tag in ["tagFullyEmptyFile", "tagHeadersNoContents", "tagBasicData"]
            ],
        )

        self.check_tags(
            controller, ["tagFullyEmptyFile", "tagHeadersNoContents", "tagBasicData"]
        )

    def test_do_not_queue_same_job_twice(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        task_manager = self.get_fake_task_manager(controller)

        file_path = path_for_fixture_file(
            controller,
            "tagFullyEmptyFile.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        file_path2 = path_for_fixture_file(
            controller,
            "tagHeadersNoContents.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        self.add_and_process_through_ingest_view_materialization(
            controller, file_path, clear_scheduler_queue_after=True
        )

        # Single process job request should be waiting for the first file
        self.assertEqual(
            task_manager.get_extract_and_merge_queue_info(
                controller.region,
                controller.ingest_instance,
                controller.is_bq_materialization_enabled,
            ).size(),
            1,
        )

        self.add_and_process_through_ingest_view_materialization(controller, file_path2)

        task_manager.test_run_next_extract_and_merge_task()
        task_manager.test_pop_finished_extract_and_merge_task()

        self.assertEqual(
            task_manager.get_extract_and_merge_queue_info(
                controller.region,
                controller.ingest_instance,
                controller.is_bq_materialization_enabled,
            ).size(),
            0,
        )

        # This is the task that got queued by after we normalized the path,
        # which will schedule the next extract_and_merge.
        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        # These are the tasks that got queued by finishing an ingest view
        # materialization job.
        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(
                controller.region, controller.ingest_instance
            ).size(),
        )
        self.assertEqual(
            1,
            task_manager.get_extract_and_merge_queue_info(
                controller.region,
                controller.ingest_instance,
                controller.is_bq_materialization_enabled,
            ).size(),
        )

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", True),
                ("tagHeadersNoContents", True),
            ],
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", True),
                ("tagHeadersNoContents", False),  # This view has not been ingested yet
            ],
        )

    def test_next_schedule_runs_before_extract_and_merge_clears(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )
        task_manager = self.get_fake_task_manager(controller)

        file_path = path_for_fixture_file(
            controller,
            "tagFullyEmptyFile.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        self.add_and_process_through_ingest_view_materialization(
            controller, file_path, clear_scheduler_queue_after=True
        )

        self.assertEqual(
            1,
            task_manager.get_extract_and_merge_queue_info(
                controller.region,
                controller.ingest_instance,
                controller.is_bq_materialization_enabled,
            ).size(),
        )

        file_path2 = path_for_fixture_file(
            controller,
            "tagHeadersNoContents.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        self.add_and_process_through_ingest_view_materialization(controller, file_path2)

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        # At this point we have a series of tasks handling / renaming /
        # splitting the new ingest view files, then scheduling the next job. They run in
        # quick succession.
        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        # Process job tasks starts as a result of the first schedule.
        task_manager.test_run_next_extract_and_merge_task()

        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_extract_and_merge_task()
        task_manager.test_pop_finished_scheduler_task()

        # We should have still queued a process job, even though the last
        # one hadn't run when schedule executes
        task_manager.test_run_next_extract_and_merge_task()
        task_manager.test_pop_finished_extract_and_merge_task()

        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(
                controller.region, controller.ingest_instance
            ).size(),
        )
        self.assertEqual(
            0,
            task_manager.get_extract_and_merge_queue_info(
                controller.region,
                controller.ingest_instance,
                controller.is_bq_materialization_enabled,
            ).size(),
        )
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", True),
                ("tagHeadersNoContents", True),
            ],
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", True),
                ("tagHeadersNoContents", True),
            ],
        )

    def test_extract_and_merge_task_run_twice(self) -> None:
        # Cloud Tasks has an at-least once guarantee - make sure rerunning a task in series does not crash
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            self.fail("Expected FakeGCSFileSystem")

        task_manager = self.get_fake_task_manager(controller)

        dt = datetime.datetime.now()
        file_path = path_for_fixture_file(
            controller,
            "tagFullyEmptyFile.csv",
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            should_normalize=True,
            dt=dt,
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path,
            region_code=controller.region_code(),
        )
        parts = filename_parts_from_path(file_path)

        delegate = controller.ingest_view_materialization_args_generator.delegate
        args = delegate.build_new_args(
            ingest_view_name=parts.file_tag,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=dt,
        )
        delegate.register_new_job(args)

        # TODO(#11424): Delete this block once BQ-materialization is enabled everywhere.
        if controller.ingest_file_metadata_manager:
            metadata = controller.ingest_file_metadata_manager.get_ingest_view_metadata_for_export_job(
                args
            )
            controller.ingest_file_metadata_manager.register_ingest_view_export_file_name(
                metadata, file_path
            )

        # At this point we have a series of tasks handling / renaming /
        # splitting the new files, then scheduling the next job. They run in
        # quick succession.
        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        # Process job tasks starts as a result of the first schedule.
        task_manager.test_run_next_extract_and_merge_task()

        _, args = task_manager.test_pop_finished_extract_and_merge_task()

        task_manager.create_direct_ingest_extract_and_merge_task(
            controller.region, args, controller.is_bq_materialization_enabled
        )

        # Now run the repeated task
        task_manager.test_run_next_extract_and_merge_task()
        task_manager.test_pop_finished_extract_and_merge_task()

        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(
                controller.region, controller.ingest_instance
            ).size(),
        )
        self.assertEqual(
            0,
            task_manager.get_extract_and_merge_queue_info(
                controller.region,
                controller.ingest_instance,
                controller.is_bq_materialization_enabled,
            ).size(),
        )
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[],
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", True)
            ],
        )

    def test_process_already_normalized_paths(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        file_tags = list(sorted(controller.get_ingest_view_rank_list()))

        add_paths_with_tags_and_process(
            self,
            controller,
            file_tags,
            pre_normalized_file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        self.validate_file_metadata(controller)

    def _path_in_storage_dir(
        self, path: GcsfsFilePath, controller: BaseDirectIngestController
    ) -> bool:
        return path.abs_path().startswith(controller.storage_directory_path.abs_path())

    def _path_in_split_file_storage_subdir(
        self, path: GcsfsFilePath, controller: BaseDirectIngestController
    ) -> bool:
        if self._path_in_storage_dir(path, controller):
            directory, _ = os.path.split(path.abs_path())
            if SPLIT_FILE_STORAGE_SUBDIR in directory:
                return True
        return False

    def test_process_ingest_view_file_that_needs_splitting(
        self,
    ) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        file_tags = list(sorted(controller.get_ingest_view_rank_list()))

        add_paths_with_tags_and_process(
            self,
            controller,
            file_tags,
            pre_normalized_file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        processed_split_file_paths = defaultdict(list)
        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )
        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            if self._path_in_split_file_storage_subdir(path, controller):
                file_tag = filename_parts_from_path(path).file_tag
                processed_split_file_paths[file_tag].append(path)

        self.assertEqual(1, len(processed_split_file_paths.keys()))
        self.assertEqual(2, len(processed_split_file_paths["tagBasicData"]))

        found_suffixes = {
            filename_parts_from_path(p).filename_suffix
            for p in processed_split_file_paths["tagBasicData"]
            if isinstance(p, GcsfsFilePath)
        }
        self.assertEqual(
            found_suffixes, {"00001_file_split_size1", "00002_file_split_size1"}
        )
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                (tag, True) for tag in controller.get_ingest_view_rank_list()
            ],
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", True),
                ("tagHeadersNoContents", True),
                ("tagBasicData", True),
                ("tagBasicData", True),
                ("tagBasicData", True),
            ],
        )

        self.check_imported_path_count(controller, 3)

        self.check_tags(controller, controller.get_ingest_view_rank_list())

    @patch.object(
        BaseDirectIngestController,
        "_split_file_if_necessary",
        Mock(side_effect=ValueError("Splitting crashed")),
    )
    def test_failing_to_process_a_file_that_needs_splitting_no_loop(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        task_manager = self.get_fake_task_manager(controller)

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        # This file exceeds the split limit, but since we have mocked
        # _split_files_if_necessary to do nothing, it won't get split.
        file_path = path_for_fixture_file(
            controller,
            "tagBasicData.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        self.add_and_process_through_ingest_view_materialization(controller, file_path)

        while task_manager.scheduler_tasks:
            with self.assertRaisesRegex(ValueError, "^Splitting crashed$"):
                task_manager.test_run_next_scheduler_task()
            task_manager.scheduler_tasks.pop(0)

        controller.kick_scheduler(just_finished_job=False)

        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        task_manager.test_run_next_extract_and_merge_task()
        task_manager.test_pop_finished_extract_and_merge_task()

        # The process job task, which will try to process a file that is too
        # big, will not schedule another job for the same file (which would
        # just get us in a loop).
        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(
                controller.region, controller.ingest_instance
            ).size(),
        )
        self.assertEqual(
            0,
            task_manager.get_extract_and_merge_queue_info(
                controller.region,
                controller.ingest_instance,
                controller.is_bq_materialization_enabled,
            ).size(),
        )
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[("tagBasicData", True)],
            expected_ingest_metadata_tags_with_is_processed=[("tagBasicData", False)],
        )

    def test_move_files_from_previous_days_to_storage(
        self,
    ) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        prev_date_datetime = datetime.datetime.fromisoformat("2019-09-15")
        file_path_from_prev_day = path_for_fixture_file(
            controller,
            "tagHeadersNoContents.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            dt=prev_date_datetime,
        )

        # pylint:disable=protected-access
        processed_file_from_prev_day = controller.fs._to_processed_file_path(
            file_path_from_prev_day
        )

        unexpected_file_path_from_prev_day = path_for_fixture_file(
            controller,
            "Unexpected_Tag.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
            dt=prev_date_datetime,
        )

        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            processed_file_from_prev_day,
            region_code=controller.region_code(),
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            unexpected_file_path_from_prev_day,
            region_code=controller.region_code(),
        )

        file_tags = list(sorted(controller.get_ingest_view_rank_list()))

        unexpected_tags = ["Unexpected_Tag"]

        # This will test that all paths get moved to storage,
        # except the unexpected tag.
        add_paths_with_tags_and_process(
            self, controller, file_tags, unexpected_tags=unexpected_tags
        )

        paths_from_prev_date = []
        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )
        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            # TODO(#11424): Delete this check once all states have been migrated to BQ-based
            #  ingest view materialization.
            if path == IngestViewMaterializationGatingContext.gating_config_path():
                continue
            parts = filename_parts_from_path(path)
            expected_storage_dir_str = os.path.join(
                controller.storage_directory_path.abs_path(),
                parts.file_type.value,
                f"{prev_date_datetime.year:04}",
                f"{prev_date_datetime.month:02}",
                f"{prev_date_datetime.day:02}",
            )
            if path.abs_path().startswith(expected_storage_dir_str):
                paths_from_prev_date.append(path)

        self.assertTrue(len(paths_from_prev_date), 1)
        self.assertTrue("tagHeadersNoContents" in paths_from_prev_date[0].abs_path())
        expected_raw_metadata_tags_with_is_processed = [
            (tag, True) for tag in file_tags
        ]
        expected_raw_metadata_tags_with_is_processed.extend(
            [(tag, False) for tag in unexpected_tags]
        )
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed,
            expected_ingest_metadata_tags_with_is_processed=[
                (tag, True) for tag in file_tags
            ],
        )

        self.check_tags(controller, controller.get_ingest_view_rank_list())

    def test_move_files_from_previous_days_to_storage_incomplete_current_day(
        self,
    ) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        current_date_datetime = datetime.datetime.now(tz=pytz.UTC)
        prev_date_datetime = current_date_datetime - datetime.timedelta(days=1)

        with freeze_time(prev_date_datetime.isoformat()):
            unexpected_file_path_from_prev_day = path_for_fixture_file(
                controller,
                "Unexpected_Tag.csv",
                should_normalize=True,
                file_type=GcsfsDirectIngestFileType.RAW_DATA,
            )

            processed_file_from_prev_day = path_for_fixture_file(
                controller,
                "tagHeadersNoContents.csv",
                should_normalize=True,
                file_type=GcsfsDirectIngestFileType.RAW_DATA,
            )

            fixture_util.add_direct_ingest_path(
                controller.fs.gcs_file_system,
                unexpected_file_path_from_prev_day,
                region_code=controller.region_code(),
            )
            fixture_util.add_direct_ingest_path(
                controller.fs.gcs_file_system,
                processed_file_from_prev_day,
                region_code=controller.region_code(),
            )

        run_task_queues_to_empty(controller)

        file_path_from_current_day = path_for_fixture_file(
            controller,
            "tagFullyEmptyFile.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path_from_current_day,
            region_code=controller.region_code(),
        )

        run_task_queues_to_empty(controller)

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )
        self.assertTrue(len(controller.fs.gcs_file_system.all_paths), 3)

        storage_paths: List[GcsfsFilePath] = []
        processed_paths: List[GcsfsFilePath] = []
        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            if self._path_in_storage_dir(path, controller):
                if "Unexpected_Tag" in path.abs_path():
                    self.fail("Unexpected tag found in storage dir")
                storage_paths.append(path)
            if controller.fs.is_processed_file(path):
                processed_paths.append(path)

        self.assertEqual(len(storage_paths), 3)

        expected_raw_storage_dir_prefix = os.path.join(
            controller.storage_directory_path.abs_path(),
            GcsfsDirectIngestFileType.RAW_DATA.value,
        )

        expected_ingest_storage_dir_prefix = os.path.join(
            controller.storage_directory_path.abs_path(),
            GcsfsDirectIngestFileType.INGEST_VIEW.value,
        )
        self.assertTrue(
            storage_paths[0].abs_path().startswith(expected_raw_storage_dir_prefix)
            or storage_paths[0]
            .abs_path()
            .startswith(expected_ingest_storage_dir_prefix),
            f"Path {storage_paths[0].abs_path()} should start with {expected_raw_storage_dir_prefix} or "
            f"{expected_ingest_storage_dir_prefix}.",
        )

        # Paths that are moved retain their 'processed_' prefix.
        self.assertEqual(len(processed_paths), 4)

        processed_paths_not_in_storage = [
            path
            for path in processed_paths
            if not self._path_in_storage_dir(path, controller)
        ]

        self.assertEqual(len(processed_paths_not_in_storage), 1)

        processed_path_str = processed_paths_not_in_storage[0].abs_path()
        self.assertTrue(
            processed_path_str.startswith(controller.ingest_bucket_path.abs_path())
        )
        self.assertTrue("tagFullyEmptyFile" in processed_path_str)
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                ("Unexpected_Tag", False),
                ("tagFullyEmptyFile", True),
                ("tagHeadersNoContents", True),
            ],
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagFullyEmptyFile", True),
                ("tagHeadersNoContents", True),
            ],
        )

        self.check_tags(controller, ["tagFullyEmptyFile", "tagHeadersNoContents"])

    def test_cloud_function_fails_on_new_file(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        task_manager = self.get_fake_task_manager(controller)

        file_path = path_for_fixture_file(
            controller,
            "tagFullyEmptyFile.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        file_path2 = path_for_fixture_file(
            controller,
            "tagHeadersNoContents.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        file_path3 = path_for_fixture_file(
            controller,
            "tagBasicData.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        # Upload two new files without triggering the controller
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path,
            region_code=controller.region_code(),
            fail_handle_file_call=True,
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path2,
            region_code=controller.region_code(),
            fail_handle_file_call=True,
        )

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(
                controller.region, controller.ingest_instance
            ).size(),
        )
        self.assertEqual(
            0,
            task_manager.get_extract_and_merge_queue_info(
                controller.region,
                controller.ingest_instance,
                controller.is_bq_materialization_enabled,
            ).size(),
        )

        # Later file that succeeds will trigger proper upload of all files
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path3,
            region_code=controller.region_code(),
        )

        run_task_queues_to_empty(controller)
        check_all_paths_processed(
            self,
            controller,
            ["tagFullyEmptyFile", "tagHeadersNoContents", "tagBasicData"],
            unexpected_tags=[],
        )
        self.validate_file_metadata(controller)

    def test_cloud_function_fails_on_new_file_rename_later_with_cron(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        task_manager = self.get_fake_task_manager(controller)

        file_path = path_for_fixture_file(
            controller,
            "tagFullyEmptyFile.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        file_path2 = path_for_fixture_file(
            controller,
            "tagHeadersNoContents.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        file_path3 = path_for_fixture_file(
            controller,
            "tagBasicData.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        # Upload new files without triggering the controller
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path,
            region_code=controller.region_code(),
            fail_handle_file_call=True,
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path2,
            region_code=controller.region_code(),
            fail_handle_file_call=True,
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            file_path3,
            region_code=controller.region_code(),
            fail_handle_file_call=True,
        )

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(
                controller.region, controller.ingest_instance
            ).size(),
        )
        self.assertEqual(
            0,
            task_manager.get_extract_and_merge_queue_info(
                controller.region,
                controller.ingest_instance,
                controller.is_bq_materialization_enabled,
            ).size(),
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )
        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            self.assertFalse(controller.fs.is_normalized_file_path(path))

        # Cron job to handle unseen files triggers later
        controller.cloud_task_manager.create_direct_ingest_handle_new_files_task(
            controller.region,
            controller.ingest_bucket_path,
            can_start_ingest=True,
        )

        run_task_queues_to_empty(controller)

        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            # TODO(#11424): Delete this check once all states have been migrated to BQ-based
            #  ingest view materialization.
            if path == IngestViewMaterializationGatingContext.gating_config_path():
                continue
            self.assertTrue(controller.fs.is_normalized_file_path(path))

        self.validate_file_metadata(controller)

    def test_can_start_ingest_is_false_launched_region_throws(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            can_start_ingest=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            Mock(return_value="production"),
        ):
            file_tags = list(reversed(sorted(controller.get_ingest_view_rank_list())))

            add_paths_with_tags(controller, file_tags)
            with self.assertRaisesRegex(
                ValueError,
                "^The can_start_ingest flag should only be used for regions where ingest is not yet"
                " launched in a particular environment. If we want to be able to selectively pause"
                " ingest processing for a state, we will first have to build a config that is"
                " respected by both the /ensure_all_raw_file_paths_normalized endpoint and any cloud"
                " functions that trigger ingest.$",
            ):
                run_task_queues_to_empty(controller)

    def test_can_start_ingest_is_false_does_not_start_ingest(
        self,
    ) -> None:
        controller = build_fake_direct_ingest_controller(
            StagingOnlyStateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            can_start_ingest=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        file_tags = list(reversed(sorted(controller.get_ingest_view_rank_list())))

        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            Mock(return_value="production"),
        ):
            add_paths_with_tags(controller, file_tags)
            run_task_queues_to_empty(controller)

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )
        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            # TODO(#11424): Delete this check once all states have been migrated to BQ-based
            #  ingest view materialization.
            if path == IngestViewMaterializationGatingContext.gating_config_path():
                continue
            self.assertTrue(controller.fs.is_normalized_file_path(path))
            self.assertTrue(controller.fs.is_seen_unprocessed_file(path))
            self.assertFalse(controller.fs.is_split_file(path))
            self.assertFalse(controller.fs.is_processed_file(path))
            self.assertEqual(
                GcsfsDirectIngestFileType.RAW_DATA,
                filename_parts_from_path(path).file_type,
            )

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[],
            expected_ingest_metadata_tags_with_is_processed=[],
        )

    def test_unlaunched_region_raises_if_can_start_ingest_true(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StagingOnlyStateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )
        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            Mock(return_value="production"),
        ):
            add_paths_with_tags(controller, ["tagFullyEmptyFile"])
            with self.assertRaisesRegex(
                DirectIngestError,
                r"^Bad environment \[production\] for region \[us_xx\].$",
            ):
                run_task_queues_to_empty(controller)

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )
        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            # TODO(#11424): Delete this check once all states have been migrated to BQ-based
            #  ingest view materialization.
            if path == IngestViewMaterializationGatingContext.gating_config_path():
                continue
            self.assertTrue(controller.fs.is_normalized_file_path(path))
            self.assertTrue(controller.fs.is_seen_unprocessed_file(path))
            self.assertFalse(controller.fs.is_split_file(path))
            self.assertFalse(controller.fs.is_processed_file(path))
            self.assertEqual(
                GcsfsDirectIngestFileType.RAW_DATA,
                filename_parts_from_path(path).file_type,
            )

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[],
            expected_ingest_metadata_tags_with_is_processed=[],
        )

    def test_launched_region_raises_if_can_start_ingest_false(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=True,
            can_start_ingest=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        add_paths_with_tags(controller, ["tagFullyEmptyFile"])
        with self.assertRaisesRegex(
            ValueError,
            "^The can_start_ingest flag should only be used for regions where ingest is not yet launched",
        ):
            run_task_queues_to_empty(controller)

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )
        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            self.assertFalse(controller.fs.is_normalized_file_path(path))

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[],
            expected_ingest_metadata_tags_with_is_processed=[],
        )

    def test_processing_continues_if_there_are_subfolders_in_ingest_dir(self) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        subdir_path_name = "subdir/"
        subdir_path = GcsfsDirectoryPath.from_dir_and_subdir(
            controller.ingest_bucket_path, subdir_path_name
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            subdir_path,
            region_code=controller.region_code(),
            has_fixture=False,
        )
        path_names = [
            ("subdir/Unexpected_Tag.csv", False),
            ("tagFullyEmptyFile.csv", True),
            ("tagHeadersNoContents.csv", True),
            ("tagBasicData.csv", True),
            ("subdir/tagBasicData2.csv", False),
        ]

        for path_name, has_fixture in path_names:
            fixture_util.add_direct_ingest_path(
                controller.fs.gcs_file_system,
                path_for_fixture_file(
                    controller,
                    path_name,
                    should_normalize=False,
                    file_type=GcsfsDirectIngestFileType.RAW_DATA,
                ),
                region_code=controller.region_code(),
                has_fixture=has_fixture,
            )

        run_task_queues_to_empty(controller)

        dir_paths_found = []
        storage_file_paths = []
        ingest_file_paths = []

        for path in controller.fs.gcs_file_system.all_paths:
            # TODO(#11424): Delete this check once all states have been migrated to BQ-based
            #  ingest view materialization.
            if path == IngestViewMaterializationGatingContext.gating_config_path():
                continue
            if isinstance(path, GcsfsDirectoryPath):
                dir_paths_found.append(path)
                continue

            if path.abs_path().startswith(controller.storage_directory_path.abs_path()):
                storage_file_paths.append(path)
            else:
                self.assertTrue(
                    path.abs_path().startswith(controller.ingest_bucket_path.abs_path())
                )
                ingest_file_paths.append(path)

        self.assertEqual(1, len(dir_paths_found))
        self.assertEqual(subdir_path, dir_paths_found[0])

        # Three raw data files and three ingest view files
        self.assertEqual(6, len(storage_file_paths))
        storage_tags = {
            filename_parts_from_path(path).file_tag for path in storage_file_paths
        }
        self.assertEqual(
            {"tagFullyEmptyFile", "tagHeadersNoContents", "tagBasicData"}, storage_tags
        )

        for path in storage_file_paths:
            self.assertTrue(controller.fs.is_normalized_file_path(path))
            self.assertTrue(controller.fs.is_processed_file(path))

        self.assertEqual(2, len(ingest_file_paths))
        ingest_tags = {
            filename_parts_from_path(path).file_tag for path in ingest_file_paths
        }
        self.assertEqual({"tagBasicData2", "Unexpected_Tag"}, ingest_tags)

        for path in ingest_file_paths:
            self.assertTrue(controller.fs.is_normalized_file_path(path))
            self.assertTrue(controller.fs.is_seen_unprocessed_file(path))
            self.assertEqual(subdir_path, GcsfsDirectoryPath.from_file_path(path))

        self.validate_file_metadata(controller)

    def test_do_not_schedule_raw_data_import_task_if_already_processed_yet_in_gcs(
        self,
    ) -> None:
        controller = build_fake_direct_ingest_controller(
            StateTestDirectIngestController,
            ingest_instance=self.ingest_instance,
            run_async=False,
            materialization_config_yaml=_MATERIALIZATION_CONFIG_YAML,
        )
        path_to_fixture = path_for_fixture_file(
            controller,
            "tagFullyEmptyFile.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system,
            path_to_fixture,
            region_code=controller.region_code(),
            has_fixture=True,
        )
        controller.raw_file_metadata_manager.mark_raw_file_as_discovered(
            path_to_fixture
        )
        controller.raw_file_metadata_manager.mark_raw_file_as_processed(path_to_fixture)

        controller.schedule_next_ingest_task(
            current_task_id=build_scheduler_task_id(
                controller.region, controller.ingest_instance
            ),
            just_finished_job=True,
        )
        self.assertEqual(
            0,
            controller.cloud_task_manager.get_raw_data_import_queue_info(
                controller.region
            ).size(),
        )
        run_task_queues_to_empty(controller)

    # TODO(#11424): Delete this test when we delete LegacyExtractAndMergeArgs.
    def test_serialize_gcsfs_ingest_args(self) -> None:
        now = datetime.datetime.now()

        str_now = datetime_to_serializable(now)
        now_converted = serializable_to_datetime(str_now)

        self.assertTrue(now, now_converted)

        args = LegacyExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(),
            file_path=GcsfsFilePath.from_absolute_path("foo/bar.csv"),
        )

        args_dict = attr_to_json_dict(args)  # type: ignore[arg-type]
        serialized = json.dumps(args_dict).encode()
        args_dict = json.loads(serialized)
        result_args = attr_from_json_dict(args_dict)
        self.assertEqual(args, result_args)

    def test_serialize_extract_and_merge_args(self) -> None:
        now = datetime.datetime.now()

        str_now = datetime_to_serializable(now)
        now_converted = serializable_to_datetime(str_now)

        self.assertTrue(now, now_converted)

        args = NewExtractAndMergeArgs(
            ingest_time=datetime.datetime.now(),
            ingest_view_name="myIngestviewName",
            ingest_instance=DirectIngestInstance.SECONDARY,
            batch_number=10,
            upper_bound_datetime_inclusive=datetime.datetime(2022, 4, 11, 1, 2, 3),
        )

        args_dict = attr_to_json_dict(args)  # type: ignore[arg-type]
        serialized = json.dumps(args_dict).encode()
        args_dict = json.loads(serialized)
        result_args = attr_from_json_dict(args_dict)
        self.assertEqual(args, result_args)
