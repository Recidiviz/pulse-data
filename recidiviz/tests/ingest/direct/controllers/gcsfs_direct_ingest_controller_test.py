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
"""Tests for GcsfsDirectIngestController."""
import abc
import datetime
import json
import os
import unittest
from collections import defaultdict
from typing import List, Optional, Set, Tuple, Type, TypeVar

import pytest
from freezegun import freeze_time
from mock import patch, Mock

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.serialization import (
    attr_to_json_dict,
    datetime_to_serializable,
    serializable_to_datetime,
    attr_from_json_dict,
)
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import (
    CsvGcsfsDirectIngestController,
)
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    SPLIT_FILE_STORAGE_SUBDIR,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import (
    GcsfsDirectIngestController,
)
from recidiviz.ingest.direct.controllers.direct_ingest_types import (
    IngestArgsType,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsIngestArgs,
    filename_parts_from_path,
    GcsfsDirectIngestFileType,
    GcsfsIngestViewExportArgs,
)
from recidiviz.ingest.direct.controllers.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestFileMetadataManager,
)
from recidiviz.ingest.direct.errors import DirectIngestError
from recidiviz.persistence.database.bq_refresh.bq_refresh_utils import (
    postgres_to_bq_lock_name_with_suffix,
)
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import fixture_util
from recidiviz.tests.ingest.direct.direct_ingest_util import (
    build_gcsfs_controller_for_tests,
    add_paths_with_tags_and_process,
    path_for_fixture_file,
    run_task_queues_to_empty,
    check_all_paths_processed,
    FakeDirectIngestRawFileImportManager,
    add_paths_with_tags,
    FakeDirectIngestRegionRawFileConfig,
)
from recidiviz.tests.ingest.direct.fake_direct_ingest_big_query_client import (
    FakeDirectIngestBigQueryClient,
)
from recidiviz.tests.ingest.direct.fake_synchronous_direct_ingest_cloud_task_manager import (
    FakeSynchronousDirectIngestCloudTaskManager,
)
from recidiviz.tests.utils.fake_region import (
    TEST_STATE_REGION,
    TEST_COUNTY_REGION,
    fake_region,
)
from recidiviz.tools.postgres import local_postgres_helpers

CsvGcsfsDirectIngestControllerT = TypeVar(
    "CsvGcsfsDirectIngestControllerT", bound=CsvGcsfsDirectIngestController
)


class BaseTestCsvGcsfsDirectIngestController(CsvGcsfsDirectIngestController):
    """Base class for test direct ingest controllers used in this file."""

    def __init__(
        self,
        region_name: str,
        system_level: SystemLevel,
        ingest_directory_path: Optional[str],
        storage_directory_path: Optional[str],
        max_delay_sec_between_files: int,
    ):
        super().__init__(
            region_name,
            system_level,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files,
        )
        self.local_paths: Set[str] = set()

    @classmethod
    @abc.abstractmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        pass

    def _get_contents_handle(
        self, args: GcsfsIngestArgs
    ) -> Optional[GcsfsFileContentsHandle]:
        handle = super()._get_contents_handle(args)
        if handle:
            self.local_paths.add(handle.local_file_path)
        return handle

    def has_temp_paths_in_disk(self) -> bool:
        for path in self.local_paths:
            if os.path.exists(path):
                return True
        return False


class CrashingStateTestGcsfsDirectIngestController(
    BaseTestCsvGcsfsDirectIngestController
):
    def __init__(
        self,
        ingest_directory_path: str,
        storage_directory_path: str,
        max_delay_sec_between_files: int = 0,
    ):
        super().__init__(
            TEST_STATE_REGION.region_code,
            SystemLevel.STATE,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files,
        )

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return ["tagC"]

    def _run_ingest_job(self, args: IngestArgsType) -> bool:
        raise Exception("insta-crash")


class StateTestGcsfsDirectIngestController(BaseTestCsvGcsfsDirectIngestController):
    def __init__(
        self,
        ingest_directory_path: str,
        storage_directory_path: str,
        max_delay_sec_between_files: int = 0,
    ):
        super().__init__(
            TEST_STATE_REGION.region_code,
            SystemLevel.STATE,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files,
        )

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return ["tagA", "tagB", "tagC"]


class SingleTagStateTestGcsfsDirectIngestController(
    BaseTestCsvGcsfsDirectIngestController
):
    def __init__(
        self,
        ingest_directory_path: str,
        storage_directory_path: str,
        max_delay_sec_between_files: int = 0,
    ):
        super().__init__(
            TEST_STATE_REGION.region_code,
            SystemLevel.STATE,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files,
        )

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return ["tagC"]


class CountyTestGcsfsDirectIngestController(BaseTestCsvGcsfsDirectIngestController):
    def __init__(
        self,
        ingest_directory_path: str,
        storage_directory_path: str,
        max_delay_sec_between_files: int = 0,
    ):
        super().__init__(
            TEST_COUNTY_REGION.region_code,
            SystemLevel.COUNTY,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files,
        )

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return ["tagA", "tagB"]


# TODO(#6197): These patches should be cleaned up after the new normalized direct ingest BQ views land.
@pytest.mark.uses_db
@patch("recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-staging"))
@patch(
    "recidiviz.ingest.direct.views.normalized_direct_ingest_big_query_view_types"
    ".get_region_raw_file_config",
    Mock(return_value=FakeDirectIngestRegionRawFileConfig("US_XX")),
)
@patch(
    "recidiviz.ingest.direct.views.unnormalized_direct_ingest_big_query_view_types"
    ".get_region_raw_file_config",
    Mock(return_value=FakeDirectIngestRegionRawFileConfig("US_XX")),
)
class TestGcsfsDirectIngestController(unittest.TestCase):
    """Tests for GcsfsDirectIngestController."""

    FIXTURE_PATH_PREFIX = "direct/controllers"

    TEST_INGEST_LAUNCHED_REGION = fake_region(
        region_code=TEST_STATE_REGION.region_code, environment="production"
    )
    TEST_INGEST_LAUNCHED_IN_STAGING_REGION = fake_region(
        region_code=TEST_STATE_REGION.region_code, environment="staging"
    )

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

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
        controller: GcsfsDirectIngestController,
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
                (tag, True) for tag in controller.get_file_tag_rank_list()
            ]

        if not controller.region.is_ingest_launched_in_env():
            expected_ingest_metadata_tags_with_is_processed = []
        elif expected_ingest_metadata_tags_with_is_processed is None:
            expected_ingest_metadata_tags_with_is_processed = [
                (tag, True) for tag in controller.get_file_tag_rank_list()
            ]

        file_metadata_manager = controller.file_metadata_manager
        if not isinstance(
            file_metadata_manager, PostgresDirectIngestFileMetadataManager
        ):
            self.fail(f"Unexpected file_metadata_manager type {file_metadata_manager}")

        session = SessionFactory.for_database(self.operations_database_key)
        try:
            raw_file_results = session.query(schema.DirectIngestRawFileMetadata).all()

            raw_file_metadata_list = [
                # pylint:disable=protected-access
                file_metadata_manager._raw_file_schema_metadata_as_entity(metadata)
                for metadata in raw_file_results
            ]

            ingest_file_results = session.query(
                schema.DirectIngestIngestFileMetadata
            ).all()

            ingest_file_metadata_list = [
                # pylint:disable=protected-access
                file_metadata_manager._ingest_file_schema_metadata_as_entity(metadata)
                for metadata in ingest_file_results
            ]

        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        actual_raw_metadata_tags_with_is_processed = [
            (metadata.file_tag, bool(metadata.processed_time))
            for metadata in raw_file_metadata_list
        ]

        if not controller.region.is_ingest_launched_in_env():
            self.assertEqual([], actual_raw_metadata_tags_with_is_processed)

        self.assertEqual(
            sorted(expected_raw_metadata_tags_with_is_processed),
            sorted(actual_raw_metadata_tags_with_is_processed),
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
        )

        # TODO(#3020): Update this to better test that metadata for split files gets properly registered

    def run_async_file_order_test_for_controller_cls(
        self, controller_cls: Type[CsvGcsfsDirectIngestControllerT]
    ) -> BaseTestCsvGcsfsDirectIngestController:
        """Writes all expected files to the mock fs, then kicks the controller
        and ensures that all jobs are run to completion in the proper order."""

        controller = build_gcsfs_controller_for_tests(
            controller_cls, self.FIXTURE_PATH_PREFIX, run_async=True
        )

        file_tags = list(reversed(sorted(controller.get_file_tag_rank_list())))

        add_paths_with_tags_and_process(self, controller, file_tags)

        if not isinstance(controller, BaseTestCsvGcsfsDirectIngestController):
            self.fail(
                "Controller is not of type BaseTestCsvGcsfsDirectIngestController."
            )
        self.assertFalse(controller.has_temp_paths_in_disk())

        self.validate_file_metadata(controller)

        return controller

    def check_tags(
        self, controller: GcsfsDirectIngestController, tags: List[str]
    ) -> None:
        self.assertIsInstance(
            controller.ingest_view_export_manager.big_query_client,
            FakeDirectIngestBigQueryClient,
        )
        if not isinstance(
            controller.ingest_view_export_manager.big_query_client,
            FakeDirectIngestBigQueryClient,
        ):
            self.fail("Expected FakeDirectIngestBigQueryClient but did not find one.")
        self.assertCountEqual(
            tags,
            controller.ingest_view_export_manager.big_query_client.exported_file_tags,
        )

    def get_fake_task_manager(
        self, controller: GcsfsDirectIngestController
    ) -> FakeSynchronousDirectIngestCloudTaskManager:
        if not isinstance(
            controller.cloud_task_manager, FakeSynchronousDirectIngestCloudTaskManager
        ):
            self.fail("Expected FakeSynchronousDirectIngestCouldManager")
        return controller.cloud_task_manager

    def check_imported_path_count(
        self, controller: GcsfsDirectIngestController, expected_count: int
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

    def add_and_process_through_ingest_view_export(
        self,
        controller: GcsfsDirectIngestController,
        file_path: GcsfsFilePath,
        clear_scheduler_queue_after: bool = False,
    ) -> None:

        task_manager = self.get_fake_task_manager(controller)
        fixture_util.add_direct_ingest_path(controller.fs.gcs_file_system, file_path)

        # Tasks for handling unnormalized file_path
        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()
        # file_path raw data import
        task_manager.test_run_next_bq_import_export_task()
        task_manager.test_pop_finished_bq_import_export_task()
        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()
        # file_path ingest view export
        task_manager.test_run_next_bq_import_export_task()
        task_manager.test_pop_finished_bq_import_export_task()

        if clear_scheduler_queue_after:
            while task_manager.scheduler_tasks:
                task_manager.test_run_next_scheduler_task()
                task_manager.test_pop_finished_scheduler_task()

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_state_runs_files_in_order(self) -> None:
        controller = self.run_async_file_order_test_for_controller_cls(
            StateTestGcsfsDirectIngestController
        )

        self.check_imported_path_count(controller, 3)

        self.check_tags(controller, controller.get_file_tag_rank_list())

    @patch(
        "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
        Mock(return_value=FakeGCSFileSystem()),
    )
    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_state_runs_files_in_order_locking(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
        )
        controller.lock_manager.lock(postgres_to_bq_lock_name_with_suffix("state"))
        file_tags = list(reversed(sorted(controller.get_file_tag_rank_list())))
        add_paths_with_tags(controller, file_tags)
        run_task_queues_to_empty(controller)

        if not isinstance(controller, BaseTestCsvGcsfsDirectIngestController):
            self.fail(
                "Controller is not of type BaseTestCsvGcsfsDirectIngestController."
            )

        self.assertFalse(controller.has_temp_paths_in_disk())
        self.validate_file_metadata(
            controller,
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagA", False),
                ("tagB", False),
                ("tagC", False),
            ],
        )
        controller.lock_manager.unlock(postgres_to_bq_lock_name_with_suffix("state"))
        controller.kick_scheduler(just_finished_job=False)
        run_task_queues_to_empty(controller)

        check_all_paths_processed(
            self, controller, ["tagA", "tagB", "tagC"], unexpected_tags=[]
        )
        self.check_imported_path_count(controller, 3)
        self.check_tags(controller, controller.get_file_tag_rank_list())
        self.validate_file_metadata(
            controller,
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagA", True),
                ("tagB", True),
                ("tagC", True),
            ],
        )

    @patch(
        "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
        Mock(return_value=FakeGCSFileSystem()),
    )
    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_state_runs_files_in_order_locking_region(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
        )
        controller.lock_manager.lock(controller.ingest_process_lock_for_region())
        file_tags = list(reversed(sorted(controller.get_file_tag_rank_list())))
        add_paths_with_tags(controller, file_tags)
        run_task_queues_to_empty(controller)

        if not isinstance(controller, BaseTestCsvGcsfsDirectIngestController):
            self.fail(
                "Controller is not of type BaseTestCsvGcsfsDirectIngestController."
            )

        self.assertFalse(controller.has_temp_paths_in_disk())
        self.validate_file_metadata(
            controller,
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagA", False),
                ("tagB", False),
                ("tagC", False),
            ],
        )
        controller.lock_manager.unlock(controller.ingest_process_lock_for_region())
        controller.kick_scheduler(just_finished_job=False)
        run_task_queues_to_empty(controller)

        check_all_paths_processed(
            self, controller, ["tagA", "tagB", "tagC"], unexpected_tags=[]
        )
        self.check_imported_path_count(controller, 3)
        self.check_tags(controller, controller.get_file_tag_rank_list())
        self.validate_file_metadata(
            controller,
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagA", True),
                ("tagB", True),
                ("tagC", True),
            ],
        )

    @patch(
        "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
        Mock(return_value=FakeGCSFileSystem()),
    )
    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_crash_releases_lock(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            CrashingStateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
        )
        file_tags = list(reversed(sorted(controller.get_file_tag_rank_list())))
        add_paths_with_tags(controller, file_tags)
        with self.assertRaises(Exception):
            run_task_queues_to_empty(controller)

        # Assert that there are no leftover locks
        self.assertTrue(controller.lock_manager.no_active_locks_with_prefix(""))

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_COUNTY_REGION))
    def test_county_runs_files_in_order(self) -> None:
        self.run_async_file_order_test_for_controller_cls(
            CountyTestGcsfsDirectIngestController
        )

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_state_single_split_tag(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            SingleTagStateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
        )

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        file_tags: List[str] = ["tagC"]
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
        self.assertEqual(2, len(processed_split_file_paths["tagC"]))

        found_suffixes = {
            filename_parts_from_path(p).filename_suffix
            for p in processed_split_file_paths["tagC"]
            if isinstance(p, GcsfsFilePath)
        }
        self.assertEqual(
            found_suffixes, {"00001_file_split_size1", "00002_file_split_size1"}
        )

        self.check_imported_path_count(controller, 1)

        expected_raw_metadata_tags_with_is_processed = [("tagC", True)]
        expected_ingest_metadata_tags_with_is_processed = [
            ("tagC", True),
            ("tagC", True),
            ("tagC", True),
        ]
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed,
            expected_ingest_metadata_tags_with_is_processed=expected_ingest_metadata_tags_with_is_processed,
        )

        self.check_tags(controller, controller.get_file_tag_rank_list())

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_state_unexpected_tag(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
        )

        file_tags = ["tagA", "Unexpected_Tag", "tagB", "tagC"]
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
            if isinstance(path, GcsfsFilePath) and controller.fs.is_split_file(path)
        }
        self.assertFalse(split_paths)

        self.check_imported_path_count(controller, 3)

        expected_raw_metadata_tags_with_is_processed = [
            ("tagA", True),
            ("tagB", True),
            ("tagC", True),
            ("Unexpected_Tag", False),
        ]
        expected_ingest_metadata_tags_with_is_processed = [
            ("tagA", True),
            ("tagB", True),
            ("tagC", True),
        ]
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed,
            expected_ingest_metadata_tags_with_is_processed=expected_ingest_metadata_tags_with_is_processed,
        )

        self.check_tags(controller, controller.get_file_tag_rank_list())

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_state_tag_we_import_but_do_not_ingest(
        self,
    ) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
        )

        file_tags = ["tagA", "tagWeDoNotIngest", "tagB", "tagC"]

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
            if isinstance(path, GcsfsFilePath) and controller.fs.is_split_file(path)
        }
        self.assertFalse(split_paths)

        self.check_imported_path_count(controller, 4)

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                (tag, True) for tag in file_tags
            ],
            expected_ingest_metadata_tags_with_is_processed=[
                (tag, True) for tag in ["tagA", "tagB", "tagC"]
            ],
        )

        self.check_tags(controller, ["tagA", "tagB", "tagC"])

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_do_not_queue_same_job_twice(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
        )

        task_manager = self.get_fake_task_manager(controller)

        file_path = path_for_fixture_file(
            controller,
            "tagA.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        file_path2 = path_for_fixture_file(
            controller,
            "tagB.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        self.add_and_process_through_ingest_view_export(
            controller, file_path, clear_scheduler_queue_after=True
        )

        # Single process job request should be waiting for the first file
        self.assertEqual(
            task_manager.get_process_job_queue_info(
                self.TEST_INGEST_LAUNCHED_REGION
            ).size(),
            1,
        )

        self.add_and_process_through_ingest_view_export(controller, file_path2)

        task_manager.test_run_next_process_job_task()
        task_manager.test_pop_finished_process_job_task()

        self.assertEqual(
            task_manager.get_process_job_queue_info(
                self.TEST_INGEST_LAUNCHED_REGION
            ).size(),
            0,
        )

        # This is the task that got queued by after we normalized the path,
        # which will schedule the next process_job.
        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        # These are the tasks that got queued by finishing an ingest view export
        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        self.assertEqual(
            0, task_manager.get_scheduler_queue_info(controller.region).size()
        )
        self.assertEqual(
            1, task_manager.get_process_job_queue_info(controller.region).size()
        )

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                ("tagA", True),
                ("tagB", True),
            ],
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagA", True),
                ("tagB", False),  # This view has not been ingested yet
            ],
        )

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_next_schedule_runs_before_process_job_clears(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
        )
        task_manager = self.get_fake_task_manager(controller)

        file_path = path_for_fixture_file(
            controller,
            "tagA.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        self.add_and_process_through_ingest_view_export(
            controller, file_path, clear_scheduler_queue_after=True
        )

        self.assertEqual(
            1, task_manager.get_process_job_queue_info(controller.region).size()
        )

        file_path2 = path_for_fixture_file(
            controller,
            "tagB.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        self.add_and_process_through_ingest_view_export(controller, file_path2)

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
        task_manager.test_run_next_process_job_task()

        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_process_job_task()
        task_manager.test_pop_finished_scheduler_task()

        # We should have still queued a process job, even though the last
        # one hadn't run when schedule executes
        task_manager.test_run_next_process_job_task()
        task_manager.test_pop_finished_process_job_task()

        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        self.assertEqual(
            0, task_manager.get_scheduler_queue_info(controller.region).size()
        )
        self.assertEqual(
            0, task_manager.get_process_job_queue_info(controller.region).size()
        )
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                ("tagA", True),
                ("tagB", True),
            ],
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagA", True),
                ("tagB", True),
            ],
        )

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_process_job_task_run_twice(self) -> None:
        # Cloud Tasks has an at-least once guarantee - make sure rerunning a task in series does not crash
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            self.fail("Expected FakeGCSFileSystem")

        task_manager = self.get_fake_task_manager(controller)

        dt = datetime.datetime.now()
        file_path = path_for_fixture_file(
            controller,
            "tagA.csv",
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

        fixture_util.add_direct_ingest_path(controller.fs.gcs_file_system, file_path)
        parts = filename_parts_from_path(file_path)
        metadata = controller.file_metadata_manager.register_ingest_file_export_job(
            GcsfsIngestViewExportArgs(
                ingest_view_name=parts.file_tag,
                upper_bound_datetime_prev=None,
                upper_bound_datetime_to_export=dt,
            )
        )
        controller.file_metadata_manager.register_ingest_view_export_file_name(
            metadata, file_path
        )

        # At this point we have a series of tasks handling / renaming /
        # splitting the new files, then scheduling the next job. They run in
        # quick succession.
        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        # Process job tasks starts as a result of the first schedule.
        task_manager.test_run_next_process_job_task()

        task = task_manager.test_pop_finished_process_job_task()

        task_manager.create_direct_ingest_process_job_task(*task)  # type: ignore[arg-type]

        # Now run the repeated task
        task_manager.test_run_next_process_job_task()
        task_manager.test_pop_finished_process_job_task()

        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        self.assertEqual(
            0, task_manager.get_scheduler_queue_info(controller.region).size()
        )
        self.assertEqual(
            0, task_manager.get_process_job_queue_info(controller.region).size()
        )
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[],
            expected_ingest_metadata_tags_with_is_processed=[("tagA", True)],
        )

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_process_already_normalized_paths(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
        )

        file_tags = list(sorted(controller.get_file_tag_rank_list()))

        add_paths_with_tags_and_process(
            self,
            controller,
            file_tags,
            pre_normalized_file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        self.validate_file_metadata(controller)

    def _path_in_storage_dir(
        self, path: GcsfsFilePath, controller: GcsfsDirectIngestController
    ) -> bool:
        return path.abs_path().startswith(controller.storage_directory_path.abs_path())

    def _path_in_split_file_storage_subdir(
        self, path: GcsfsFilePath, controller: GcsfsDirectIngestController
    ) -> bool:
        if self._path_in_storage_dir(path, controller):
            directory, _ = os.path.split(path.abs_path())
            if SPLIT_FILE_STORAGE_SUBDIR in directory:
                return True
        return False

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_process_ingest_view_file_that_needs_splitting(
        self,
    ) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
        )

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        file_tags = list(sorted(controller.get_file_tag_rank_list()))

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
        self.assertEqual(2, len(processed_split_file_paths["tagC"]))

        found_suffixes = {
            filename_parts_from_path(p).filename_suffix
            for p in processed_split_file_paths["tagC"]
            if isinstance(p, GcsfsFilePath)
        }
        self.assertEqual(
            found_suffixes, {"00001_file_split_size1", "00002_file_split_size1"}
        )
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                (tag, True) for tag in controller.get_file_tag_rank_list()
            ],
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagA", True),
                ("tagB", True),
                ("tagC", True),
                ("tagC", True),
                ("tagC", True),
            ],
        )

        self.check_imported_path_count(controller, 3)

        self.check_tags(controller, controller.get_file_tag_rank_list())

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    @patch.object(
        GcsfsDirectIngestController,
        "_split_file_if_necessary",
        Mock(side_effect=ValueError("Splitting crashed")),
    )
    def test_failing_to_process_a_file_that_needs_splitting_no_loop(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
        )

        task_manager = self.get_fake_task_manager(controller)

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        # This file exceeds the split limit, but since we have mocked
        # _split_files_if_necessary to do nothing, it won't get split.
        file_path = path_for_fixture_file(
            controller,
            "tagC.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        self.add_and_process_through_ingest_view_export(controller, file_path)

        while task_manager.scheduler_tasks:
            with self.assertRaises(ValueError) as e:
                task_manager.test_run_next_scheduler_task()
            self.assertEqual(str(e.exception), "Splitting crashed")
            task_manager.scheduler_tasks.pop(0)

        controller.kick_scheduler(just_finished_job=False)

        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        task_manager.test_run_next_process_job_task()
        task_manager.test_pop_finished_process_job_task()

        # The process job task, which will try to process a file that is too
        # big, will not schedule another job for the same file (which would
        # just get us in a loop).
        self.assertEqual(
            0, task_manager.get_scheduler_queue_info(controller.region).size()
        )
        self.assertEqual(
            0, task_manager.get_process_job_queue_info(controller.region).size()
        )
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[("tagC", True)],
            expected_ingest_metadata_tags_with_is_processed=[("tagC", False)],
        )

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_move_files_from_previous_days_to_storage(
        self,
    ) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
        )

        prev_date_datetime = datetime.datetime.fromisoformat("2019-09-15")
        file_path_from_prev_day = path_for_fixture_file(
            controller,
            "tagB.csv",
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
            controller.fs.gcs_file_system, processed_file_from_prev_day
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system, unexpected_file_path_from_prev_day
        )

        file_tags = list(sorted(controller.get_file_tag_rank_list()))

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
        self.assertTrue("tagB" in paths_from_prev_date[0].abs_path())
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

        self.check_tags(controller, controller.get_file_tag_rank_list())

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_move_files_from_previous_days_to_storage_incomplete_current_day(
        self,
    ) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
            max_delay_sec_between_files=1,
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        current_date_datetime = datetime.datetime.utcnow()
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
                "tagB.csv",
                should_normalize=True,
                file_type=GcsfsDirectIngestFileType.RAW_DATA,
            )

            fixture_util.add_direct_ingest_path(
                controller.fs.gcs_file_system, unexpected_file_path_from_prev_day
            )
            fixture_util.add_direct_ingest_path(
                controller.fs.gcs_file_system, processed_file_from_prev_day
            )

        run_task_queues_to_empty(controller)

        file_path_from_current_day = path_for_fixture_file(
            controller,
            "tagA.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system, file_path_from_current_day
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
            processed_path_str.startswith(controller.ingest_directory_path.abs_path())
        )
        self.assertTrue("tagA" in processed_path_str)
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                ("Unexpected_Tag", False),
                ("tagA", True),
                ("tagB", True),
            ],
            expected_ingest_metadata_tags_with_is_processed=[
                ("tagA", True),
                ("tagB", True),
            ],
        )

        self.check_tags(controller, ["tagA", "tagB"])

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_cloud_function_fails_on_new_file(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
        )

        task_manager = self.get_fake_task_manager(controller)

        file_path = path_for_fixture_file(
            controller,
            "tagA.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        file_path2 = path_for_fixture_file(
            controller,
            "tagB.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        file_path3 = path_for_fixture_file(
            controller,
            "tagC.csv",
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
            controller.fs.gcs_file_system, file_path, fail_handle_file_call=True
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system, file_path2, fail_handle_file_call=True
        )

        self.assertEqual(
            0, task_manager.get_scheduler_queue_info(controller.region).size()
        )
        self.assertEqual(
            0, task_manager.get_process_job_queue_info(controller.region).size()
        )

        # Later file that succeeds will trigger proper upload of all files
        fixture_util.add_direct_ingest_path(controller.fs.gcs_file_system, file_path3)

        run_task_queues_to_empty(controller)
        check_all_paths_processed(
            self, controller, ["tagA", "tagB", "tagC"], unexpected_tags=[]
        )
        self.validate_file_metadata(controller)

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_cloud_function_fails_on_new_file_rename_later_with_cron(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
        )

        task_manager = self.get_fake_task_manager(controller)

        file_path = path_for_fixture_file(
            controller,
            "tagA.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        file_path2 = path_for_fixture_file(
            controller,
            "tagB.csv",
            should_normalize=False,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        file_path3 = path_for_fixture_file(
            controller,
            "tagC.csv",
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
            controller.fs.gcs_file_system, file_path, fail_handle_file_call=True
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system, file_path2, fail_handle_file_call=True
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system, file_path3, fail_handle_file_call=True
        )

        self.assertEqual(
            0, task_manager.get_scheduler_queue_info(controller.region).size()
        )
        self.assertEqual(
            0, task_manager.get_process_job_queue_info(controller.region).size()
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
            controller.region, can_start_ingest=True
        )

        run_task_queues_to_empty(controller)

        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            self.assertTrue(controller.fs.is_normalized_file_path(path))

        self.validate_file_metadata(controller)

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_can_start_ingest_is_false_launched_region_throws(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
            can_start_ingest=False,
        )

        file_tags = list(reversed(sorted(controller.get_file_tag_rank_list())))

        add_paths_with_tags(controller, file_tags)
        with self.assertRaises(ValueError) as e:
            run_task_queues_to_empty(controller)

        self.assertEqual(
            str(e.exception),
            "The can_start_ingest flag should only be used for regions where ingest is not yet"
            " launched in a particular environment. If we want to be able to selectively pause"
            " ingest processing for a state, we will first have to build a config that is"
            " respected by both the /ensure_all_file_paths_normalized endpoint and any cloud"
            " functions that trigger ingest.",
        )

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_IN_STAGING_REGION),
    )
    def test_can_start_ingest_is_false_does_not_start_ingest(
        self,
    ) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
            can_start_ingest=False,
        )

        file_tags = list(reversed(sorted(controller.get_file_tag_rank_list())))

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

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_IN_STAGING_REGION),
    )
    def test_unlaunched_region_raises_if_can_start_ingest_true(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
        )

        add_paths_with_tags(controller, ["tagA"])
        with self.assertRaises(DirectIngestError) as e:
            run_task_queues_to_empty(controller)

        self.assertEqual(
            str(e.exception), "Bad environment [production] for region [us_xx]."
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

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_launched_region_raises_if_can_start_ingest_false(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True,
            can_start_ingest=False,
        )

        add_paths_with_tags(controller, ["tagA"])
        with self.assertRaises(ValueError) as e:
            run_task_queues_to_empty(controller)

        self.assertTrue(
            str(e.exception).startswith(
                "The can_start_ingest flag should only be used for regions where ingest is not yet launched"
            )
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

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[],
            expected_ingest_metadata_tags_with_is_processed=[],
        )

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_processing_continues_if_there_are_subfolders_in_ingest_dir(self) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
        )

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        subdir_path_name = "subdir/"
        subdir_path = GcsfsDirectoryPath.from_dir_and_subdir(
            controller.ingest_directory_path, subdir_path_name
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system, subdir_path, has_fixture=False
        )
        path_names = [
            ("subdir/Unexpected_Tag.csv", False),
            ("tagA.csv", True),
            ("tagB.csv", True),
            ("tagC.csv", True),
            ("subdir/tagC_2.csv", False),
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
                has_fixture=has_fixture,
            )

        run_task_queues_to_empty(controller)

        dir_paths_found = []
        storage_file_paths = []
        ingest_file_paths = []

        for path in controller.fs.gcs_file_system.all_paths:
            if isinstance(path, GcsfsDirectoryPath):
                dir_paths_found.append(path)
                continue

            if path.abs_path().startswith(controller.storage_directory_path.abs_path()):
                storage_file_paths.append(path)
            else:
                self.assertTrue(
                    path.abs_path().startswith(
                        controller.ingest_directory_path.abs_path()
                    )
                )
                ingest_file_paths.append(path)

        self.assertEqual(1, len(dir_paths_found))
        self.assertEqual(subdir_path, dir_paths_found[0])

        # Three raw data files and three ingest view files
        self.assertEqual(6, len(storage_file_paths))
        storage_tags = {
            filename_parts_from_path(path).file_tag for path in storage_file_paths
        }
        self.assertEqual({"tagA", "tagB", "tagC"}, storage_tags)

        for path in storage_file_paths:
            self.assertTrue(controller.fs.is_normalized_file_path(path))
            self.assertTrue(controller.fs.is_processed_file(path))

        self.assertEqual(2, len(ingest_file_paths))
        ingest_tags = {
            filename_parts_from_path(path).file_tag for path in ingest_file_paths
        }
        self.assertEqual({"tagC", "Unexpected_Tag"}, ingest_tags)

        for path in ingest_file_paths:
            self.assertTrue(controller.fs.is_normalized_file_path(path))
            self.assertTrue(controller.fs.is_seen_unprocessed_file(path))
            self.assertEqual(subdir_path, GcsfsDirectoryPath.from_file_path(path))

        self.validate_file_metadata(controller)

    @patch(
        "recidiviz.utils.regions.get_region",
        Mock(return_value=TEST_INGEST_LAUNCHED_REGION),
    )
    def test_do_not_schedule_raw_data_import_task_if_already_processed_yet_in_gcs(
        self,
    ) -> None:
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
        )
        path_to_fixture = path_for_fixture_file(
            controller,
            "tagA.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )
        fixture_util.add_direct_ingest_path(
            controller.fs.gcs_file_system, path_to_fixture, has_fixture=True
        )
        controller.file_metadata_manager.mark_file_as_discovered(path_to_fixture)
        controller.file_metadata_manager.mark_file_as_processed(path_to_fixture)

        controller.schedule_next_ingest_job_or_wait_if_necessary(just_finished_job=True)
        for task_name in controller.cloud_task_manager.get_bq_import_export_queue_info(
            controller.region
        ).task_names:
            self.assertNotRegex(task_name, "raw")
        run_task_queues_to_empty(controller)

    def test_serialize_gcsfs_ingest_args(self) -> None:
        now = datetime.datetime.now()

        str_now = datetime_to_serializable(now)
        now_converted = serializable_to_datetime(str_now)

        self.assertTrue(now, now_converted)

        args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=GcsfsFilePath.from_absolute_path("foo/bar.csv"),
        )

        args_dict = attr_to_json_dict(args)  # type: ignore[arg-type]
        serialized = json.dumps(args_dict).encode()
        args_dict = json.loads(serialized)
        result_args = attr_from_json_dict(args_dict)
        self.assertEqual(args, result_args)
