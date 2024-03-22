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
"""Tests for IngestRawFileImportController."""
import abc
import os
import time
import unittest
from typing import List, Optional, Tuple

import pytest
from mock import Mock, patch
from more_itertools import one

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockAlreadyExists,
    GCSPseudoLockFailedUnlock,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.controllers.ingest_raw_file_import_controller import (
    IngestRawFileImportController,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    build_scheduler_task_id,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import GcsfsRawDataBQImportArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import DirectIngestError
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.tests.ingest.direct import fixture_util
from recidiviz.tests.ingest.direct.direct_ingest_test_util import (
    run_task_queues_to_empty,
)
from recidiviz.tests.ingest.direct.fakes.fake_ingest_raw_file_import_controller import (
    FakeDirectIngestRawFileImportManager,
    FakeIngestRawFileImportController,
    build_fake_ingest_raw_file_import_controller,
)
from recidiviz.tests.ingest.direct.fakes.fake_synchronous_direct_ingest_cloud_task_manager import (
    FakeSynchronousDirectIngestCloudTaskManager,
)
from recidiviz.tests.utils.monitoring_test_utils import OTLMock
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.utils.types import assert_type

DEFAULT_INPUT_RAW_FILE_TAGS = [
    "tagMoreBasicData",
    "tagHeadersNoContents",
    "tagBasicData",
]


def check_all_paths_processed(
    test_case: unittest.TestCase,
    controller: IngestRawFileImportController,
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

        file_tag = filename_parts_from_path(path).file_tag

        if file_tag not in unexpected_tags:
            # Test all expected files have been moved to storage
            test_case.assertTrue(
                path.abs_path().startswith(
                    controller.raw_data_storage_directory_path.abs_path()
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
    controller: IngestRawFileImportController,
    file_tags: List[str],
    unexpected_tags: Optional[List[str]] = None,
) -> None:
    if unexpected_tags is None:
        unexpected_tags = []

    run_task_queues_to_empty(controller)
    check_all_paths_processed(test_case, controller, file_tags, unexpected_tags)


def add_paths_with_tags_and_process(
    test_case: unittest.TestCase,
    controller: IngestRawFileImportController,
    file_tags: List[str],
    should_normalize: bool = False,
    unexpected_tags: Optional[List[str]] = None,
) -> None:
    """Runs a test that queues files for all the provided file tags, waits
    for the controller to finish processing everything, then makes sure that
    all files not in |unexpected_tags| have been moved to storage.
    """
    add_paths_with_tags(controller, file_tags, should_normalize=should_normalize)
    process_task_queues(test_case, controller, file_tags, unexpected_tags)


def add_paths_with_tags(
    controller: IngestRawFileImportController,
    file_tags: List[str],
    should_normalize: bool = False,
    file_extension: str = "csv",
) -> None:
    if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
        raise ValueError(
            f"Controller fs must have type "
            f"FakeGCSFileSystem. Found instead "
            f"type [{type(controller.fs.gcs_file_system)}]"
        )

    for file_tag in file_tags:
        fixture_util.add_direct_ingest_path(
            fs=controller.fs.gcs_file_system,
            bucket_path=controller.raw_data_bucket_path,
            filename=f"{file_tag}.{file_extension}",
            should_normalize=should_normalize,
            region_code=controller.region_code(),
        )
        time.sleep(0.05)


@pytest.mark.uses_db
class IngestRawFileImportControllerTest(unittest.TestCase):
    """Base class for all ingest controller tests."""

    # We set __test__ to False to tell `pytest` not to collect this class for running tests
    # (as successful runs rely on the implementation of an abstract method).
    # In sub-classes, __test__ should be re-set to True.
    __test__ = False

    # Stores the location of the postgres DB for this test run
    temp_db_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.otl_mock = OTLMock()
        self.otl_mock.set_up()

        self.operations_database_key = SQLAlchemyDatabaseKey.for_schema(
            SchemaType.OPERATIONS
        )

        local_persistence_helpers.use_on_disk_postgresql_database(
            self.operations_database_key
        )

        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.metadata_patcher.start().return_value = "recidiviz-staging"

    def tearDown(self) -> None:
        self.otl_mock.tear_down()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.operations_database_key
        )
        self.metadata_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    @property
    @abc.abstractmethod
    def ingest_instance(self) -> DirectIngestInstance:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def rerun_just_started_statuses(self) -> List[DirectIngestStatus]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def initial_instance_statuses(self) -> List[DirectIngestStatus]:
        raise NotImplementedError

    def get_fake_task_manager(
        self, controller: IngestRawFileImportController
    ) -> FakeSynchronousDirectIngestCloudTaskManager:
        if not isinstance(
            controller.cloud_task_manager, FakeSynchronousDirectIngestCloudTaskManager
        ):
            self.fail(
                f"Expected FakeSynchronousDirectIngestCouldManager, got: {controller.cloud_task_manager}"
            )
        return controller.cloud_task_manager

    def validate_file_metadata(
        self,
        controller: IngestRawFileImportController,
        expected_raw_metadata_tags_with_is_processed: Optional[
            List[Tuple[str, bool]]
        ] = None,
    ) -> None:
        """Validates that the file metadata was recorded as expected."""

        if not controller.region.is_ingest_launched_in_env():
            expected_raw_metadata_tags_with_is_processed = []
        elif expected_raw_metadata_tags_with_is_processed is None:
            expected_raw_metadata_tags_with_is_processed = [
                (tag, True) for tag in DEFAULT_INPUT_RAW_FILE_TAGS
            ]
        raw_file_metadata_manager = controller.raw_file_metadata_manager
        if not isinstance(
            raw_file_metadata_manager, DirectIngestRawFileMetadataManager
        ):
            self.fail(
                f"Unexpected raw_file_metadata_manager type {raw_file_metadata_manager}"
            )

        with SessionFactory.using_database(
            self.operations_database_key, autocommit=False
        ) as session:
            raw_file_results = session.query(schema.DirectIngestRawFileMetadata).filter(
                schema.DirectIngestRawFileMetadata.raw_data_instance
                == controller.ingest_instance.value
            )

            raw_file_metadata_list = [
                convert_schema_object_to_entity(metadata, DirectIngestRawFileMetadata)
                for metadata in raw_file_results
            ]

        actual_raw_metadata_tags_with_is_processed = [
            (metadata.file_tag, bool(metadata.file_processed_time))
            for metadata in raw_file_metadata_list
        ]

        if not controller.region.is_ingest_launched_in_env():
            self.assertEqual([], actual_raw_metadata_tags_with_is_processed)

        self.assertEqual(
            sorted(expected_raw_metadata_tags_with_is_processed),
            sorted(actual_raw_metadata_tags_with_is_processed),
            "Raw file metadata does not match expected",
        )

    def run_no_data_test(self) -> FakeIngestRawFileImportController:
        """Creates a controller and kick scheduler when there is no work."""
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.initial_instance_statuses,
            run_async=True,
        )

        controller.kick_scheduler()
        run_task_queues_to_empty(controller)

        if not isinstance(controller, FakeIngestRawFileImportController):
            self.fail(
                "Controller is not of type IngestRawFileImportControllerForTests."
            )
        self.assertFalse(controller.has_temp_paths_in_disk())

        self.validate_file_metadata(
            controller, expected_raw_metadata_tags_with_is_processed=[]
        )
        return controller

    def run_async_file_order_test_for_controller_cls(
        self,
    ) -> FakeIngestRawFileImportController:
        """Writes all expected files to the mock fs, then kicks the controller
        and ensures that all jobs are run to completion in the proper order."""

        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
        )

        file_tags = list(reversed(sorted(DEFAULT_INPUT_RAW_FILE_TAGS)))

        add_paths_with_tags_and_process(self, controller, file_tags)

        if not isinstance(controller, FakeIngestRawFileImportController):
            self.fail(
                "Controller is not of type IngestRawFileImportControllerForTests."
            )
        self.assertFalse(controller.has_temp_paths_in_disk())

        self.validate_file_metadata(controller)

        return controller

    def check_imported_path_count(
        self, controller: IngestRawFileImportController, expected_count: int
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

    def test_state_runs_files_in_order(self) -> None:
        controller = self.run_async_file_order_test_for_controller_cls()

        self.check_imported_path_count(controller, 3)

    def test_unzip_zip_files_single_file_inside(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
        )

        expected_file_tags = [
            "tagBasicData",
            "tagHeadersNoContents",
            "tagMoreBasicData",
        ]

        add_paths_with_tags(
            controller, ["tagBasicData"], should_normalize=False, file_extension="zip"
        )
        add_paths_with_tags(
            controller,
            ["tagHeadersNoContents", "tagMoreBasicData"],
            should_normalize=False,
        )
        run_task_queues_to_empty(controller)

        deprecated_files = controller.fs.ls_with_blob_prefix(
            controller.raw_data_storage_directory_path.bucket_path.bucket_name,
            os.path.join(
                controller.raw_data_storage_directory_path.relative_path, "deprecated"
            ),
        )
        deprecated_file = assert_type(one(deprecated_files), GcsfsFilePath)
        self.assertTrue("tagBasicData.zip" in deprecated_file.file_name)

        # Delete deprecated file so it doesn't get in the way of other checking in
        # check_all_paths_processed.
        controller.fs.delete(deprecated_file)

        # This confirms that all three expected files were discovered and processed
        # fully, inluding the file inside of the zip file.
        check_all_paths_processed(
            self, controller, expected_file_tags, unexpected_tags=[]
        )

        if not isinstance(controller, FakeIngestRawFileImportController):
            self.fail(
                "Controller is not of type IngestRawFileImportControllerForTests."
            )
        self.assertFalse(controller.has_temp_paths_in_disk())

        self.validate_file_metadata(controller)

        self.check_imported_path_count(controller, 3)

    def test_unzip_zip_files_multiple_files_inside(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
        )

        expected_file_tags = [
            "tagBasicData",
            "tagHeadersNoContents",
            "tagMoreBasicData",
        ]

        # This zip file has two CSV files inside: tagBasicData.csv and
        # tagHeadersNoContents.csv.
        add_paths_with_tags(
            controller,
            ["tagBasicDatatagHeadersNoContents"],
            should_normalize=False,
            file_extension="zip",
        )
        add_paths_with_tags(
            controller,
            ["tagMoreBasicData"],
            should_normalize=False,
        )
        run_task_queues_to_empty(controller)

        deprecated_files = controller.fs.ls_with_blob_prefix(
            controller.raw_data_storage_directory_path.bucket_path.bucket_name,
            os.path.join(
                controller.raw_data_storage_directory_path.relative_path, "deprecated"
            ),
        )
        deprecated_file = assert_type(one(deprecated_files), GcsfsFilePath)
        self.assertTrue(
            "tagBasicDatatagHeadersNoContents.zip" in deprecated_file.file_name
        )

        # Delete deprecated file so it doesn't get in the way of other checking in
        # check_all_paths_processed.
        controller.fs.delete(deprecated_file)

        # This confirms that all three expected files were discovered and processed
        # fully, inluding the file inside of the zip file.
        check_all_paths_processed(
            self, controller, expected_file_tags, unexpected_tags=[]
        )

        if not isinstance(controller, FakeIngestRawFileImportController):
            self.fail(
                "Controller is not of type IngestRawFileImportControllerForTests."
            )
        self.assertFalse(controller.has_temp_paths_in_disk())

        self.validate_file_metadata(controller)

        self.check_imported_path_count(controller, 3)

    def test_unzip_zip_normalized_paths(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
        )

        expected_file_tags = [
            "tagBasicData",
            "tagHeadersNoContents",
            "tagMoreBasicData",
        ]

        add_paths_with_tags(
            controller, ["tagBasicData"], should_normalize=True, file_extension="zip"
        )
        add_paths_with_tags(
            controller,
            ["tagHeadersNoContents", "tagMoreBasicData"],
            should_normalize=True,
        )
        run_task_queues_to_empty(controller)

        deprecated_files = controller.fs.ls_with_blob_prefix(
            controller.raw_data_storage_directory_path.bucket_path.bucket_name,
            os.path.join(
                controller.raw_data_storage_directory_path.relative_path, "deprecated"
            ),
        )
        deprecated_file = assert_type(one(deprecated_files), GcsfsFilePath)
        self.assertTrue("tagBasicData.zip" in deprecated_file.file_name)

        # Delete deprecated file so it doesn't get in the way of other checking in
        # check_all_paths_processed.
        controller.fs.delete(deprecated_file)

        # This confirms that all three expected files were discovered and processed
        # fully, inluding the file inside of the zip file.
        check_all_paths_processed(
            self, controller, expected_file_tags, unexpected_tags=[]
        )

        if not isinstance(controller, FakeIngestRawFileImportController):
            self.fail(
                "Controller is not of type IngestRawFileImportControllerForTests."
            )
        self.assertFalse(controller.has_temp_paths_in_disk())

        self.validate_file_metadata(controller)

        self.check_imported_path_count(controller, 3)

    def test_state_unexpected_tag(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
        )

        file_tags = [
            "tagMoreBasicData",
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

        self.check_imported_path_count(controller, 3)

        expected_raw_metadata_tags_with_is_processed = [
            ("tagMoreBasicData", True),
            ("tagHeadersNoContents", True),
            ("tagBasicData", True),
            ("Unexpected_Tag", False),
        ]
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed,
        )

    def test_state_tag_we_import_but_do_not_ingest(
        self,
    ) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
        )

        file_tags = [
            "tagMoreBasicData",
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

        self.check_imported_path_count(controller, 4)

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[
                (tag, True) for tag in file_tags
            ],
        )

    def test_process_already_normalized_paths(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
        )

        file_tags = list(sorted(DEFAULT_INPUT_RAW_FILE_TAGS))

        add_paths_with_tags_and_process(
            self,
            controller,
            file_tags,
            should_normalize=True,
        )

        self.validate_file_metadata(controller)

    def _path_in_storage_dir(
        self, path: GcsfsFilePath, controller: IngestRawFileImportController
    ) -> bool:
        return path.abs_path().startswith(
            controller.raw_data_storage_directory_path.abs_path()
        )

    def test_cloud_function_fails_on_new_file(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=False,
        )

        task_manager = self.get_fake_task_manager(controller)

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        # Upload two new files without triggering the controller
        fixture_util.add_direct_ingest_path(
            fs=controller.fs.gcs_file_system,
            bucket_path=controller.raw_data_bucket_path,
            filename="tagMoreBasicData.csv",
            should_normalize=False,
            region_code=controller.region_code(),
            fail_handle_file_call=True,
        )
        fixture_util.add_direct_ingest_path(
            fs=controller.fs.gcs_file_system,
            bucket_path=controller.raw_data_bucket_path,
            filename="tagHeadersNoContents.csv",
            should_normalize=False,
            region_code=controller.region_code(),
            fail_handle_file_call=True,
        )

        self.assertTrue(
            task_manager.get_scheduler_queue_info(
                controller.region, controller.ingest_instance
            ).is_empty(),
        )

        # Later file that succeeds will trigger proper upload of all files
        fixture_util.add_direct_ingest_path(
            fs=controller.fs.gcs_file_system,
            bucket_path=controller.raw_data_bucket_path,
            filename="tagBasicData.csv",
            should_normalize=True,
            region_code=controller.region_code(),
        )

        run_task_queues_to_empty(controller)
        check_all_paths_processed(
            self,
            controller,
            ["tagMoreBasicData", "tagHeadersNoContents", "tagBasicData"],
            unexpected_tags=[],
        )
        self.validate_file_metadata(controller)

    def test_cloud_function_fails_on_new_file_rename_later_with_cron(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=False,
        )

        task_manager = self.get_fake_task_manager(controller)

        if not isinstance(controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(
                f"Controller fs must have type "
                f"FakeGCSFileSystem. Found instead "
                f"type [{type(controller.fs.gcs_file_system)}]"
            )

        # Upload new files without triggering the controller
        fixture_util.add_direct_ingest_path(
            fs=controller.fs.gcs_file_system,
            bucket_path=controller.raw_data_bucket_path,
            filename="tagMoreBasicData.csv",
            should_normalize=False,
            region_code=controller.region_code(),
            fail_handle_file_call=True,
        )
        fixture_util.add_direct_ingest_path(
            fs=controller.fs.gcs_file_system,
            bucket_path=controller.raw_data_bucket_path,
            filename="tagHeadersNoContents.csv",
            should_normalize=False,
            region_code=controller.region_code(),
            fail_handle_file_call=True,
        )
        fixture_util.add_direct_ingest_path(
            fs=controller.fs.gcs_file_system,
            bucket_path=controller.raw_data_bucket_path,
            filename="tagBasicData.csv",
            should_normalize=False,
            region_code=controller.region_code(),
            fail_handle_file_call=True,
        )

        self.assertTrue(
            task_manager.get_scheduler_queue_info(
                controller.region, controller.ingest_instance
            ).is_empty(),
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
            ingest_instance=controller.ingest_instance,
            can_start_ingest=True,
        )

        run_task_queues_to_empty(controller)

        for path in controller.fs.gcs_file_system.all_paths:
            if not isinstance(path, GcsfsFilePath):
                self.fail(f"Unexpected path type: {path.abs_path()}")
            self.assertTrue(controller.fs.is_normalized_file_path(path))

        self.validate_file_metadata(controller)

    def test_can_start_ingest_is_false_launched_region_throws(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            # US_DD is launched in production, unlike US_XX
            state_code=StateCode.US_DD,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
            can_start_ingest=False,
        )

        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            Mock(return_value="production"),
        ):
            file_tags = ["ingest12"]

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
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
            can_start_ingest=False,
        )

        file_tags = list(reversed(sorted(DEFAULT_INPUT_RAW_FILE_TAGS)))

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
            self.assertTrue(controller.fs.is_normalized_file_path(path))
            self.assertTrue(controller.fs.is_seen_unprocessed_file(path))
            self.assertFalse(controller.fs.is_processed_file(path))

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[],
        )

    def test_unlaunched_region_raises_if_can_start_ingest_true(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
        )
        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            Mock(return_value="production"),
        ):
            add_paths_with_tags(controller, ["tagMoreBasicData"])
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
            self.assertTrue(controller.fs.is_normalized_file_path(path))
            self.assertTrue(controller.fs.is_seen_unprocessed_file(path))
            self.assertFalse(controller.fs.is_processed_file(path))

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[],
        )

    def test_launched_region_raises_if_can_start_ingest_false(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
            run_async=True,
            can_start_ingest=False,
        )

        add_paths_with_tags(controller, ["tagMoreBasicData"])
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
        )

    def test_processing_continues_if_there_are_subfolders_in_ingest_dir(self) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses,
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
            controller.raw_data_bucket_path, subdir_path_name
        )
        controller.fs.gcs_file_system.test_add_path(subdir_path, local_path=None)

        path_names = [
            ("subdir/Unexpected_Tag.csv", False),
            ("tagMoreBasicData.csv", True),
            ("tagHeadersNoContents.csv", True),
            ("tagBasicData.csv", True),
            ("subdir/tagBasicData2.csv", False),
        ]

        for path_name, has_fixture in path_names:
            fixture_util.add_direct_ingest_path(
                fs=controller.fs.gcs_file_system,
                bucket_path=controller.raw_data_bucket_path,
                filename=path_name,
                should_normalize=False,
                region_code=controller.region_code(),
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

            if path.abs_path().startswith(
                controller.raw_data_storage_directory_path.abs_path()
            ):
                storage_file_paths.append(path)
            else:
                self.assertTrue(
                    path.abs_path().startswith(
                        controller.raw_data_bucket_path.abs_path()
                    )
                )
                ingest_file_paths.append(path)

        self.assertEqual(1, len(dir_paths_found))
        self.assertEqual(subdir_path, dir_paths_found[0])

        # Three raw data files
        self.assertEqual(3, len(storage_file_paths))
        storage_tags = {
            filename_parts_from_path(path).file_tag for path in storage_file_paths
        }
        self.assertEqual(
            {"tagMoreBasicData", "tagHeadersNoContents", "tagBasicData"}, storage_tags
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

    def test_do_not_schedule_another_raw_data_import_task_if_already_processed_in_gcs(
        self,
    ) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses
            + [DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS],
            run_async=False,
        )
        raw_data_path = fixture_util.add_direct_ingest_path(
            fs=controller.fs.gcs_file_system,
            bucket_path=controller.raw_data_bucket_path,
            filename="tagMoreBasicData.csv",
            should_normalize=True,
            region_code=controller.region_code(),
            has_fixture=True,
        )
        controller.raw_file_metadata_manager.mark_raw_file_as_discovered(raw_data_path)
        controller.raw_file_metadata_manager.mark_raw_file_as_processed(raw_data_path)

        controller.schedule_next_ingest_task(
            current_task_id=build_scheduler_task_id(
                controller.region, controller.ingest_instance
            ),
        )
        self.assertTrue(
            controller.cloud_task_manager.get_raw_data_import_queue_info(
                controller.region, controller.ingest_instance
            ).is_empty(),
        )
        run_task_queues_to_empty(controller)

    def test_do_raw_data_import_locks_file_to_prevent_duplicate_imports(
        self,
    ) -> None:
        controller = build_fake_ingest_raw_file_import_controller(
            state_code=StateCode.US_XX,
            ingest_instance=self.ingest_instance,
            initial_statuses=self.rerun_just_started_statuses
            + [DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS],
            run_async=False,
        )
        add_paths_with_tags(controller, ["someDuplicate"])
        raw_data_path = fixture_util.add_direct_ingest_path(
            fs=controller.fs.gcs_file_system,
            bucket_path=controller.raw_data_bucket_path,
            filename="someDuplicate.csv",
            should_normalize=True,
            region_code=controller.region_code(),
            has_fixture=False,
        )
        controller.raw_file_metadata_manager.mark_raw_file_as_discovered(raw_data_path)
        data_args = GcsfsRawDataBQImportArgs(raw_data_file_path=raw_data_path)

        task_manager = self.get_fake_task_manager(controller)
        task_manager.create_direct_ingest_raw_data_import_task(
            controller.region, controller.ingest_instance, data_args
        )
        task_manager.create_direct_ingest_raw_data_import_task(
            controller.region, controller.ingest_instance, data_args
        )
        with self.assertRaises(GCSPseudoLockFailedUnlock):
            task_manager.test_run_next_raw_data_import_task(controller.ingest_instance)
        with SessionFactory.using_database(
            controller.raw_file_metadata_manager.database_key
        ) as session:
            metadata = controller.raw_file_metadata_manager._get_raw_file_metadata_for_path(  # pylint: disable=protected-access
                session,
                raw_data_path,
            )

            metadata.file_processed_time = None

        with self.assertRaises(GCSPseudoLockAlreadyExists):
            task_manager.test_run_next_raw_data_import_task(controller.ingest_instance)


class TestSecondaryController(IngestRawFileImportControllerTest):
    """Organizes base direct ingest controller tests for reruns in SECONDARY instances with raw data import in
    SECONDARY."""

    __test__ = True

    @property
    def ingest_instance(self) -> DirectIngestInstance:
        return DirectIngestInstance.SECONDARY

    @property
    def rerun_just_started_statuses(self) -> List[DirectIngestStatus]:
        return [
            DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
        ]

    @property
    def initial_instance_statuses(self) -> List[DirectIngestStatus]:
        return [DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS]

    def test_run_with_no_work_secondary(self) -> None:
        controller = self.run_no_data_test()

        # We should still end up in NO_RAW_DATA_REIMPORT_IN_PROGRESS status since there is nothing to do
        self.assertEqual(
            DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
            controller.ingest_instance_status_manager.get_current_status(),
        )


class TestPrimaryController(IngestRawFileImportControllerTest):
    """Organizes base direct ingest controller tests for PRIMARY instances with raw data import in PRIMARY."""

    __test__ = True

    @property
    def ingest_instance(self) -> DirectIngestInstance:
        return DirectIngestInstance.PRIMARY

    @property
    def rerun_just_started_statuses(self) -> List[DirectIngestStatus]:
        return [DirectIngestStatus.INITIAL_STATE]

    @property
    def initial_instance_statuses(self) -> List[DirectIngestStatus]:
        return [DirectIngestStatus.INITIAL_STATE]

    def test_run_with_no_work_primary(self) -> None:
        controller = self.run_no_data_test()

        self.assertEqual(
            DirectIngestStatus.RAW_DATA_UP_TO_DATE,
            controller.ingest_instance_status_manager.get_current_status(),
        )
