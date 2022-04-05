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
from typing import List, Optional, Set, Tuple

from freezegun import freeze_time
from mock import patch, Mock

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.serialization import attr_to_json_dict, \
    datetime_to_serializable, serializable_to_datetime, attr_from_json_dict
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller \
    import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    SPLIT_FILE_STORAGE_SUBDIR, GcsfsFileContentsHandle
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath, \
    GcsfsDirectoryPath
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs, filename_parts_from_path, GcsfsDirectIngestFileType, GcsfsIngestViewExportArgs
from recidiviz.ingest.direct.controllers.postgres_direct_ingest_file_metadata_manager import \
    PostgresDirectIngestFileMetadataManager
from recidiviz.persistence.database.base_schema import OperationsBase
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    build_gcsfs_controller_for_tests, add_paths_with_tags_and_process, \
    path_for_fixture_file, \
    run_task_queues_to_empty, check_all_paths_processed, FakeDirectIngestRawFileImportManager
from recidiviz.tests.ingest.direct.fake_direct_ingest_big_query_client import FakeDirectIngestBigQueryClient
from recidiviz.tests.ingest.direct.fake_direct_ingest_gcs_file_system import FakeDirectIngestGCSFileSystem
from recidiviz.tests.ingest.direct. \
    fake_synchronous_direct_ingest_cloud_task_manager import \
    FakeSynchronousDirectIngestCloudTaskManager
from recidiviz.tests.utils import fakes
from recidiviz.tests.utils.fake_region import TEST_STATE_REGION, \
    TEST_COUNTY_REGION, fake_region


class BaseTestCsvGcsfsDirectIngestController(CsvGcsfsDirectIngestController):
    """Base class for test direct ingest controllers used in this file."""
    def __init__(self,
                 region_name: str,
                 system_level: SystemLevel,
                 ingest_directory_path: Optional[str],
                 storage_directory_path: Optional[str],
                 max_delay_sec_between_files: int):
        super().__init__(region_name,
                         system_level,
                         ingest_directory_path,
                         storage_directory_path,
                         max_delay_sec_between_files)
        self.local_paths: Set[str] = set()

    @classmethod
    @abc.abstractmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        pass

    def _get_contents_handle(
            self, args: GcsfsIngestArgs) -> Optional[GcsfsFileContentsHandle]:
        handle = super()._get_contents_handle(args)
        if handle:
            self.local_paths.add(handle.local_file_path)
        return handle

    def has_temp_paths_in_disk(self):
        for path in self.local_paths:
            if os.path.exists(path):
                return True
        return False


class StateTestGcsfsDirectIngestController(
        BaseTestCsvGcsfsDirectIngestController):
    def __init__(self,
                 ingest_directory_path: str,
                 storage_directory_path: str,
                 max_delay_sec_between_files: int = 0):
        super().__init__(TEST_STATE_REGION.region_code,
                         SystemLevel.STATE,
                         ingest_directory_path,
                         storage_directory_path,
                         max_delay_sec_between_files)

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return ['tagA', 'tagB', 'tagC']


class SingleTagStateTestGcsfsDirectIngestController(
        BaseTestCsvGcsfsDirectIngestController):
    def __init__(self,
                 ingest_directory_path: str,
                 storage_directory_path: str,
                 max_delay_sec_between_files: int = 0):
        super().__init__(TEST_STATE_REGION.region_code,
                         SystemLevel.STATE,
                         ingest_directory_path,
                         storage_directory_path,
                         max_delay_sec_between_files)

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return ['tagC']


class CountyTestGcsfsDirectIngestController(
        BaseTestCsvGcsfsDirectIngestController):
    def __init__(self,
                 ingest_directory_path: str,
                 storage_directory_path: str,
                 max_delay_sec_between_files: int = 0):
        super().__init__(TEST_COUNTY_REGION.region_code,
                         SystemLevel.COUNTY,
                         ingest_directory_path,
                         storage_directory_path,
                         max_delay_sec_between_files)

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return ['tagA', 'tagB']


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
class TestGcsfsDirectIngestController(unittest.TestCase):
    """Tests for GcsfsDirectIngestController."""

    FIXTURE_PATH_PREFIX = 'direct/controllers'

    TEST_BQ_IMPORT_ENABLED_REGION = fake_region(
        region_code='us_xx',
        is_raw_vs_ingest_file_name_detection_enabled=True,
        are_raw_data_bq_imports_enabled_in_env=True
    )

    TEST_SQL_PRE_PROCESSING_LAUNCHED_REGION = fake_region(
        region_code='us_xx',
        is_raw_vs_ingest_file_name_detection_enabled=True,
        are_raw_data_bq_imports_enabled_in_env=True,
        are_ingest_view_exports_enabled_in_env=True
    )

    @classmethod
    def setUpClass(cls) -> None:
        fakes.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        fakes.use_on_disk_postgresql_database(OperationsBase)

    def tearDown(self) -> None:
        fakes.teardown_on_disk_postgresql_database(OperationsBase)

    @classmethod
    def tearDownClass(cls) -> None:
        fakes.stop_and_clear_on_disk_postgresql_database()

    def validate_file_metadata(
            self,
            controller: GcsfsDirectIngestController,
            expected_raw_metadata_tags_with_is_processed: Optional[List[Tuple[str, bool]]] = None,
            expected_ingest_metadata_tags_with_is_processed: Optional[List[Tuple[str, bool]]] = None):
        """Validates that the file metadata was recorded as expected."""

        if not controller.region.is_raw_vs_ingest_file_name_detection_enabled():
            expected_raw_metadata_tags_with_is_processed = []
        elif expected_raw_metadata_tags_with_is_processed is None:
            expected_raw_metadata_tags_with_is_processed = [(tag, True)
                                                            for tag in controller.get_file_tag_rank_list()]

        if not controller.region.are_ingest_view_exports_enabled_in_env():
            expected_ingest_metadata_tags_with_is_processed = []
        elif expected_ingest_metadata_tags_with_is_processed is None:
            expected_ingest_metadata_tags_with_is_processed = [(tag, True)
                                                               for tag in controller.get_file_tag_rank_list()]

        file_metadata_manager = controller.file_metadata_manager
        if not isinstance(file_metadata_manager, PostgresDirectIngestFileMetadataManager):
            self.fail(f'Unexpected file_metadata_manager type {file_metadata_manager}')

        session = SessionFactory.for_schema_base(OperationsBase)
        try:
            raw_file_results = session.query(schema.DirectIngestRawFileMetadata).all()

            raw_file_metadata_list = [
                # pylint:disable=protected-access
                file_metadata_manager._raw_file_schema_metadata_as_entity(metadata) for metadata in raw_file_results]

            ingest_file_results = session.query(schema.DirectIngestIngestFileMetadata).all()

            ingest_file_metadata_list = [
                # pylint:disable=protected-access
                file_metadata_manager._ingest_file_schema_metadata_as_entity(metadata)
                for metadata in ingest_file_results]

        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        actual_raw_metadata_tags_with_is_processed = [(metadata.file_tag, bool(metadata.processed_time))
                                                      for metadata in raw_file_metadata_list]

        if not controller.region.is_raw_vs_ingest_file_name_detection_enabled():
            self.assertEqual([], actual_raw_metadata_tags_with_is_processed)

        self.assertEqual(sorted(expected_raw_metadata_tags_with_is_processed),
                         sorted(actual_raw_metadata_tags_with_is_processed))

        actual_ingest_metadata_tags_with_is_processed = [(metadata.file_tag, bool(metadata.processed_time))
                                                         for metadata in ingest_file_metadata_list]

        if not controller.region.are_ingest_view_exports_enabled_in_env():
            self.assertEqual([], actual_ingest_metadata_tags_with_is_processed)

        self.assertEqual(sorted(expected_ingest_metadata_tags_with_is_processed),
                         sorted(actual_ingest_metadata_tags_with_is_processed))

        # TODO(3020): Update this to better test that metadata for split files gets properly registered

    def run_async_file_order_test_for_controller_cls(self, controller_cls) -> GcsfsDirectIngestController:
        """Writes all expected files to the mock fs, then kicks the controller
        and ensures that all jobs are run to completion in the proper order."""

        controller = build_gcsfs_controller_for_tests(controller_cls,
                                                      self.FIXTURE_PATH_PREFIX,
                                                      run_async=True)

        file_tags = list(
            reversed(sorted(controller.get_file_tag_rank_list())))

        add_paths_with_tags_and_process(self, controller, file_tags)

        self.assertIsInstance(controller,
                              BaseTestCsvGcsfsDirectIngestController)
        self.assertFalse(controller.has_temp_paths_in_disk())

        self.validate_file_metadata(controller)

        return controller

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_STATE_REGION))
    def test_state_runs_files_in_order(self):
        self.run_async_file_order_test_for_controller_cls(
            StateTestGcsfsDirectIngestController)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_BQ_IMPORT_ENABLED_REGION))
    def test_state_runs_files_in_order_bq_import_enabled(self):

        controller = self.run_async_file_order_test_for_controller_cls(
            StateTestGcsfsDirectIngestController)

        self.assertIsInstance(controller.raw_file_import_manager, FakeDirectIngestRawFileImportManager)
        self.assertEqual(3, len(controller.raw_file_import_manager.imported_paths))

        self.assertIsInstance(controller.ingest_view_export_manager.big_query_client, FakeDirectIngestBigQueryClient)
        self.assertCountEqual([], controller.ingest_view_export_manager.big_query_client.exported_file_tags)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_SQL_PRE_PROCESSING_LAUNCHED_REGION))
    def test_state_runs_files_in_order_sql_preprocessing_fully_launched(self):
        controller = self.run_async_file_order_test_for_controller_cls(StateTestGcsfsDirectIngestController)

        self.assertIsInstance(controller.raw_file_import_manager, FakeDirectIngestRawFileImportManager)
        self.assertEqual(3, len(controller.raw_file_import_manager.imported_paths))

        self.assertIsInstance(controller.ingest_view_export_manager.big_query_client, FakeDirectIngestBigQueryClient)
        self.assertCountEqual(controller.get_file_tag_rank_list(),
                              controller.ingest_view_export_manager.big_query_client.exported_file_tags)

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_COUNTY_REGION))
    def test_county_runs_files_in_order(self):
        self.run_async_file_order_test_for_controller_cls(
            CountyTestGcsfsDirectIngestController)

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_STATE_REGION))
    def test_state_unexpected_tag(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        file_tags = ['tagA', 'Unexpected_Tag', 'tagB', 'tagC']
        unexpected_tags = ['Unexpected_Tag']

        add_paths_with_tags_and_process(
            self, controller, file_tags, unexpected_tags)

        split_paths = {path
                       for path in controller.fs.all_paths
                       if controller.fs.is_split_file(path)}
        self.assertFalse(split_paths)
        self.validate_file_metadata(controller)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_BQ_IMPORT_ENABLED_REGION))
    def test_state_unexpected_tag_bq_imports_enabled(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        file_tags = ['tagA', 'Unexpected_Tag', 'tagB', 'tagC']
        unexpected_tags = ['Unexpected_Tag']

        add_paths_with_tags_and_process(
            self, controller, file_tags, unexpected_tags)

        split_paths = {path
                       for path in controller.fs.all_paths
                       if controller.fs.is_split_file(path)}
        self.assertFalse(split_paths)

        self.assertIsInstance(controller.raw_file_import_manager, FakeDirectIngestRawFileImportManager)
        self.assertEqual(3, len(controller.raw_file_import_manager.imported_paths))
        self.assertIsInstance(controller.ingest_view_export_manager.big_query_client, FakeDirectIngestBigQueryClient)
        self.assertCountEqual([], controller.ingest_view_export_manager.big_query_client.exported_file_tags)

        expected_raw_metadata_tags_with_is_processed = [('tagA', True), ('tagB', True), ('tagC', True),
                                                        ('Unexpected_Tag', False)]
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed)


    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_SQL_PRE_PROCESSING_LAUNCHED_REGION))
    def test_state_single_split_tag_sql_preprocessing_fully_launched(self):
        controller = build_gcsfs_controller_for_tests(
            SingleTagStateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        file_tags = ['tagC']
        unexpected_tags = []

        add_paths_with_tags_and_process(
            self, controller, file_tags, unexpected_tags)

        processed_split_file_paths = defaultdict(list)
        for path in controller.fs.all_paths:
            if self._path_in_split_file_storage_subdir(path, controller):
                file_tag = filename_parts_from_path(path).file_tag
                processed_split_file_paths[file_tag].append(path)

        self.assertEqual(1, len(processed_split_file_paths.keys()))
        self.assertEqual(2, len(processed_split_file_paths['tagC']))

        found_suffixes = {filename_parts_from_path(p).filename_suffix
                          for p in processed_split_file_paths['tagC']}
        self.assertEqual(found_suffixes, {'00001_file_split_size1',
                                          '00002_file_split_size1'})

        self.assertIsInstance(controller.raw_file_import_manager, FakeDirectIngestRawFileImportManager)
        self.assertEqual(1, len(controller.raw_file_import_manager.imported_paths))

        expected_raw_metadata_tags_with_is_processed = [('tagC', True)]
        expected_ingest_metadata_tags_with_is_processed = [('tagC', True), ('tagC', True), ('tagC', True)]
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed,
            expected_ingest_metadata_tags_with_is_processed=expected_ingest_metadata_tags_with_is_processed
        )

        self.assertIsInstance(controller.ingest_view_export_manager.big_query_client, FakeDirectIngestBigQueryClient)
        self.assertCountEqual(controller.get_file_tag_rank_list(),
                              controller.ingest_view_export_manager.big_query_client.exported_file_tags)


    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_SQL_PRE_PROCESSING_LAUNCHED_REGION))
    def test_state_unexpected_tag_sql_preprocessing_fully_launched(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        file_tags = ['tagA', 'Unexpected_Tag', 'tagB', 'tagC']
        unexpected_tags = ['Unexpected_Tag']

        add_paths_with_tags_and_process(
            self, controller, file_tags, unexpected_tags)

        split_paths = {path
                       for path in controller.fs.all_paths
                       if controller.fs.is_split_file(path)}
        self.assertFalse(split_paths)

        self.assertIsInstance(controller.raw_file_import_manager, FakeDirectIngestRawFileImportManager)
        self.assertEqual(3, len(controller.raw_file_import_manager.imported_paths))

        expected_raw_metadata_tags_with_is_processed = [('tagA', True), ('tagB', True), ('tagC', True),
                                                        ('Unexpected_Tag', False)]
        expected_ingest_metadata_tags_with_is_processed = [('tagA', True), ('tagB', True), ('tagC', True)]
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed,
            expected_ingest_metadata_tags_with_is_processed=expected_ingest_metadata_tags_with_is_processed
        )

        self.assertIsInstance(controller.ingest_view_export_manager.big_query_client, FakeDirectIngestBigQueryClient)
        self.assertCountEqual(controller.get_file_tag_rank_list(),
                              controller.ingest_view_export_manager.big_query_client.exported_file_tags)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_BQ_IMPORT_ENABLED_REGION))
    def test_state_tag_we_import_but_do_not_ingest(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        file_tags = ['tagA', 'tagWeDoNotIngest', 'tagB', 'tagC']

        add_paths_with_tags_and_process(self, controller, file_tags)

        split_paths = {path
                       for path in controller.fs.all_paths
                       if controller.fs.is_split_file(path)}
        self.assertFalse(split_paths)

        self.assertIsInstance(controller.raw_file_import_manager, FakeDirectIngestRawFileImportManager)
        self.assertEqual(4, len(controller.raw_file_import_manager.imported_paths))
        self.assertCountEqual([], controller.ingest_view_export_manager.big_query_client.exported_file_tags)
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[(tag, True) for tag in file_tags])

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_SQL_PRE_PROCESSING_LAUNCHED_REGION))
    def test_state_tag_we_import_but_do_not_ingest_sql_preprocessing_fully_launched(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        file_tags = ['tagA', 'tagWeDoNotIngest', 'tagB', 'tagC']

        add_paths_with_tags_and_process(self, controller, file_tags)

        split_paths = {path
                       for path in controller.fs.all_paths
                       if controller.fs.is_split_file(path)}
        self.assertFalse(split_paths)

        self.assertIsInstance(controller.raw_file_import_manager, FakeDirectIngestRawFileImportManager)
        self.assertEqual(4, len(controller.raw_file_import_manager.imported_paths))

        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[(tag, True) for tag in file_tags],
            expected_ingest_metadata_tags_with_is_processed=[(tag, True) for tag in ['tagA', 'tagB', 'tagC']])

        self.assertIsInstance(controller.ingest_view_export_manager.big_query_client, FakeDirectIngestBigQueryClient)
        self.assertCountEqual(['tagA', 'tagB', 'tagC'],
                              controller.ingest_view_export_manager.big_query_client.exported_file_tags)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_do_not_queue_same_job_twice(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        self.assertIsInstance(
            controller.cloud_task_manager,
            FakeSynchronousDirectIngestCloudTaskManager,
            "Expected FakeSynchronousDirectIngestCloudTaskManager")
        task_manager = controller.cloud_task_manager

        file_path = \
            path_for_fixture_file(controller, 'tagA.csv',
                                  should_normalize=False)
        file_path2 = \
            path_for_fixture_file(controller, 'tagB.csv',
                                  should_normalize=False)
        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        controller.fs.test_add_path(file_path)

        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        controller.fs.test_add_path(file_path2)

        # Task for handling unnormalized file_path2
        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        task_manager.test_run_next_process_job_task()
        task_manager.test_pop_finished_process_job_task()

        # This is the task that got queued by after we normalized the path,
        # which will schedule the next process_job.
        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        # This is the task that got queued by finishing a job
        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(controller.region).size())
        self.assertEqual(
            1,
            task_manager.get_process_job_queue_info(controller.region).size())

        self.validate_file_metadata(controller)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_next_schedule_runs_before_process_job_clears(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        self.assertIsInstance(
            controller.cloud_task_manager,
            FakeSynchronousDirectIngestCloudTaskManager,
            "Expected FakeSynchronousDirectIngestCloudTaskManager")
        task_manager = controller.cloud_task_manager

        file_path = \
            path_for_fixture_file(controller, 'tagA.csv',
                                  should_normalize=False)
        file_path2 = \
            path_for_fixture_file(controller, 'tagB.csv',
                                  should_normalize=False)
        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        controller.fs.test_add_path(file_path)
        controller.fs.test_add_path(file_path2)

        # At this point we have a series of tasks handling / renaming /
        # splitting the new files, then scheduling the next job. They run in
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
            0,
            task_manager.get_scheduler_queue_info(controller.region).size())
        self.assertEqual(
            0,
            task_manager.get_process_job_queue_info(controller.region).size())
        self.validate_file_metadata(controller)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_SQL_PRE_PROCESSING_LAUNCHED_REGION))
    def test_process_job_task_run_twice(self):
        # Cloud Tasks has an at-least once guarantee - make sure rerunning a task in series does not crash
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        self.assertIsInstance(
            controller.cloud_task_manager,
            FakeSynchronousDirectIngestCloudTaskManager,
            "Expected FakeSynchronousDirectIngestCloudTaskManager")
        task_manager = controller.cloud_task_manager

        dt = datetime.datetime.now()
        file_path = \
            path_for_fixture_file(controller, 'tagA.csv',
                                  file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
                                  should_normalize=True,
                                  dt=dt)

        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        controller.fs.test_add_path(file_path)
        parts = filename_parts_from_path(file_path)
        metadata = controller.file_metadata_manager.register_ingest_file_export_job(GcsfsIngestViewExportArgs(
            ingest_view_name=parts.file_tag,
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=dt
        ))
        controller.file_metadata_manager.register_ingest_view_export_file_name(metadata, file_path)

        # At this point we have a series of tasks handling / renaming /
        # splitting the new files, then scheduling the next job. They run in
        # quick succession.
        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        # Process job tasks starts as a result of the first schedule.
        task_manager.test_run_next_process_job_task()

        task = task_manager.test_pop_finished_process_job_task()

        task_manager.create_direct_ingest_process_job_task(*task)

        # Now run the repeated task
        task_manager.test_run_next_process_job_task()
        task_manager.test_pop_finished_process_job_task()

        while task_manager.scheduler_tasks:
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(controller.region).size())
        self.assertEqual(
            0,
            task_manager.get_process_job_queue_info(controller.region).size())
        self.validate_file_metadata(controller,
                                    expected_raw_metadata_tags_with_is_processed=[],
                                    expected_ingest_metadata_tags_with_is_processed=[('tagA', True)])


    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_do_not_schedule_more_than_one_delayed_scheduler_job(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
            max_delay_sec_between_files=300
        )
        self.assertIsInstance(
            controller.cloud_task_manager,
            FakeSynchronousDirectIngestCloudTaskManager,
            "Expected FakeSynchronousDirectIngestCloudTaskManager")
        task_manager = controller.cloud_task_manager

        path = path_for_fixture_file(controller,
                                     'tagB.csv',
                                     should_normalize=False)

        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        # This will kick the scheduler once
        controller.fs.test_add_path(path)

        # Kick the scheduler 5 times
        for _ in range(5):
            controller.kick_scheduler(just_finished_job=False)

        # We have now queued 6 immediate jobs
        for _ in range(6):
            task_manager.test_run_next_scheduler_task()
            task_manager.test_pop_finished_scheduler_task()

        # But after running all those jobs, there should only be one job in
        # the queue.
        self.assertEqual(
            1,
            task_manager.get_scheduler_queue_info(controller.region).size())
        self.assertEqual(
            0,
            task_manager.get_process_job_queue_info(controller.region).size())

        self.validate_file_metadata(controller)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_process_already_normalized_paths(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        file_tags = list(sorted(controller.get_file_tag_rank_list()))

        add_paths_with_tags_and_process(self,
                                        controller,
                                        file_tags,
                                        pre_normalize_filename=True)

        self.validate_file_metadata(controller)

    def _path_in_storage_dir(
            self,
            path: GcsfsFilePath,
            controller: GcsfsDirectIngestController):
        return path.abs_path().startswith(
            controller.storage_directory_path.abs_path())

    def _path_in_split_file_storage_subdir(
            self,
            path: GcsfsFilePath,
            controller: GcsfsDirectIngestController):
        if self._path_in_storage_dir(path, controller):
            directory, _ = os.path.split(path.abs_path())
            if SPLIT_FILE_STORAGE_SUBDIR in directory:
                return True
        return False

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_process_file_that_needs_splitting(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        file_tags = list(sorted(controller.get_file_tag_rank_list()))

        add_paths_with_tags_and_process(self,
                                        controller,
                                        file_tags,
                                        pre_normalize_filename=True)

        processed_split_file_paths = defaultdict(list)
        for path in controller.fs.all_paths:
            if self._path_in_split_file_storage_subdir(path, controller):
                file_tag = filename_parts_from_path(path).file_tag
                processed_split_file_paths[file_tag].append(path)

        self.assertEqual(1, len(processed_split_file_paths.keys()))
        self.assertEqual(2, len(processed_split_file_paths['tagC']))

        found_suffixes = {filename_parts_from_path(p).filename_suffix
                          for p in processed_split_file_paths['tagC']}
        self.assertEqual(found_suffixes, {'00001_file_split_size1',
                                          '00002_file_split_size1'})
        self.validate_file_metadata(controller)

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_BQ_IMPORT_ENABLED_REGION))
    def test_process_ingest_view_file_that_needs_splitting_bq_import_enabled(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        file_tags = list(sorted(controller.get_file_tag_rank_list()))

        add_paths_with_tags_and_process(self,
                                        controller,
                                        file_tags,
                                        pre_normalize_filename=True,
                                        file_type=GcsfsDirectIngestFileType.INGEST_VIEW)

        processed_split_file_paths = defaultdict(list)
        for path in controller.fs.all_paths:
            if self._path_in_split_file_storage_subdir(path, controller):
                file_tag = filename_parts_from_path(path).file_tag
                processed_split_file_paths[file_tag].append(path)

        self.assertEqual(1, len(processed_split_file_paths.keys()))
        self.assertEqual(2, len(processed_split_file_paths['tagC']))

        found_suffixes = {filename_parts_from_path(p).filename_suffix
                          for p in processed_split_file_paths['tagC']}
        self.assertEqual(found_suffixes, {'00001_file_split_size1',
                                          '00002_file_split_size1'})
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[],
            expected_ingest_metadata_tags_with_is_processed=[])

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_SQL_PRE_PROCESSING_LAUNCHED_REGION))
    def test_process_ingest_view_file_that_needs_splitting_sql_preprocessing_fully_launched(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        file_tags = list(sorted(controller.get_file_tag_rank_list()))

        add_paths_with_tags_and_process(self,
                                        controller,
                                        file_tags,
                                        pre_normalize_filename=True,
                                        file_type=GcsfsDirectIngestFileType.RAW_DATA)

        processed_split_file_paths = defaultdict(list)
        for path in controller.fs.all_paths:
            if self._path_in_split_file_storage_subdir(path, controller):
                file_tag = filename_parts_from_path(path).file_tag
                processed_split_file_paths[file_tag].append(path)

        self.assertEqual(1, len(processed_split_file_paths.keys()))
        self.assertEqual(2, len(processed_split_file_paths['tagC']))

        found_suffixes = {filename_parts_from_path(p).filename_suffix
                          for p in processed_split_file_paths['tagC']}
        self.assertEqual(found_suffixes, {'00001_file_split_size1',
                                          '00002_file_split_size1'})
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[(tag, True) for tag in controller.get_file_tag_rank_list()],
            expected_ingest_metadata_tags_with_is_processed=[
                ('tagA', True), ('tagB', True), ('tagC', True), ('tagC', True), ('tagC', True)])

        self.assertIsInstance(controller.raw_file_import_manager, FakeDirectIngestRawFileImportManager)
        self.assertEqual(3, len(controller.raw_file_import_manager.imported_paths))

        self.assertIsInstance(controller.ingest_view_export_manager.big_query_client, FakeDirectIngestBigQueryClient)
        self.assertCountEqual(controller.get_file_tag_rank_list(),
                              controller.ingest_view_export_manager.big_query_client.exported_file_tags)

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_BQ_IMPORT_ENABLED_REGION))
    def test_process_raw_file_that_gets_imported_but_not_ingested_never_split(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=True)

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        # pylint:disable=protected-access
        file_tags = ['tagWeDoNotIngest']

        add_paths_with_tags_and_process(self,
                                        controller,
                                        file_tags,
                                        pre_normalize_filename=True,
                                        file_type=GcsfsDirectIngestFileType.RAW_DATA)

        processed_split_file_paths = defaultdict(list)
        for path in controller.fs.all_paths:
            if self._path_in_split_file_storage_subdir(path, controller):
                file_tag = filename_parts_from_path(path).file_tag
                processed_split_file_paths[file_tag].append(path)

        self.assertEqual(0, len(processed_split_file_paths.keys()))

        for path in controller.fs.all_paths:
            if not isinstance(path, GcsfsFilePath):
                continue
            self.assertTrue('file_split' not in path.file_name)
        expected_raw_metadata_tags_with_is_processed = [(tag, True) for tag in file_tags]
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_failing_to_process_a_file_that_needs_splitting_no_loop(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        self.assertIsInstance(
            controller.cloud_task_manager,
            FakeSynchronousDirectIngestCloudTaskManager,
            "Expected FakeSynchronousDirectIngestCloudTaskManager")
        task_manager = controller.cloud_task_manager

        # Set line limit to 1
        controller.ingest_file_split_line_limit = 1

        # This file exceeds the split limit, but since we add it with
        # fail_handle_file_call=True, it won't get picked up and split.
        file_path = path_for_fixture_file(
            controller,
            'tagC.csv',
            should_normalize=True,
            dt=datetime.datetime.fromisoformat('2019-09-19'))
        controller.fs.test_add_path(file_path, fail_handle_file_call=True)

        controller.kick_scheduler(just_finished_job=False)

        task_manager.test_run_next_scheduler_task()
        task_manager.test_pop_finished_scheduler_task()

        task_manager.test_run_next_process_job_task()
        task_manager.test_pop_finished_process_job_task()

        # The process job task, which will try to process a file that is too
        # big, will not schedule another job for the same file (which would
        # just get us in a loop).
        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(controller.region).size())
        self.assertEqual(
            0,
            task_manager.get_process_job_queue_info(controller.region).size())

        self.validate_file_metadata(controller)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_move_files_from_previous_days_to_storage(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)

        previous_date = '2019-09-15'
        file_path_from_prev_day = path_for_fixture_file(
            controller,
            'tagB.csv',
            should_normalize=True,
            dt=datetime.datetime.fromisoformat(previous_date))

        # pylint:disable=protected-access
        processed_file_from_prev_day = \
            controller.fs._to_processed_file_path(file_path_from_prev_day)

        unexpected_file_path_from_prev_day = path_for_fixture_file(
            controller,
            'Unexpected_Tag.csv',
            should_normalize=True,
            dt=datetime.datetime.fromisoformat(previous_date))

        controller.fs.test_add_path(processed_file_from_prev_day)
        controller.fs.test_add_path(unexpected_file_path_from_prev_day)

        file_tags = list(sorted(controller.get_file_tag_rank_list()))

        # This will test that all paths get moved to storage,
        # except the unexpected tag.
        add_paths_with_tags_and_process(self,
                                        controller,
                                        file_tags,
                                        unexpected_tags=['Unexpected_Tag'])

        paths_from_prev_date = []
        for path in controller.fs.all_paths:
            expected_storage_dir_str = os.path.join(
                controller.storage_directory_path.abs_path(), previous_date)
            if path.abs_path().startswith(expected_storage_dir_str):
                paths_from_prev_date.append(path)

        self.assertTrue(len(paths_from_prev_date), 1)
        self.assertTrue('tagB' in paths_from_prev_date[0].abs_path())
        self.validate_file_metadata(controller)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_BQ_IMPORT_ENABLED_REGION))
    def test_move_files_from_previous_days_to_storage_bq_import_enabled(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)

        prev_date_datetime = datetime.datetime.fromisoformat('2019-09-15')
        file_path_from_prev_day = path_for_fixture_file(
            controller,
            'tagB.csv',
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            dt=prev_date_datetime)

        # pylint:disable=protected-access
        processed_file_from_prev_day = \
            controller.fs._to_processed_file_path(file_path_from_prev_day)

        unexpected_file_path_from_prev_day = path_for_fixture_file(
            controller,
            'Unexpected_Tag.csv',
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
            dt=prev_date_datetime)

        controller.fs.test_add_path(processed_file_from_prev_day)
        controller.fs.test_add_path(unexpected_file_path_from_prev_day)

        file_tags = list(sorted(controller.get_file_tag_rank_list()))

        unexpected_tags = ['Unexpected_Tag']

        # This will test that all paths get moved to storage,
        # except the unexpected tag.
        add_paths_with_tags_and_process(self,
                                        controller,
                                        file_tags,
                                        unexpected_tags=unexpected_tags)

        paths_from_prev_date = []
        for path in controller.fs.all_paths:
            parts = filename_parts_from_path(path)
            expected_storage_dir_str = os.path.join(
                controller.storage_directory_path.abs_path(),
                parts.file_type.value,
                f'{prev_date_datetime.year:04}',
                f'{prev_date_datetime.month:02}',
                f'{prev_date_datetime.day:02}')
            if path.abs_path().startswith(expected_storage_dir_str):
                paths_from_prev_date.append(path)

        self.assertTrue(len(paths_from_prev_date), 1)
        self.assertTrue('tagB' in paths_from_prev_date[0].abs_path())
        expected_raw_metadata_tags_with_is_processed = [(tag, True) for tag in file_tags]
        expected_raw_metadata_tags_with_is_processed.extend([(tag, False) for tag in unexpected_tags])
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_SQL_PRE_PROCESSING_LAUNCHED_REGION))
    def test_move_files_from_previous_days_to_storage_sql_preprocessing_fully_launched(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)

        prev_date_datetime = datetime.datetime.fromisoformat('2019-09-15')
        file_path_from_prev_day = path_for_fixture_file(
            controller,
            'tagB.csv',
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            dt=prev_date_datetime)

        # pylint:disable=protected-access
        processed_file_from_prev_day = \
            controller.fs._to_processed_file_path(file_path_from_prev_day)

        unexpected_file_path_from_prev_day = path_for_fixture_file(
            controller,
            'Unexpected_Tag.csv',
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
            dt=prev_date_datetime)

        controller.fs.test_add_path(processed_file_from_prev_day)
        controller.fs.test_add_path(unexpected_file_path_from_prev_day)

        file_tags = list(sorted(controller.get_file_tag_rank_list()))

        unexpected_tags = ['Unexpected_Tag']

        # This will test that all paths get moved to storage,
        # except the unexpected tag.
        add_paths_with_tags_and_process(self,
                                        controller,
                                        file_tags,
                                        unexpected_tags=unexpected_tags)

        paths_from_prev_date = []
        for path in controller.fs.all_paths:
            parts = filename_parts_from_path(path)
            expected_storage_dir_str = os.path.join(
                controller.storage_directory_path.abs_path(),
                parts.file_type.value,
                f'{prev_date_datetime.year:04}',
                f'{prev_date_datetime.month:02}',
                f'{prev_date_datetime.day:02}')
            if path.abs_path().startswith(expected_storage_dir_str):
                paths_from_prev_date.append(path)

        self.assertTrue(len(paths_from_prev_date), 1)
        self.assertTrue('tagB' in paths_from_prev_date[0].abs_path())
        expected_raw_metadata_tags_with_is_processed = [(tag, True) for tag in file_tags]
        expected_raw_metadata_tags_with_is_processed.extend([(tag, False) for tag in unexpected_tags])
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=expected_raw_metadata_tags_with_is_processed,
            expected_ingest_metadata_tags_with_is_processed=[(tag, True) for tag in file_tags])

        self.assertIsInstance(controller.ingest_view_export_manager.big_query_client, FakeDirectIngestBigQueryClient)
        self.assertCountEqual(controller.get_file_tag_rank_list(),
                              controller.ingest_view_export_manager.big_query_client.exported_file_tags)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_move_files_from_previous_days_to_storage_incomplete_current_day(
            self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        prev_date_datetime = datetime.datetime.fromisoformat('2019-09-15')
        current_date_datetime = datetime.datetime.fromisoformat('2019-09-16')

        file_path_from_prev_day = path_for_fixture_file(
            controller,
            'tagB.csv',
            should_normalize=True,
            dt=prev_date_datetime)

        # pylint:disable=protected-access
        processed_file_from_prev_day = \
            controller.fs._to_processed_file_path(file_path_from_prev_day)

        unexpected_file_path_from_prev_day = path_for_fixture_file(
            controller,
            'Unexpected_Tag.csv',
            should_normalize=True,
            dt=prev_date_datetime)

        file_path_from_current_day = path_for_fixture_file(
            controller,
            'tagA.csv',
            should_normalize=True,
            dt=current_date_datetime)

        controller.fs.test_add_path(processed_file_from_prev_day)
        controller.fs.test_add_path(unexpected_file_path_from_prev_day)
        controller.fs.test_add_path(file_path_from_current_day)

        run_task_queues_to_empty(controller)

        self.assertTrue(len(controller.fs.all_paths), 3)

        storage_paths = []
        processed_paths = []
        for path in controller.fs.all_paths:
            if self._path_in_storage_dir(path, controller):
                if 'Unexpected_Tag' in path.abs_path():
                    self.fail('Unexpected tag found in storage dir')
                storage_paths.append(path)
            if controller.fs.is_processed_file(path):
                processed_paths.append(path)

        self.assertEqual(len(storage_paths), 1)

        expected_storage_dir_str = os.path.join(
            controller.storage_directory_path.abs_path(), prev_date_datetime.date().isoformat())
        self.assertTrue(
            storage_paths[0].abs_path().startswith(expected_storage_dir_str),
            f'Path {storage_paths[0].abs_path()} should start with {expected_storage_dir_str}.')

        # Path that is moved retains its 'processed_' prefix.
        self.assertEqual(len(processed_paths), 2)

        processed_paths_not_in_storage = \
            [path
             for path in processed_paths
             if not self._path_in_storage_dir(path, controller)]

        self.assertEqual(len(processed_paths_not_in_storage), 1)

        processed_path_str = processed_paths_not_in_storage[0].abs_path()
        self.assertTrue(processed_path_str.startswith(
            controller.ingest_directory_path.abs_path()))
        self.assertTrue('tagA' in processed_path_str)
        self.validate_file_metadata(controller)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_BQ_IMPORT_ENABLED_REGION))
    def test_move_files_from_previous_days_to_storage_incomplete_current_day_bq_import_enabled(
            self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        prev_date_datetime = datetime.datetime.fromisoformat('2019-09-15')
        current_date_datetime = datetime.datetime.fromisoformat('2019-09-16')

        file_path_from_prev_day = path_for_fixture_file(
            controller,
            'tagB.csv',
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            dt=prev_date_datetime)

        # pylint:disable=protected-access
        processed_file_from_prev_day = \
            controller.fs._to_processed_file_path(file_path_from_prev_day)

        unexpected_file_path_from_prev_day = path_for_fixture_file(
            controller,
            'Unexpected_Tag.csv',
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
            dt=prev_date_datetime)

        file_path_from_current_day = path_for_fixture_file(
            controller,
            'tagA.csv',
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            dt=current_date_datetime)

        controller.fs.test_add_path(processed_file_from_prev_day)
        controller.fs.test_add_path(unexpected_file_path_from_prev_day)
        controller.fs.test_add_path(file_path_from_current_day)

        run_task_queues_to_empty(controller)

        self.assertTrue(len(controller.fs.all_paths), 3)

        storage_paths = []
        processed_paths = []
        for path in controller.fs.all_paths:
            if self._path_in_storage_dir(path, controller):
                if 'Unexpected_Tag' in path.abs_path():
                    self.fail('Unexpected tag found in storage dir')
                storage_paths.append(path)
            if controller.fs.is_processed_file(path):
                processed_paths.append(path)

        self.assertEqual(len(storage_paths), 1)

        expected_storage_dir_str = os.path.join(
            controller.storage_directory_path.abs_path(),
            GcsfsDirectIngestFileType.INGEST_VIEW.value,
            f'{prev_date_datetime.year:04}',
            f'{prev_date_datetime.month:02}',
            f'{prev_date_datetime.day:02}')
        self.assertTrue(
            storage_paths[0].abs_path().startswith(expected_storage_dir_str),
            f'Path {storage_paths[0].abs_path()} should start with {expected_storage_dir_str}.')

        # Path that is moved retains its 'processed_' prefix.
        self.assertEqual(len(processed_paths), 2)

        processed_paths_not_in_storage = \
            [path
             for path in processed_paths
             if not self._path_in_storage_dir(path, controller)]

        self.assertEqual(len(processed_paths_not_in_storage), 1)

        processed_path_str = processed_paths_not_in_storage[0].abs_path()
        self.assertTrue(processed_path_str.startswith(
            controller.ingest_directory_path.abs_path()))
        self.assertTrue('tagA' in processed_path_str)
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[('Unexpected_Tag', False)])

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_SQL_PRE_PROCESSING_LAUNCHED_REGION))
    def test_move_files_from_previous_days_to_storage_incomplete_current_day_sql_preprocessing_fully_lauched(
            self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False,
            max_delay_sec_between_files=1)

        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        current_date_datetime = datetime.datetime.utcnow()
        prev_date_datetime = current_date_datetime - datetime.timedelta(days=1)

        with freeze_time(prev_date_datetime.isoformat()):
            unexpected_file_path_from_prev_day = path_for_fixture_file(
                controller,
                'Unexpected_Tag.csv',
                should_normalize=True,
                file_type=GcsfsDirectIngestFileType.RAW_DATA)

            processed_file_from_prev_day = path_for_fixture_file(
                controller,
                'tagB.csv',
                should_normalize=True,
                file_type=GcsfsDirectIngestFileType.RAW_DATA)

            controller.fs.test_add_path(unexpected_file_path_from_prev_day)
            controller.fs.test_add_path(processed_file_from_prev_day)

        run_task_queues_to_empty(controller)

        file_path_from_current_day = path_for_fixture_file(
            controller,
            'tagA.csv',
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA)

        controller.fs.test_add_path(file_path_from_current_day)

        run_task_queues_to_empty(controller)

        self.assertTrue(len(controller.fs.all_paths), 3)

        storage_paths = []
        processed_paths = []
        for path in controller.fs.all_paths:
            if self._path_in_storage_dir(path, controller):
                if 'Unexpected_Tag' in path.abs_path():
                    self.fail('Unexpected tag found in storage dir')
                storage_paths.append(path)
            if controller.fs.is_processed_file(path):
                processed_paths.append(path)

        self.assertEqual(len(storage_paths), 3)

        expected_raw_storage_dir_prefix = os.path.join(
            controller.storage_directory_path.abs_path(),
            GcsfsDirectIngestFileType.RAW_DATA.value)

        expected_ingest_storage_dir_prefix = os.path.join(
            controller.storage_directory_path.abs_path(),
            GcsfsDirectIngestFileType.INGEST_VIEW.value)
        self.assertTrue(
            storage_paths[0].abs_path().startswith(expected_raw_storage_dir_prefix) or
            storage_paths[0].abs_path().startswith(expected_ingest_storage_dir_prefix),
            f'Path {storage_paths[0].abs_path()} should start with {expected_raw_storage_dir_prefix} or '
            f'{expected_ingest_storage_dir_prefix}.')

        # Paths that are moved retain their 'processed_' prefix.
        self.assertEqual(len(processed_paths), 4)

        processed_paths_not_in_storage = \
            [path
             for path in processed_paths
             if not self._path_in_storage_dir(path, controller)]

        self.assertEqual(len(processed_paths_not_in_storage), 1)

        processed_path_str = processed_paths_not_in_storage[0].abs_path()
        self.assertTrue(processed_path_str.startswith(
            controller.ingest_directory_path.abs_path()))
        self.assertTrue('tagA' in processed_path_str)
        self.validate_file_metadata(
            controller,
            expected_raw_metadata_tags_with_is_processed=[('Unexpected_Tag', False), ('tagA', True), ('tagB', True)],
            expected_ingest_metadata_tags_with_is_processed=[('tagA', True), ('tagB', True)])

        self.assertIsInstance(controller.ingest_view_export_manager.big_query_client, FakeDirectIngestBigQueryClient)
        self.assertCountEqual(['tagA', 'tagB'],
                              controller.ingest_view_export_manager.big_query_client.exported_file_tags)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_cloud_function_fails_on_new_file(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        self.assertIsInstance(
            controller.cloud_task_manager,
            FakeSynchronousDirectIngestCloudTaskManager,
            "Expected FakeSynchronousDirectIngestCloudTaskManager")
        task_manager = controller.cloud_task_manager

        file_path = \
            path_for_fixture_file(controller, 'tagA.csv',
                                  should_normalize=False)
        file_path2 = \
            path_for_fixture_file(controller, 'tagB.csv',
                                  should_normalize=False)
        file_path3 = \
            path_for_fixture_file(controller, 'tagC.csv',
                                  should_normalize=False)

        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        # Upload two new files without triggering the controller
        controller.fs.test_add_path(file_path, fail_handle_file_call=True)
        controller.fs.test_add_path(file_path2, fail_handle_file_call=True)

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(controller.region).size())
        self.assertEqual(
            0,
            task_manager.get_process_job_queue_info(controller.region).size())

        # Later file that succeeds will trigger proper upload of all files
        controller.fs.test_add_path(file_path3)

        run_task_queues_to_empty(controller)
        check_all_paths_processed(self,
                                  controller,
                                  ['tagA', 'tagB', 'tagC'],
                                  unexpected_tags=[])
        self.validate_file_metadata(controller)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_cloud_function_fails_on_new_file_rename_later_with_cron(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        self.assertIsInstance(
            controller.cloud_task_manager,
            FakeSynchronousDirectIngestCloudTaskManager,
            "Expected FakeSynchronousDirectIngestCloudTaskManager")
        task_manager = controller.cloud_task_manager

        file_path = \
            path_for_fixture_file(controller, 'tagA.csv',
                                  should_normalize=False)
        file_path2 = \
            path_for_fixture_file(controller, 'tagB.csv',
                                  should_normalize=False)
        file_path3 = \
            path_for_fixture_file(controller, 'tagC.csv',
                                  should_normalize=False)

        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        # Upload new files without triggering the controller
        controller.fs.test_add_path(file_path, fail_handle_file_call=True)
        controller.fs.test_add_path(file_path2, fail_handle_file_call=True)
        controller.fs.test_add_path(file_path3, fail_handle_file_call=True)

        self.assertEqual(
            0,
            task_manager.get_scheduler_queue_info(controller.region).size())
        self.assertEqual(
            0,
            task_manager.get_process_job_queue_info(controller.region).size())

        for path in controller.fs.all_paths:
            self.assertFalse(controller.fs.is_normalized_file_path(path))

        # Cron job to handle unseen files triggers later
        controller.cloud_task_manager. \
            create_direct_ingest_handle_new_files_task(
                controller.region, can_start_ingest=False)

        run_task_queues_to_empty(controller)

        for path in controller.fs.all_paths:
            self.assertTrue(controller.fs.is_normalized_file_path(path))

        self.validate_file_metadata(controller)

    @patch("recidiviz.utils.regions.get_region", Mock(return_value=TEST_STATE_REGION))
    def test_processing_continues_if_there_are_subfolders_in_ingest_dir(self):
        controller = build_gcsfs_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)

        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        subdir_path = \
            path_for_fixture_file(controller, 'subdir/',
                                  should_normalize=False)
        paths = [
            subdir_path,
            path_for_fixture_file(controller, 'subdir/Unexpected_Tag.csv',
                                  should_normalize=False),
            path_for_fixture_file(controller, 'tagA.csv',
                                  should_normalize=False),
            path_for_fixture_file(controller, 'tagB.csv',
                                  should_normalize=False),
            path_for_fixture_file(controller, 'tagC.csv',
                                  should_normalize=False),
            path_for_fixture_file(controller, 'subdir/tagC_2.csv',
                                  should_normalize=False),
        ]

        for path in paths:
            controller.fs.test_add_path(path)

        run_task_queues_to_empty(controller)

        dir_paths_found = []
        storage_file_paths = []
        ingest_file_paths = []

        for path in controller.fs.all_paths:
            if isinstance(path, GcsfsDirectoryPath):
                dir_paths_found.append(path)
                continue

            if path.abs_path().startswith(
                    controller.storage_directory_path.abs_path()):
                storage_file_paths.append(path)
            else:
                self.assertTrue(path.abs_path().startswith(
                    controller.ingest_directory_path.abs_path()))
                ingest_file_paths.append(path)

        self.assertEqual(1, len(dir_paths_found))
        self.assertEqual(subdir_path, dir_paths_found[0])

        self.assertEqual(3, len(storage_file_paths))
        storage_tags = {filename_parts_from_path(path).file_tag
                        for path in storage_file_paths}
        self.assertEqual({'tagA', 'tagB', 'tagC'}, storage_tags)

        for path in storage_file_paths:
            self.assertTrue(controller.fs.is_normalized_file_path(path))
            self.assertTrue(controller.fs.is_processed_file(path))

        self.assertEqual(2, len(ingest_file_paths))
        ingest_tags = {filename_parts_from_path(path).file_tag
                       for path in ingest_file_paths}
        self.assertEqual({'tagC', 'Unexpected_Tag'}, ingest_tags)

        for path in ingest_file_paths:
            self.assertTrue(controller.fs.is_normalized_file_path(path))
            self.assertTrue(controller.fs.is_seen_unprocessed_file(path))
            self.assertEqual(subdir_path,
                             GcsfsDirectoryPath.from_file_path(path))

        self.validate_file_metadata(controller)

    def test_serialize_gcsfs_ingest_args(self):
        now = datetime.datetime.now()

        str_now = datetime_to_serializable(now)
        now_converted = serializable_to_datetime(str_now)

        self.assertTrue(now, now_converted)

        args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=GcsfsFilePath.from_absolute_path('foo/bar.csv'),
        )

        args_dict = attr_to_json_dict(args)
        serialized = json.dumps(args_dict).encode()
        args_dict = json.loads(serialized)
        result_args = attr_from_json_dict(args_dict)
        self.assertEqual(args, result_args)
