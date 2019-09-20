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
"""Helpers for direct ingest tests."""
import datetime
import os
import threading
import time
import unittest
from typing import Set, List, Union, Optional, Dict

from mock import Mock, patch

from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    DirectIngestGCSFileSystem, to_normalized_unprocessed_file_path
from recidiviz.ingest.direct.controllers.gcsfs_path import \
    GcsfsFilePath, GcsfsBucketPath, GcsfsDirectoryPath, GcsfsPath
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    filename_parts_from_path, GcsfsIngestArgs
from recidiviz.ingest.direct.controllers.gcsfs_factory import GcsfsFactory
from recidiviz.tests.ingest.direct.fake_async_direct_ingest_cloud_task_manager \
    import FakeAsyncDirectIngestCloudTaskManager
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.direct.\
    fake_synchronous_direct_ingest_cloud_task_manager import \
    FakeSynchronousDirectIngestCloudTaskManager


class FakeDirectIngestGCSFileSystem(DirectIngestGCSFileSystem):
    """Test-only implementation of the DirectIngestGCSFileSystem."""
    def __init__(self):
        self.mutex = threading.Lock()
        self.all_paths: Set[GcsfsFilePath] = set()
        self.uploaded_contents: Dict[str, str] = {}
        self.controller: Optional[GcsfsDirectIngestController] = None

    def test_set_controller(self, controller: GcsfsDirectIngestController):
        self.controller = controller

    def test_add_path(self, path: GcsfsFilePath):
        if not isinstance(path, GcsfsFilePath):
            raise ValueError(f'Path has unexpected type {type(path)}')
        self._add_path(path)

    def _add_path(self, path: GcsfsFilePath):
        with self.mutex:
            self.all_paths.add(path)

        if self.controller and \
                path.abs_path().startswith(
                        self.controller.ingest_directory_path.abs_path()):
            self.controller.handle_file(path, start_ingest=True)

    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        with self.mutex:
            return path.abs_path() in [p.abs_path() for p in self.all_paths]

    def download_as_string(self, path: GcsfsFilePath) -> bytes:
        if not self.exists(path):
            raise ValueError(f'Path [{path.abs_path()}] does not exist.')

        if path.abs_path() in self.uploaded_contents:
            return bytes(self.uploaded_contents[path.abs_path()], 'utf-8')

        directory_path, _ = os.path.split(path.abs_path())

        parts = filename_parts_from_path(path)
        suffix = parts.filename_suffix if parts.filename_suffix else ''
        fixture_filename = \
            f'{parts.file_tag}{suffix}.{parts.extension}'

        actual_fixture_file_path = os.path.join(directory_path,
                                                fixture_filename)

        fixture_contents = fixtures.as_string_from_relative_path(
            actual_fixture_file_path)
        return bytes(fixture_contents, 'utf-8')

    def upload_from_string(self,
                           path: GcsfsFilePath,
                           contents: str,
                           content_type: str):
        self.uploaded_contents[path.abs_path()] = contents
        self._add_path(path)

    def copy(self,
             src_path: GcsfsFilePath,
             dst_path: GcsfsPath) -> None:

        if isinstance(dst_path, GcsfsFilePath):
            path = dst_path
        elif isinstance(dst_path, GcsfsDirectoryPath):
            path = \
                GcsfsFilePath.from_directory_and_file_name(dst_path,
                                                           src_path.file_name)
        else:
            raise ValueError(f'Unexpected path type [{type(dst_path)}]')

        self._add_path(path)

    def delete(self, path: GcsfsFilePath) -> None:
        with self.mutex:
            path_to_remove = None
            for p in self.all_paths:
                if p == path:
                    path_to_remove = p
                    break

            if path_to_remove is not None:
                self.all_paths.remove(path_to_remove)

    def _ls_with_blob_prefix(self,
                             bucket_name: str,
                             blob_prefix: str) -> List[GcsfsFilePath]:
        with self.mutex:
            return [path for path in self.all_paths
                    if path.bucket_name == bucket_name
                    and path.blob_name
                    and path.blob_name.startswith(blob_prefix)]


def build_controller_for_tests(controller_cls,
                               run_async: bool) -> BaseDirectIngestController:
    if issubclass(controller_cls, GcsfsDirectIngestController):
        raise ValueError(f"Controller class {controller_cls} is instance of "
                         f"GcsfsDirectIngestController - use "
                         f"build_gcsfs_controller_for_tests instead.")

    with patch(
            'recidiviz.ingest.direct.controllers.'
            'base_direct_ingest_controller.DirectIngestCloudTaskManagerImpl') \
            as mock_task_factory_cls:
        task_manager = FakeAsyncDirectIngestCloudTaskManager() \
            if run_async else FakeSynchronousDirectIngestCloudTaskManager()
        mock_task_factory_cls.return_value = task_manager
        return controller_cls()

@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
def build_gcsfs_controller_for_tests(
        controller_cls,
        fixture_path_prefix: str,
        run_async: bool
) -> GcsfsDirectIngestController:

    fake_fs = FakeDirectIngestGCSFileSystem()

    def mock_build_fs():
        return fake_fs

    with patch(
            'recidiviz.ingest.direct.controllers.'
            'base_direct_ingest_controller.DirectIngestCloudTaskManagerImpl') \
            as mock_task_factory_cls:
        task_manager = FakeAsyncDirectIngestCloudTaskManager() \
            if run_async else FakeSynchronousDirectIngestCloudTaskManager()
        mock_task_factory_cls.return_value = task_manager
        with patch.object(GcsfsFactory, 'build', new=mock_build_fs):
            controller = controller_cls(
                ingest_directory_path=f'{fixture_path_prefix}/fixtures',
                storage_directory_path='storage/path')
            task_manager.set_controller(controller)
            fake_fs.test_set_controller(controller)
            return controller


def ingest_args_for_fixture_file(
        controller: GcsfsDirectIngestController,
        filename: str,
        should_normalize: bool = True) -> GcsfsIngestArgs:
    file_path = path_for_fixture_file(controller, filename, should_normalize)
    return GcsfsIngestArgs(
        ingest_time=datetime.datetime.now(),
        file_path=file_path,
    )


def path_for_fixture_file(controller: GcsfsDirectIngestController,
                          filename: str,
                          should_normalize: bool,
                          dt: Optional[datetime.datetime] = None):
    file_path_str = os.path.join(
        controller.ingest_directory_path.abs_path(), filename)

    if should_normalize:
        file_path_str = to_normalized_unprocessed_file_path(file_path_str, dt)
    return GcsfsFilePath.from_absolute_path(file_path_str)


def add_paths_with_tags_and_process(test_case: unittest.TestCase,
                                    controller: GcsfsDirectIngestController,
                                    file_tags: List[str],
                                    unexpected_tags: List[str] = None,
                                    pre_normalize_filename: bool = False):
    """Runs a test that queues files for all the provided file tags, waits
    for the controller to finish processing everything, then makes sure that
    all files not in |unexpected_tags| have been moved to storage.
    """
    if unexpected_tags is None:
        unexpected_tags = []

    if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
        raise ValueError(f"Controller fs must have type "
                         f"FakeDirectIngestGCSFileSystem. Found instead "
                         f"type [{type(controller.fs)}]")
    for file_tag in file_tags:

        file_path = path_for_fixture_file(
            controller,
            f'{file_tag}.csv',
            should_normalize=pre_normalize_filename)
        controller.fs.test_add_path(file_path)
        time.sleep(.05)

    run_task_queues_to_empty(controller)

    file_tags_processed = set()
    for path in controller.fs.all_paths:
        file_tag = filename_parts_from_path(path).file_tag

        if file_tag not in unexpected_tags:
            # Test all expected files have been moved to storage
            test_case.assertTrue(
                path.abs_path().startswith(
                    controller.storage_directory_path.abs_path()),
                f'{path} does not start with expected prefix')

            file_tags_processed.add(filename_parts_from_path(path).file_tag)
        else:
            test_case.assertTrue(path.file_name.startswith('unprocessed'))

    # Test that each expected file tag has been processed
    test_case.assertEqual(file_tags_processed,
                          set(file_tags).difference(set(unexpected_tags)))


def run_task_queues_to_empty(controller: GcsfsDirectIngestController):
    if isinstance(controller.cloud_task_manager,
                  FakeAsyncDirectIngestCloudTaskManager):
        controller.cloud_task_manager.wait_for_all_tasks_to_run()
    elif isinstance(controller.cloud_task_manager,
                    FakeSynchronousDirectIngestCloudTaskManager):
        tm = controller.cloud_task_manager
        while tm.get_scheduler_queue_info(controller.region).size() \
                or tm.get_process_job_queue_info(controller.region).size():
            if tm.get_scheduler_queue_info(controller.region).size():
                tm.test_run_next_scheduler_task()
                tm.test_pop_finished_scheduler_task()
            if tm.get_process_job_queue_info(controller.region).size():
                tm.test_run_next_process_job_task()
                tm.test_pop_finished_process_job_task()
    else:
        raise ValueError(f"Unexpected type for cloud task manager: "
                         f"[{type(controller.cloud_task_manager)}]")
