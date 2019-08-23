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
from typing import Set, List

from gcsfs.core import GCSFile
from mock import Mock, patch, create_autospec

from recidiviz.cloud_functions.cloud_function_utils import \
    to_normalized_unprocessed_file_path
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    DirectIngestGCSFileSystem
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
        self.all_paths: Set[str] = set()

    def test_add_path(self, path: str):
        with self.mutex:
            self.all_paths.add(path)

    def exists(self, path: str) -> bool:
        with self.mutex:
            return path in self.all_paths

    def open(self, path: str) -> GCSFile:
        directory_path, _ = os.path.split(path)

        parts = filename_parts_from_path(path)
        fixture_filename = f'{parts.file_tag}.{parts.extension}'

        actual_fixture_file_path = os.path.join(directory_path,
                                                fixture_filename)

        fixture_contents = fixtures.as_string_from_relative_path(
            actual_fixture_file_path)

        mock_gcs_file = create_autospec(GCSFile)
        mock_gcs_file.read.return_value = bytes(fixture_contents, 'utf-8')
        mock_gcs_file.__enter__ = Mock(return_value=mock_gcs_file)
        mock_gcs_file.__exit__ = Mock(return_value=False)
        return mock_gcs_file

    def mv(self, path1: str, path2: str) -> None:
        with self.mutex:
            self.all_paths.remove(path1)
            self.all_paths.add(path2)

    def _ls_with_file_prefix(self,
                             directory_path: str,
                             file_prefix: str) -> List[str]:
        with self.mutex:
            path_file_name_tuples = \
                [(path, os.path.split(path)[1])
                 for path in self.all_paths if path.startswith(directory_path)]
            return [path for path, filename in path_file_name_tuples
                    if filename.startswith(file_prefix)]


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
def build_controller_for_tests(
        controller_cls,
        fixture_path_prefix: str,
        run_async: bool
) -> GcsfsDirectIngestController:

    def mock_build_fs():
        return FakeDirectIngestGCSFileSystem()

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
            return controller


def ingest_args_for_fixture_file(controller: GcsfsDirectIngestController,
                                 filename: str) -> GcsfsIngestArgs:
    original_path = os.path.join(controller.ingest_directory_path, filename)
    file_path = to_normalized_unprocessed_file_path(original_path)
    return GcsfsIngestArgs(
        ingest_time=datetime.datetime.now(),
        file_path=file_path,
    )


def add_paths_with_tags_and_process(test_case: unittest.TestCase,
                                    controller: GcsfsDirectIngestController,
                                    file_tags: List[str],
                                    unexpected_tags: List[str] = None):
    """Runs a test that queues files for all the provided file tags, waits
    for the controller to finish processing everything, then makes sure that
    all files not in |unexpected_tags| have been moved to storage.
    """
    if unexpected_tags is None:
        unexpected_tags = []

    for file_tag in file_tags:
        args = ingest_args_for_fixture_file(controller,
                                            f'{file_tag}.csv')
        if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(controller.fs)}]")

        controller.fs.test_add_path(args.file_path)

        controller.kick_scheduler(just_finished_job=False)
        time.sleep(.05)

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

    file_tags_processed = set()
    for path in controller.fs.all_paths:
        file_tag = filename_parts_from_path(path).file_tag

        if file_tag not in unexpected_tags:
            # Test all expected files have been moved to storage
            test_case.assertTrue(
                path.startswith(controller.storage_directory_path),
                f'{path} does not start with expected prefix')

            file_tags_processed.add(filename_parts_from_path(path).file_tag)
        else:
            _, file_name = os.path.split(path)
            test_case.assertTrue(file_name.startswith('unprocessed'))

    # Test that each expected file tag has been processed
    test_case.assertEqual(file_tags_processed,
                          set(file_tags).difference(set(unexpected_tags)))
