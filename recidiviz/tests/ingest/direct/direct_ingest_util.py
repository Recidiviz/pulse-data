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
import time
import unittest
from typing import List, Union, Optional, Dict, Type

import attr
from mock import Mock, patch

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import DirectIngestPreProcessedIngestView
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    DirectIngestGCSFileSystem, to_normalized_unprocessed_file_path
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import \
    DirectIngestRawFileImportManager, DirectIngestRawFileConfig, DirectIngestRegionRawFileConfig
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import DirectIngestPreProcessedIngestViewCollector
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    filename_parts_from_path, GcsfsIngestArgs, GcsfsDirectIngestFileType
from recidiviz.ingest.direct.controllers.gcsfs_factory import GcsfsFactory
from recidiviz.ingest.direct.controllers.gcsfs_path import \
    GcsfsFilePath, GcsfsDirectoryPath, GcsfsPath
from recidiviz.persistence.entity.operations.entities import DirectIngestFileMetadata
from recidiviz.tests.ingest.direct.fake_direct_ingest_big_query_client import FakeDirectIngestBigQueryClient
from recidiviz.tests.ingest.direct.fake_async_direct_ingest_cloud_task_manager \
    import FakeAsyncDirectIngestCloudTaskManager
from recidiviz.tests.ingest.direct.fake_direct_ingest_gcs_file_system import FakeDirectIngestGCSFileSystem
from recidiviz.tests.ingest.direct. \
    fake_synchronous_direct_ingest_cloud_task_manager import \
    FakeSynchronousDirectIngestCloudTaskManager
from recidiviz.utils import metadata
from recidiviz.utils.regions import Region


@attr.s
class FakeDirectIngestRegionRawFileConfig(DirectIngestRegionRawFileConfig):

    def _get_raw_data_file_configs(self) -> Dict[str, DirectIngestRawFileConfig]:
        return {
            'tagA': DirectIngestRawFileConfig(
                file_tag='tagA',
                primary_key_cols=[],
                datetime_cols=[],
                encoding='UTF-8',
                separator=',',
                ignore_quotes=False,
            ),
            'tagB': DirectIngestRawFileConfig(
                file_tag='tagB',
                primary_key_cols=[],
                datetime_cols=[],
                encoding='UTF-8',
                separator=',',
                ignore_quotes=False,
            ),
            'tagC': DirectIngestRawFileConfig(
                file_tag='tagC',
                primary_key_cols=[],
                datetime_cols=[],
                encoding='UTF-8',
                separator=',',
                ignore_quotes=False,
            ),
            'tagWeDoNotIngest': DirectIngestRawFileConfig(
                file_tag='tagWeDoNotIngest',
                primary_key_cols=[],
                datetime_cols=[],
                encoding='UTF-8',
                separator=',',
                ignore_quotes=False,
            )
        }


class FakeDirectIngestRawFileImportManager(DirectIngestRawFileImportManager):
    """Fake implementation of DirectIngestRawFileImportManager for tests."""

    def __init__(self,
                 *,
                 region: Region,
                 fs: DirectIngestGCSFileSystem,
                 ingest_directory_path: GcsfsDirectoryPath,
                 temp_output_directory_path: GcsfsDirectoryPath,
                 big_query_client: BigQueryClient):

        super().__init__(region=region,
                         fs=fs,
                         ingest_directory_path=ingest_directory_path,
                         temp_output_directory_path=temp_output_directory_path,
                         big_query_client=big_query_client,
                         region_raw_file_config=FakeDirectIngestRegionRawFileConfig(region.region_code))
        self.imported_paths: List[GcsfsFilePath] = []

    def import_raw_file_to_big_query(self,
                                     path: GcsfsFilePath,
                                     file_metadata: DirectIngestFileMetadata) -> None:
        self.imported_paths.append(path)


class FakeDirectIngestPreProcessedIngestViewCollector(BigQueryViewCollector[DirectIngestPreProcessedIngestView]):
    def __init__(self,
                 region: Region,
                 controller_file_tags: List[str]):
        self.region = region
        self.controller_file_tags = controller_file_tags

    def collect_views(self) -> List[DirectIngestPreProcessedIngestView]:
        views = [
            DirectIngestPreProcessedIngestView(
                ingest_view_name=tag,
                view_query_template=('SELECT * FROM {' + tag + '}'),
                region_raw_table_config=FakeDirectIngestRegionRawFileConfig(region_code=self.region.region_code)
            )
            for tag in self.controller_file_tags
        ]

        return views

def build_controller_for_tests(controller_cls,
                               run_async: bool) -> BaseDirectIngestController:
    """Builds an instance of |controller_cls| for use in tests with several internal classes mocked properly. If
    |controller_cls| is an instance of GcsfsDirectIngestController, use build_gcsfs_controller_for_tests() instead.
    """
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
        run_async: bool,
        **kwargs,
) -> GcsfsDirectIngestController:
    """Builds an instance of |controller_cls| for use in tests with several internal classes mocked properly. """
    fake_fs = FakeDirectIngestGCSFileSystem()

    def mock_build_fs():
        return fake_fs

    if 'TestGcsfsDirectIngestController' in controller_cls.__name__:
        view_collector_cls: Type[BigQueryViewCollector] = \
            FakeDirectIngestPreProcessedIngestViewCollector
    else:
        view_collector_cls = DirectIngestPreProcessedIngestViewCollector

    with patch(f'{BaseDirectIngestController.__module__}.DirectIngestCloudTaskManagerImpl') as mock_task_factory_cls:
        with patch(f'{GcsfsDirectIngestController.__module__}.BigQueryClientImpl') as mock_big_query_client_cls:
            with patch(f'{GcsfsDirectIngestController.__module__}.DirectIngestRawFileImportManager',
                       FakeDirectIngestRawFileImportManager):
                with patch(f'{GcsfsDirectIngestController.__module__}.DirectIngestPreProcessedIngestViewCollector',
                           view_collector_cls):
                    task_manager = FakeAsyncDirectIngestCloudTaskManager() \
                        if run_async else FakeSynchronousDirectIngestCloudTaskManager()
                    mock_task_factory_cls.return_value = task_manager
                    mock_big_query_client_cls.return_value = \
                        FakeDirectIngestBigQueryClient(project_id=metadata.project_id(), fs=fake_fs)
                    with patch.object(GcsfsFactory, 'build', new=mock_build_fs):
                        controller = controller_cls(
                            ingest_directory_path=f'{fixture_path_prefix}/fixtures',
                            storage_directory_path='storage/path',
                            **kwargs)
                        task_manager.set_controller(controller)
                        fake_fs.test_set_controller(controller)
                        return controller


def ingest_args_for_fixture_file(
        controller: GcsfsDirectIngestController,
        filename: str,
        should_normalize: bool = True) -> GcsfsIngestArgs:
    file_path = path_for_fixture_file(controller,
                                      filename,
                                      should_normalize,
                                      file_type=GcsfsDirectIngestFileType.INGEST_VIEW)
    if not isinstance(file_path, GcsfsFilePath):
        raise ValueError(f'Unexpected type [{file_path}]')
    return GcsfsIngestArgs(
        ingest_time=datetime.datetime.now(),
        file_path=file_path,
    )


def path_for_fixture_file(
        controller: GcsfsDirectIngestController,
        filename: str,
        should_normalize: bool,
        file_type: Optional[GcsfsDirectIngestFileType] = None,
        dt: Optional[datetime.datetime] = None
) -> Union[GcsfsFilePath, GcsfsDirectoryPath]:
    return path_for_fixture_file_in_test_gcs_directory(
        directory=controller.ingest_directory_path,
        filename=filename,
        should_normalize=should_normalize,
        file_type=file_type,
        dt=dt)


def path_for_fixture_file_in_test_gcs_directory(
        *,
        directory: GcsfsDirectoryPath,
        filename: str,
        should_normalize: bool,
        file_type: Optional[GcsfsDirectIngestFileType] = None,
        dt: Optional[datetime.datetime] = None
) -> Union[GcsfsFilePath, GcsfsDirectoryPath]:
    file_path_str = filename

    if should_normalize:
        if not file_type:
            file_type = GcsfsDirectIngestFileType.UNSPECIFIED

        file_path_str = to_normalized_unprocessed_file_path(original_file_path=file_path_str,
                                                            file_type=file_type,
                                                            dt=dt)

    return GcsfsPath.from_bucket_and_blob_name(
        bucket_name=directory.bucket_name,
        blob_name=os.path.join(directory.relative_path, file_path_str))


def add_paths_with_tags_and_process(test_case: unittest.TestCase,
                                    controller: GcsfsDirectIngestController,
                                    file_tags: List[str],
                                    unexpected_tags: List[str] = None,
                                    pre_normalize_filename: bool = False,
                                    file_type=GcsfsDirectIngestFileType.UNSPECIFIED):
    """Runs a test that queues files for all the provided file tags, waits
    for the controller to finish processing everything, then makes sure that
    all files not in |unexpected_tags| have been moved to storage.
    """
    add_paths_with_tags(controller, file_tags, pre_normalize_filename, file_type)
    process_task_queues(test_case, controller, file_tags, unexpected_tags)


def add_paths_with_tags(controller: GcsfsDirectIngestController,
                        file_tags: List[str],
                        pre_normalize_filename: bool = False,
                        file_type=GcsfsDirectIngestFileType.UNSPECIFIED):
    if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
        raise ValueError(f"Controller fs must have type "
                         f"FakeDirectIngestGCSFileSystem. Found instead "
                         f"type [{type(controller.fs)}]")

    for file_tag in file_tags:
        file_path = path_for_fixture_file(
            controller,
            f'{file_tag}.csv',
            should_normalize=pre_normalize_filename,
            file_type=file_type)
        controller.fs.test_add_path(file_path)
        time.sleep(.05)


def process_task_queues(test_case: unittest.TestCase,
                        controller: GcsfsDirectIngestController,
                        file_tags: List[str],
                        unexpected_tags: List[str] = None):
    if unexpected_tags is None:
        unexpected_tags = []

    run_task_queues_to_empty(controller)
    check_all_paths_processed(test_case, controller, file_tags, unexpected_tags)


def check_all_paths_processed(
        test_case: unittest.TestCase,
        controller: GcsfsDirectIngestController,
        file_tags: List[str],
        unexpected_tags: List[str]):
    """Checks that all non-directory paths with expected tags have been
    processed and moved to storage.
    """

    if not isinstance(controller.fs, FakeDirectIngestGCSFileSystem):
        raise ValueError(f"Controller fs must have type "
                         f"FakeDirectIngestGCSFileSystem. Found instead "
                         f"type [{type(controller.fs)}]")

    file_tags_processed = set()
    for path in controller.fs.all_paths:
        if isinstance(path, GcsfsDirectoryPath):
            continue

        file_tag = filename_parts_from_path(path).file_tag

        if file_tag not in unexpected_tags:
            # Test all expected files have been moved to storage
            test_case.assertTrue(
                path.abs_path().startswith(
                    controller.storage_directory_path.abs_path()),
                f'{path} has not been moved to correct storage directory')

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
                or tm.get_process_job_queue_info(controller.region).size()\
                or tm.get_bq_import_export_queue_info(controller.region).size():
            if tm.get_bq_import_export_queue_info(controller.region).size():
                tm.test_run_next_bq_import_export_task()
                tm.test_pop_finished_bq_import_export_task()
            if tm.get_scheduler_queue_info(controller.region).size():
                tm.test_run_next_scheduler_task()
                tm.test_pop_finished_scheduler_task()
            if tm.get_process_job_queue_info(controller.region).size():
                tm.test_run_next_process_job_task()
                tm.test_pop_finished_process_job_task()
    else:
        raise ValueError(f"Unexpected type for cloud task manager: "
                         f"[{type(controller.cloud_task_manager)}]")
