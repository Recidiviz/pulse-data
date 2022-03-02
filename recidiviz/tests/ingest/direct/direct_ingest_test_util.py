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
from typing import Optional

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_path,
)
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.types.cloud_task_args import (
    ExtractAndMergeArgs,
    LegacyExtractAndMergeArgs,
)
from recidiviz.tests.ingest.direct.fakes.fake_async_direct_ingest_cloud_task_manager import (
    FakeAsyncDirectIngestCloudTaskManager,
)
from recidiviz.tests.ingest.direct.fakes.fake_synchronous_direct_ingest_cloud_task_manager import (
    FakeSynchronousDirectIngestCloudTaskManager,
)


def ingest_args_for_fixture_file(
    controller: BaseDirectIngestController,
    filename: str,
    should_normalize: bool = True,
) -> ExtractAndMergeArgs:
    file_path = path_for_fixture_file(
        controller,
        filename,
        should_normalize,
        file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
    )
    if not isinstance(file_path, GcsfsFilePath):
        raise ValueError(f"Unexpected type [{file_path}]")
    return LegacyExtractAndMergeArgs(
        ingest_time=datetime.datetime.now(),
        file_path=file_path,
    )


def path_for_fixture_file(
    controller: BaseDirectIngestController,
    filename: str,
    should_normalize: bool,
    file_type: Optional[GcsfsDirectIngestFileType],
    dt: Optional[datetime.datetime] = None,
) -> GcsfsFilePath:
    return path_for_fixture_file_in_test_gcs_directory(
        bucket_path=controller.ingest_bucket_path,
        filename=filename,
        should_normalize=should_normalize,
        file_type=file_type,
        dt=dt,
    )


def path_for_fixture_file_in_test_gcs_directory(
    *,
    bucket_path: GcsfsBucketPath,
    filename: str,
    should_normalize: bool,
    file_type: Optional[GcsfsDirectIngestFileType],
    dt: Optional[datetime.datetime] = None,
) -> GcsfsFilePath:
    file_path_str = filename

    if should_normalize:
        if not file_type:
            raise ValueError("Expected file_type for path normalization but got None")
        file_path_str = to_normalized_unprocessed_file_path(
            original_file_path=file_path_str, file_type=file_type, dt=dt
        )

    file_path = GcsfsFilePath.from_directory_and_file_name(
        dir_path=bucket_path,
        file_name=file_path_str,
    )
    if not isinstance(file_path, GcsfsFilePath):
        raise ValueError(
            f"Expected type GcsfsFilePath, found {type(file_path)} for path: {file_path.abs_path()}"
        )
    return file_path


def run_task_queues_to_empty(controller: BaseDirectIngestController) -> None:
    """Runs task queues until they are all empty."""
    if isinstance(controller.cloud_task_manager, FakeAsyncDirectIngestCloudTaskManager):
        controller.cloud_task_manager.wait_for_all_tasks_to_run()
    elif isinstance(
        controller.cloud_task_manager, FakeSynchronousDirectIngestCloudTaskManager
    ):
        tm = controller.cloud_task_manager
        queue_args = (controller.region, controller.ingest_instance)
        while (
            tm.get_scheduler_queue_info(*queue_args).size()
            or tm.get_process_job_queue_info(*queue_args).size()
            or tm.get_raw_data_import_queue_info(controller.region).size()
            or tm.get_ingest_view_export_queue_info(*queue_args).size()
        ):
            if tm.get_raw_data_import_queue_info(controller.region).size():
                tm.test_run_next_raw_data_import_task()
                tm.test_pop_finished_raw_data_import_task()
            if tm.get_ingest_view_export_queue_info(*queue_args).size():
                tm.test_run_next_ingest_view_export_task()
                tm.test_pop_finished_ingest_view_export_task()
            if tm.get_scheduler_queue_info(*queue_args).size():
                tm.test_run_next_scheduler_task()
                tm.test_pop_finished_scheduler_task()
            if tm.get_process_job_queue_info(*queue_args).size():
                tm.test_run_next_process_job_task()
                tm.test_pop_finished_process_job_task()
    else:
        raise ValueError(
            f"Unexpected type for cloud task manager: "
            f"[{type(controller.cloud_task_manager)}]"
        )
