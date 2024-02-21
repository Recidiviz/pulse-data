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
"""Test implementation of the DirectIngestCloudTaskManager that runs tasks
synchronously, when prompted."""
import os.path
from collections import defaultdict
from typing import Dict, List, Tuple, Union

import attr

from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    DirectIngestQueueType,
    RawDataImportCloudTaskQueueInfo,
    SchedulerCloudTaskQueueInfo,
    build_handle_new_files_task_id,
    build_raw_data_import_task_id,
    build_scheduler_task_id,
)
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.types.cloud_task_args import GcsfsRawDataBQImportArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.monitoring import context
from recidiviz.tests.ingest.direct.fakes.fake_direct_ingest_cloud_task_manager import (
    FakeDirectIngestCloudTaskQueueManager,
)


@attr.define
class _SchedulerArgs:
    task_name: str
    instance: DirectIngestInstance


@attr.define
class _HandleNewFilesArgs:
    task_name: str
    instance: DirectIngestInstance
    can_start_ingest: bool


class FakeSynchronousDirectIngestCloudTaskManager(
    FakeDirectIngestCloudTaskQueueManager
):
    """Test implementation of the DirectIngestCloudTaskManager that runs tasks
    synchronously, when prompted."""

    def __init__(self) -> None:
        super().__init__()
        self.scheduler_tasks: Dict[
            DirectIngestInstance, List[Union[_SchedulerArgs, _HandleNewFilesArgs]]
        ] = defaultdict(list)
        self.num_finished_scheduler_tasks: Dict[
            DirectIngestInstance, int
        ] = defaultdict(int)
        self.raw_data_import_tasks: Dict[
            DirectIngestInstance, List[Tuple[str, GcsfsRawDataBQImportArgs]]
        ] = defaultdict(list)
        self.num_finished_raw_data_import_tasks: Dict[
            DirectIngestInstance, int
        ] = defaultdict(int)

    def get_scheduler_queue_info(
        self, region: DirectIngestRegion, ingest_instance: DirectIngestInstance
    ) -> SchedulerCloudTaskQueueInfo:
        return SchedulerCloudTaskQueueInfo(
            queue_name=self.queue_name_for_type(
                DirectIngestQueueType.SCHEDULER, ingest_instance
            ),
            task_names=[t.task_name for t in self.scheduler_tasks[ingest_instance]],
        )

    def get_raw_data_import_queue_info(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,  # pylint: disable=unused-argument
    ) -> RawDataImportCloudTaskQueueInfo:
        return RawDataImportCloudTaskQueueInfo(
            queue_name=self.queue_name_for_type(
                DirectIngestQueueType.RAW_DATA_IMPORT, ingest_instance
            ),
            task_names=[t[0] for t in self.raw_data_import_tasks[ingest_instance]],
        )

    def create_direct_ingest_scheduler_queue_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
    ) -> None:
        """Queues *but does not run* a scheduler task."""
        if not self.controllers.get(ingest_instance):
            raise ValueError(
                f"Controller for instance={ingest_instance} is null - did you call set_controller()?"
            )

        task_id = build_scheduler_task_id(region, ingest_instance)
        self.scheduler_tasks[ingest_instance].append(
            _SchedulerArgs(
                task_name=f"projects/path/to/{task_id}", instance=ingest_instance
            )
        )

    def create_direct_ingest_handle_new_files_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        can_start_ingest: bool,
    ) -> None:
        if not self.controllers.get(ingest_instance):
            raise ValueError(
                f"Controller for instance={ingest_instance} is null - did you call set_controller()?"
            )
        task_id = build_handle_new_files_task_id(region, ingest_instance)
        self.scheduler_tasks[ingest_instance].append(
            _HandleNewFilesArgs(
                task_name=f"projects/path/to/{task_id}",
                instance=ingest_instance,
                can_start_ingest=can_start_ingest,
            )
        )

    def create_direct_ingest_raw_data_import_task(
        self,
        region: DirectIngestRegion,
        raw_data_source_instance: DirectIngestInstance,
        data_import_args: GcsfsRawDataBQImportArgs,
    ) -> None:
        if not self.controllers.get(raw_data_source_instance):
            raise ValueError(
                f"Controller for instance={raw_data_source_instance} is null - did you"
                f" call set_controller()?"
            )
        task_id = build_raw_data_import_task_id(region, data_import_args)
        self.raw_data_import_tasks[raw_data_source_instance].append(
            (f"projects/path/to/{task_id}", data_import_args)
        )

    def delete_scheduler_queue_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        task_name: str,
    ) -> None:
        scheduler_tasks = []
        for task_info in self.scheduler_tasks[ingest_instance]:
            this_task_name = task_info.task_name
            if this_task_name != task_name:
                scheduler_tasks.append(task_info)

        self.scheduler_tasks[ingest_instance] = scheduler_tasks

    def test_run_next_scheduler_task(
        self, ingest_instance: DirectIngestInstance
    ) -> None:
        """Synchronously executes the next queued scheduler task, but *does not
        remove it from the queue*."""
        if not self.scheduler_tasks[ingest_instance]:
            raise ValueError("Scheduler job tasks should not be empty.")

        if self.num_finished_scheduler_tasks[ingest_instance]:
            raise ValueError("Must first pop last finished task.")

        controller = self.controllers.get(ingest_instance)
        if not controller:
            raise ValueError(
                f"Controller for instance={ingest_instance} is null - did you call set_controller()?"
            )

        task = self.scheduler_tasks[ingest_instance][0]
        task_name = task.task_name
        _, task_id = os.path.split(task_name)

        with context.push_region_context(
            controller.region.region_code,
            controller.ingest_instance.value,
        ):
            ingest_instance = task.instance
            if (
                controller.ingest_instance == DirectIngestInstance.SECONDARY
                and ingest_instance == DirectIngestInstance.PRIMARY
            ):
                raise ValueError(
                    f"Task request [{task_name}] for instance [{ingest_instance}]"
                    f"that does not match registered controller instance"
                    f"[{controller.ingest_instance}]. Only PRIMARY instances can schedule "
                    f"tasks in SECONDARY instances."
                )
            if isinstance(task, _SchedulerArgs):
                controller.schedule_next_ingest_task(current_task_id=task_id)
            elif isinstance(task, _HandleNewFilesArgs):
                controller.handle_new_files(
                    current_task_id=task_id, can_start_ingest=task.can_start_ingest
                )
            else:
                raise ValueError(f"Unexpected task id [{task}]")
        self.num_finished_scheduler_tasks[ingest_instance] += 1

    def test_run_next_raw_data_import_task(
        self, ingest_instance: DirectIngestInstance
    ) -> None:
        """Synchronously executes the next queued raw data import task, but *does not
        remove it from the queue*."""
        if not self.raw_data_import_tasks[ingest_instance]:
            raise ValueError("Raw data import tasks should not be empty.")

        if self.num_finished_raw_data_import_tasks[ingest_instance]:
            raise ValueError("Must first pop last finished task.")

        controller = self.controllers.get(ingest_instance)
        if not controller:
            raise ValueError(
                f"Controller for instance={ingest_instance} is null - did you call set_controller()?"
            )

        _task_id, args = self.raw_data_import_tasks[ingest_instance][0]

        with context.push_region_context(
            controller.region.region_code,
            controller.ingest_instance.value,
        ):
            controller.do_raw_data_import(data_import_args=args)

        self.num_finished_raw_data_import_tasks[ingest_instance] += 1

    def test_pop_finished_scheduler_task(
        self, ingest_instance: DirectIngestInstance
    ) -> None:
        """Removes most recently run scheduler task from the queue."""
        if self.num_finished_scheduler_tasks == 0:
            raise ValueError("No finished tasks to pop.")

        self.scheduler_tasks[ingest_instance].pop(0)
        self.num_finished_scheduler_tasks[ingest_instance] -= 1

    def test_pop_finished_raw_data_import_task(
        self, ingest_instance: DirectIngestInstance
    ) -> None:
        """Removes most recently run raw data import task from the queue."""
        if self.num_finished_raw_data_import_tasks == 0:
            raise ValueError("No finished tasks to pop.")

        self.raw_data_import_tasks[ingest_instance].pop(0)
        self.num_finished_raw_data_import_tasks[ingest_instance] -= 1
