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
from recidiviz.ingest.direct.controllers.ingest_raw_file_import_controller import (
    IngestRawFileImportController,
)
from recidiviz.tests.ingest.direct.fakes.fake_async_direct_ingest_cloud_task_manager import (
    FakeAsyncDirectIngestCloudTaskManager,
)
from recidiviz.tests.ingest.direct.fakes.fake_synchronous_direct_ingest_cloud_task_manager import (
    FakeSynchronousDirectIngestCloudTaskManager,
)


def run_task_queues_to_empty(controller: IngestRawFileImportController) -> None:
    """Runs task queues until they are all empty."""
    if isinstance(controller.cloud_task_manager, FakeAsyncDirectIngestCloudTaskManager):
        controller.cloud_task_manager.wait_for_all_tasks_to_run(
            controller.ingest_instance
        )
    elif isinstance(
        controller.cloud_task_manager, FakeSynchronousDirectIngestCloudTaskManager
    ):
        tm = controller.cloud_task_manager
        queue_args = (controller.region, controller.ingest_instance)
        while (
            tm.get_scheduler_queue_info(*queue_args).size()
            or tm.get_raw_data_import_queue_info(
                region=controller.region,
                ingest_instance=controller.ingest_instance,
            ).size()
        ):
            if tm.get_raw_data_import_queue_info(
                region=controller.region,
                ingest_instance=controller.ingest_instance,
            ).size():
                tm.test_run_next_raw_data_import_task(controller.ingest_instance)
                tm.test_pop_finished_raw_data_import_task(controller.ingest_instance)
            if tm.get_scheduler_queue_info(*queue_args).size():
                tm.test_run_next_scheduler_task(controller.ingest_instance)
                tm.test_pop_finished_scheduler_task(controller.ingest_instance)
    else:
        raise ValueError(
            f"Unexpected type for cloud task manager: "
            f"[{type(controller.cloud_task_manager)}]"
        )
