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
from typing import Optional

from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.ingest.direct.fakes.fake_async_direct_ingest_cloud_task_manager import (
    FakeAsyncDirectIngestCloudTaskManager,
)
from recidiviz.tests.ingest.direct.fakes.fake_synchronous_direct_ingest_cloud_task_manager import (
    FakeSynchronousDirectIngestCloudTaskManager,
)


# TODO(#20930): Remove this once is_ingest_in_dataflow_enabled=True everywhere and just
#   use ingest_instance.
def _get_raw_data_source_instance(
    controller: BaseDirectIngestController,
) -> Optional[DirectIngestInstance]:
    if controller.is_ingest_in_dataflow_enabled:
        return controller.ingest_instance
    return controller.ingest_instance_status_manager.get_raw_data_source_instance()


def run_task_queues_to_empty(controller: BaseDirectIngestController) -> None:
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
            or tm.get_extract_and_merge_queue_info(
                *queue_args,
            ).size()
            # TODO(#20930): In legacy ingest, raw data relevant to this instance could
            #  be processed in either instance so we check both queues. Once ingest in
            #  dataflow is shipped, we should only need to check queue info for
            #  `controller.ingest_instance`.
            or tm.get_raw_data_import_queue_info(
                region=controller.region,
                ingest_instance=DirectIngestInstance.PRIMARY,
            ).size()
            or tm.get_raw_data_import_queue_info(
                region=controller.region,
                ingest_instance=DirectIngestInstance.SECONDARY,
            ).size()
            or tm.get_ingest_view_materialization_queue_info(
                *queue_args,
            ).size()
        ):
            # TODO(#20930): In legacy ingest, raw data relevant to this instance could
            #  be processed in either instance so we look at both queues. Once ingest in
            #  dataflow is shipped, we should only need to run tasks for
            #  `controller.ingest_instance`.
            for raw_data_instance in DirectIngestInstance:
                if tm.get_raw_data_import_queue_info(
                    region=controller.region,
                    ingest_instance=raw_data_instance,
                ).size():
                    tm.test_run_next_raw_data_import_task(raw_data_instance)
                    tm.test_pop_finished_raw_data_import_task(raw_data_instance)
            if tm.get_ingest_view_materialization_queue_info(
                *queue_args,
            ).size():
                tm.test_run_next_ingest_view_materialization_task(
                    controller.ingest_instance
                )
                tm.test_pop_finished_ingest_view_materialization_task(
                    controller.ingest_instance
                )
            if tm.get_scheduler_queue_info(*queue_args).size():
                tm.test_run_next_scheduler_task(controller.ingest_instance)
                tm.test_pop_finished_scheduler_task(controller.ingest_instance)
            if tm.get_extract_and_merge_queue_info(
                *queue_args,
            ).size():
                tm.test_run_next_extract_and_merge_task(controller.ingest_instance)
                tm.test_pop_finished_extract_and_merge_task(controller.ingest_instance)
    else:
        raise ValueError(
            f"Unexpected type for cloud task manager: "
            f"[{type(controller.cloud_task_manager)}]"
        )
