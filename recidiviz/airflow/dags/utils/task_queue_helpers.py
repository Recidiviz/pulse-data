# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpers for accessing information about Cloud Task queues."""

from typing import Any, Dict, List, Optional, Union

from google.cloud import tasks_v2

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def get_running_queue_instances(*queues: Optional[Dict[str, Any]]) -> List[str]:
    """Returns the queues that are currently running and will need to be unpaused later."""
    ingest_instances_to_unpause: List[str] = []
    for queue in queues:
        if queue is None:
            raise ValueError("Found null queue in queues list")
        if queue["state"] == tasks_v2.Queue.State.RUNNING:
            if "secondary" in queue["name"]:
                ingest_instances_to_unpause.append(
                    DirectIngestInstance.SECONDARY.value.lower()
                )
            else:
                ingest_instances_to_unpause.append(
                    DirectIngestInstance.PRIMARY.value.lower()
                )
    return ingest_instances_to_unpause


def queues_were_unpaused(
    queues_to_resume: Optional[List[str]],
    task_ids_for_queues_to_resume: List[str],
    task_id_if_no_queues_to_resume: str,
) -> Union[str, List[str]]:
    if queues_to_resume is None:
        raise ValueError("Found unexpectedly null queues_paused list")
    tasks_to_return_if_unpaused: List[str] = []
    for task_id in task_ids_for_queues_to_resume:
        for queue in queues_to_resume:
            if queue in task_id:
                tasks_to_return_if_unpaused.append(task_id)
    if tasks_to_return_if_unpaused:
        # If any queues were paused and need to be unpaused, return those
        return tasks_to_return_if_unpaused
    # Otherwise, if no queues were paused and need to be unpaused, return this task
    return task_id_if_no_queues_to_resume
