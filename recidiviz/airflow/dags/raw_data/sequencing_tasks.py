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
"""Sequencing tasks for the raw data import dag."""
from typing import List, Optional

from airflow.api.common.trigger_dag import trigger_dag
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.monitoring.dag_registry import get_raw_data_import_dag_id
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import RawDataResourceLock


@task.short_circuit(ignore_downstream_trigger_rules=False)
def has_files_to_import(serialized_bq_metadata: List[str] | None) -> bool:
    """If we find no files to import, let's explicitly skip the import steps of the DAG
    and skip right to clean up and storage.
    """
    return serialized_bq_metadata is not None and len(serialized_bq_metadata) != 0


@task.short_circuit(
    ignore_downstream_trigger_rules=False, trigger_rule=TriggerRule.ALL_DONE
)
def successfully_acquired_all_locks(
    maybe_serialized_acquired_locks: Optional[List[str]],
    resources_needed: List[DirectIngestRawDataResourceLockResource],
) -> bool:
    """If we were not able to acquire raw data resource locks, let's skip all
    downstream tasks to make sure no work is done.
    """
    if maybe_serialized_acquired_locks is None:
        return False

    acquired_locks = [
        RawDataResourceLock.deserialize(serialized_lock)
        for serialized_lock in maybe_serialized_acquired_locks
    ]
    resources_acquired = {
        lock.lock_resource for lock in acquired_locks if lock.released is False
    }

    return all(resource in resources_acquired for resource in resources_needed)


@task
def maybe_trigger_dag_rerun(
    *,
    region_code: str,
    raw_data_instance: DirectIngestInstance,
    deferred_files: list[str],
    has_file_import_errors: bool
) -> None:
    """Kicks off a state-specific secondary raw data import DAG re-run IFF:
    - the current DAG run was a state-specific secondary run
    - the DAG found more files in the ingest bucket that it could import in a single
      dag run
    - the DAG did not have any file import errors
    In order to help facilitate secondary re-runs where the number of files in the
    ingest bucket exceeds what we think we want to handle in a single import run.
    We don't intend on using this for PRIMARY runs because we have a regularly scheduled
    CRON job that is responsible for kicking off dag runs.
    """

    if (
        raw_data_instance == DirectIngestInstance.SECONDARY
        and deferred_files
        and not has_file_import_errors
    ):
        trigger_dag(
            dag_id=get_raw_data_import_dag_id(get_project_id()),
            conf={
                "state_code_filter": region_code.upper(),
                "raw_data_instance": raw_data_instance.value.upper(),
            },
        )
