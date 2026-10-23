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
import logging
from typing import List, Optional

from airflow.api.common.trigger_dag import trigger_dag
from airflow.decorators import task
from airflow.exceptions import DagRunAlreadyExists
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_identity_ingest_dag_id,
    get_raw_data_import_dag_id,
)
from recidiviz.airflow.dags.utils.config_utils import TENANT_FILTER
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
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
    has_file_import_errors: bool,
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


@task.short_circuit(ignore_downstream_trigger_rules=False)
def has_attempted_file_imports(serialized_file_imports: list[str] | None) -> bool:
    """Returns whether this run attempted any file imports (successful or failed), so
    downstream tasks that only matter after the data changed can be skipped otherwise.
    """
    return bool(serialized_file_imports)


@task
def trigger_identity_ingest_dag(*, region_code: str) -> None:
    """Triggers the identity ingest DAG for this state's tenant once a primary raw data
    import completes, so identity clustering re-runs against the freshly imported data.

    Runs downstream of has_attempted_file_imports, so it only fires when the import
    changed the data clustering reads; re-clustering unchanged data would reproduce the
    same result for the cost of a full pipeline run. Does nothing when the state has no
    launchable identity ingest views in this project, since the identity DAG would only
    short-circuit for it.
    """
    state_code = StateCode(region_code)
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value.lower()
    )
    if not region.has_launchable_ingest_views_for_pipeline(
        pipeline_type=IngestPipelineType.IDENTITY, project_id=get_project_id()
    ):
        return

    tenant = Tenant.from_state_code(state_code).value
    triggering_run_id = get_current_context()["run_id"]
    # Name the triggered run after the tenant and the raw data run that started it, so a
    # retry or clear of this task re-requests the same run_id and is idempotent.
    identity_run_id = f"triggered_by_raw_data_import__{tenant}__{triggering_run_id}"
    try:
        trigger_dag(
            dag_id=get_identity_ingest_dag_id(get_project_id()),
            run_id=identity_run_id,
            conf={TENANT_FILTER: tenant},
            # Keep microseconds on the execution_date. trigger_dag otherwise truncates it
            # to the second, and DagRun.find_duplicate matches on execution_date OR
            # run_id, so two states triggering within the same second would collide
            # despite their distinct run_ids.
            replace_microseconds=False,
        )
    except DagRunAlreadyExists as e:
        if e.dag_run.run_id != identity_run_id:
            # Collided with a different run on execution_date alone; fail loudly so a
            # rerun of this task creates the run rather than silently dropping it.
            raise
        # A retry or clear of this task re-requests the same run_id; the identity run
        # already exists, so treat it as already triggered. A cleared raw-data run that
        # imports newly arrived files won't re-trigger identity until the next import.
        logging.info(
            "Identity ingest DAG run [%s] already exists; skipping re-trigger.",
            identity_run_id,
        )
