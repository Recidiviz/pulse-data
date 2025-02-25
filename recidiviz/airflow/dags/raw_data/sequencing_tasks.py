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

from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
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
