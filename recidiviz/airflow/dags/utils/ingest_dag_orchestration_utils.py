# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""
Helper functions for orchestrating the Ingest Airflow Dag.
"""
from typing import List, Tuple

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.gating import is_ingest_in_dataflow_enabled
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.dataflow_orchestration_utils import (
    get_ingest_pipeline_enabled_states,
)


def should_run_secondary_ingest_pipeline(state_code: StateCode) -> bool:
    """
    Returns whether the secondary ingest pipeline should be run for the given state.
    """
    if not is_ingest_in_dataflow_enabled(state_code, DirectIngestInstance.SECONDARY):
        return False

    # TODO(#23242): Update to determine whether to run for secondary or just primary based on pipeline mappings YAML
    return True


def _should_enable_state_and_instance(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> bool:
    if not direct_ingest_regions.get_direct_ingest_region(
        state_code.value.lower()
    ).is_ingest_launched_in_env():
        return False

    if not is_ingest_in_dataflow_enabled(state_code, ingest_instance) or (
        ingest_instance is DirectIngestInstance.SECONDARY
        and not should_run_secondary_ingest_pipeline(state_code)
    ):
        return False

    return True


def get_all_enabled_state_and_instance_pairs() -> List[
    Tuple[StateCode, DirectIngestInstance]
]:
    """
    Returns a list of all state and ingest instance pairs that the ingest pipeline should be run for.
    """
    states_and_instances: List[Tuple[StateCode, DirectIngestInstance]] = []
    for state in get_ingest_pipeline_enabled_states():
        for instance in DirectIngestInstance:
            if _should_enable_state_and_instance(state, instance):
                states_and_instances.append((state, instance))

    return states_and_instances
