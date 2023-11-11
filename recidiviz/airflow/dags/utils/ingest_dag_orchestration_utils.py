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
from recidiviz.ingest.direct.gating import ingest_pipeline_can_run_in_dag
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def get_ingest_pipeline_enabled_state_and_instance_pairs() -> List[
    Tuple[StateCode, DirectIngestInstance]
]:
    """
    Returns a list of all state and ingest instance pairs that the ingest pipeline should be run for.
    """
    states_and_instances: List[Tuple[StateCode, DirectIngestInstance]] = []
    for state in get_direct_ingest_states_existing_in_env():
        for instance in DirectIngestInstance:
            # TODO(#20997): Remove feature flag and include all instances once ingest is enabled in dataflow in all states
            if ingest_pipeline_can_run_in_dag(state, instance):
                states_and_instances.append((state, instance))

    return states_and_instances
