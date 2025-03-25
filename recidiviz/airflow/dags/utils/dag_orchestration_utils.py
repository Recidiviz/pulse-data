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
from typing import Set, Tuple

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gating import is_raw_data_import_dag_enabled
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def get_raw_data_dag_enabled_state_and_instance_pairs() -> (
    Set[Tuple[StateCode, DirectIngestInstance]]
):
    """Returns a set of all state and ingest instance pairs that the raw data import DAG
    should be run for.
    """
    return {
        (state, instance)
        for state in get_direct_ingest_states_launched_in_env()
        for instance in DirectIngestInstance
        if is_raw_data_import_dag_enabled(state, instance)
    }
