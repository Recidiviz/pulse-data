# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines configuration related to updating / materializing our deployed views."""
from typing import Dict

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import ProcessDagPerfConfig

MAX_SINGLE_VIEW_REMATERIALIZATION_TIME_SECONDS = 60 * 6  # 6 min

# Add overrides here for graph nodes that are known to be more expensive to process.
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# IF YOU MUST MODIFY THIS MAP, PLEASE ADD Recidiviz/infra-review AS A REVIEWER TO YOUR
# PULL REQUEST.
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
ALLOWED_REMATERIALIZATION_TIME_OVERRIDES: Dict[BigQueryAddress, float] = {
    # This is a foundational view that is know to be expensive - we're ok with this
    # taking a bit longer to materialize.
    BigQueryAddress(
        dataset_id="sessions",
        table_id="dataflow_sessions",
    ): (60 * 10),
}

# Perf configuration for any process_dag() calls that rematerialize views
VIEW_DAG_REMATERIALIZATION_PERF_CONFIG = ProcessDagPerfConfig(
    node_max_processing_time_seconds=MAX_SINGLE_VIEW_REMATERIALIZATION_TIME_SECONDS,
    node_allowed_process_time_overrides=ALLOWED_REMATERIALIZATION_TIME_OVERRIDES,
)
