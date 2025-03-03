# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View config for velocity KPIs"""
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.velocity.dag_runtimes import (
    DAG_RUNTIMES_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.velocity.normalized_state_hydration_live_snapshot import (
    get_normalized_state_hydration_live_snapshot_view_builder,
)
from recidiviz.monitoring.platform_kpis.velocity.normalized_state_table_hydration import (
    NORMALIZED_STATE_TABLE_HYDRATION_VIEW_BUILDER,
)


def get_platform_velocity_kpi_views_to_update() -> list[BigQueryViewBuilder]:
    return [
        get_normalized_state_hydration_live_snapshot_view_builder(),
        DAG_RUNTIMES_VIEW_BUILDER,
        NORMALIZED_STATE_TABLE_HYDRATION_VIEW_BUILDER,
    ]
