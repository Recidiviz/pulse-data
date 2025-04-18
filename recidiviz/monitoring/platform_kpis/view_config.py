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
"""Views related to platform KPIs"""

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.cost.cost_views import (
    get_platform_cost_kpi_views_to_update,
)
from recidiviz.monitoring.platform_kpis.reliability.reliability_views import (
    get_platform_reliability_kpi_views_to_update,
)
from recidiviz.monitoring.platform_kpis.velocity.velocity_views import (
    get_platform_velocity_kpi_views_to_update,
)


def get_platform_kpi_views_to_update() -> list[BigQueryViewBuilder]:
    return [
        *get_platform_reliability_kpi_views_to_update(),
        *get_platform_cost_kpi_views_to_update(),
        *get_platform_velocity_kpi_views_to_update(),
    ]
