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
"""View config for platform kpi cost views."""


from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.cost.bq_monthly_costs_by_dataset import (
    BQ_MONTHLY_COSTS_BY_DATASET_VIEW_BUILDER,
)


def get_platform_cost_kpi_views_to_update() -> list[BigQueryViewBuilder]:
    return [
        BQ_MONTHLY_COSTS_BY_DATASET_VIEW_BUILDER,
    ]
