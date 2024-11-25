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
"""Collects the standard set of deployed aggregated metrics views using legacy
(expensive) query structure.
"""
from recidiviz.aggregated_metrics.legacy.aggregated_metric_view_collector import (
    collect_legacy_aggregated_metrics_view_builders,
)
from recidiviz.aggregated_metrics.standard_deployed_metrics_by_population import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.standard_deployed_unit_of_analysis_types_by_population_type import (
    UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder


def collect_standard_legacy_aggregated_metric_views() -> list[
    SimpleBigQueryViewBuilder
]:
    return collect_legacy_aggregated_metrics_view_builders(
        metrics_by_population_dict=METRICS_BY_POPULATION_TYPE,
        units_of_analysis_by_population_dict=UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
    )
