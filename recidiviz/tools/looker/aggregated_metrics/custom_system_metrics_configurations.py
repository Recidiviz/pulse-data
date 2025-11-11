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
"""
Additional configurations for autogeneration of LookML for a custom aggregated
metrics explore
"""

from recidiviz.aggregated_metrics.assignment_sessions_view_collector import (
    relevant_units_of_analysis_for_population_type,
)
from recidiviz.aggregated_metrics.configuration.collections.standard import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)

# TODO(#53131): Deprecate this explore in favor of `experiment_kpi_metrics`

# Loops through all configured population <> unit of analysis combinations and generates a dictionary
# that maps their combined name (assignment type) to a tuple with population and unit of analysis type
ASSIGNMENT_NAME_TO_TYPES: dict[
    str, tuple[MetricPopulationType, MetricUnitOfAnalysisType]
] = {
    f"{population_type.population_name_short}_{unit_of_analysis_type.short_name}".upper(): (
        population_type,
        unit_of_analysis_type,
    )
    for population_type in MetricPopulationType
    if population_type != MetricPopulationType.CUSTOM
    for unit_of_analysis_type in relevant_units_of_analysis_for_population_type(
        population_type
    )
}

# Special case: add person unit of analysis for the whole justice involved population
ASSIGNMENT_NAME_TO_TYPES["PERSON"] = (
    MetricPopulationType.JUSTICE_INVOLVED,
    MetricUnitOfAnalysisType.PERSON_ID,
)

CUSTOM_SYSTEM_IMPACT_LOOKER_METRICS = METRICS_BY_POPULATION_TYPE[
    MetricPopulationType.SUPERVISION
]
