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
"""Configured metrics for custom JII tablet app metrics displayable in Looker"""

import recidiviz.aggregated_metrics.models.aggregated_metric_configurations as metric_config
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)

JII_TABLET_APP_ASSIGNMENT_NAMES_TO_TYPES = {
    "ALL_INCARCERATION_STATES": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "INCARCERATION_STATE": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
    "INCARCERATION_FACILITY": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY,
    ),
    "INCARCERATION_FACILITY_COUNSELOR": (
        MetricPopulationType.INCARCERATION,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
    ),
}


JII_TABLET_APP_IMPACT_LOOKER_METRICS: list[AggregatedMetric] = [
    metric_config.AVG_DAILY_POPULATION,
    metric_config.DISTINCT_JII_TABLET_APP_PROVISIONED_USERS,
    metric_config.DISTINCT_REGISTERED_JII_TABLET_APP_PROVISIONED_USERS,
    metric_config.DISTINCT_LOGGED_IN_JII_TABLET_APP_USERS,
    metric_config.LOGINS_JII_TABLET_APP_USER,
]
