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
"""Configured metrics for custom insights impact metrics displayable in Looker"""

import recidiviz.aggregated_metrics.models.aggregated_metric_configurations as metric_config

ABSCONSIONS_AND_BENCH_WARRANTS_LOOKER = metric_config.ABSCONSIONS_BENCH_WARRANTS
AVG_DAILY_POPULATION_LOOKER = metric_config.AVG_DAILY_POPULATION
INCARCERATION_STARTS_LOOKER = metric_config.INCARCERATION_STARTS
INCARCERATION_STARTS_AND_INFERRED_LOOKER = (
    metric_config.INCARCERATION_STARTS_AND_INFERRED
)
VIOLATIONS_ABSCONSION = next(
    metric
    for metric in metric_config.VIOLATIONS_BY_TYPE_METRICS
    if metric.name == "violations_absconsion"
)
