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
"""Outliers-related types"""
from enum import Enum
from typing import Dict, List

import attr

from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)


class MetricOutcome(Enum):
    FAVORABLE = "FAVORABLE"
    ADVERSE = "ADVERSE"


@attr.s
class OutliersMetric:
    name: str = attr.ib()
    outcome_type: MetricOutcome = attr.ib()


@attr.s
class OutliersConfig:
    # List of metrics that are relevant for this state,
    # where each element corresponds to a column name in an aggregated_metrics views
    metrics: List[OutliersMetric] = attr.ib()

    # Location exclusions; a unit of analysis mapped to a list of ids to exclude
    unit_of_analysis_to_exclusion: Dict[MetricUnitOfAnalysisType, List[str]] = attr.ib(
        default=None
    )
