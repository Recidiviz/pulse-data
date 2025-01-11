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
"""Class to collect all the aggregated metrics used in the outliers product."""

from recidiviz.aggregated_metrics.models.aggregated_metric import EventCountMetric
from recidiviz.outliers import constants
from recidiviz.outliers.types import OutliersMetric


class OutliersAggregatedMetricsCollector:
    @classmethod
    def get_metrics(cls) -> list[EventCountMetric]:
        return [
            metric.aggregated_metric
            for metric in vars(constants).values()
            if isinstance(metric, OutliersMetric)
        ]
