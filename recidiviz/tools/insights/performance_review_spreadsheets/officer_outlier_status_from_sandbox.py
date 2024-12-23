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
"""Utilities for querying officer outlier status from a sandbox dataset.

See branch danawillow/id-perf-sandbox for the changes required to be loaded.
"""

from collections import defaultdict
from datetime import date
from enum import Enum

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.outliers.constants import (
    INCARCERATION_STARTS_MOST_SEVERE_VIOLATION_TYPE_NOT_ABSCONSION,
    VIOLATIONS_ABSCONSION,
)
from recidiviz.utils.string import StrictStringFormatter

_OFFICER_OUTLIER_STATUS_QUERY = """
SELECT officer_id, end_date, ARRAY_AGG(metric_id) AS metrics
FROM `recidiviz-123.{sandbox_prefix}_outliers_views.supervision_officer_outlier_status_materialized`
WHERE state_code="US_IX"
  AND category_type="SEX_OFFENSE_BINARY"
  AND end_date BETWEEN "2024-01-01" AND "2024-12-01"
  AND status="FAR"
GROUP BY 1, 2
"""


class MetricType(Enum):
    INCARCERATION = "INCARCERATION"
    ABSCONSION = "ABSCONSION"

    @classmethod
    def from_metric_long_name(cls, name: str) -> "MetricType":
        if name == INCARCERATION_STARTS_MOST_SEVERE_VIOLATION_TYPE_NOT_ABSCONSION.name:
            return cls.INCARCERATION
        if name == VIOLATIONS_ABSCONSION.name:
            return cls.ABSCONSION
        raise ValueError(f"Unexpected metric name {name}")


@attr.define(frozen=True)
class OutlierMetrics:
    end_date_exclusive: date
    metrics: list[MetricType]


@attr.define(frozen=True)
class OfficerOutlierStatusFromSandbox:
    """Holds mappings of which officers where outliers on which metrics, each month"""

    data: dict[str, list[OutlierMetrics]]

    @classmethod
    def from_bigquery(
        cls, bq_client: BigQueryClient, sandbox_prefix: str
    ) -> "OfficerOutlierStatusFromSandbox":
        string_formatter = StrictStringFormatter()
        results = bq_client.run_query_async(
            query_str=string_formatter.format(
                _OFFICER_OUTLIER_STATUS_QUERY, sandbox_prefix=sandbox_prefix
            ),
            use_query_cache=True,
        )
        data = defaultdict(list)
        for row in results:
            metrics = [
                MetricType.from_metric_long_name(name) for name in row["metrics"]  # type: ignore
            ]
            outlier_metrics = OutlierMetrics(
                end_date_exclusive=row["end_date"], metrics=metrics
            )
            data[row["officer_id"]].append(outlier_metrics)
        return cls(data)
