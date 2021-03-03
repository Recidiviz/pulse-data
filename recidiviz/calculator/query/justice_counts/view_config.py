# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Justice Counts view configuration."""
from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.justice_counts.views.cloudsql import (
    get_table_view_builders,
)
from recidiviz.calculator.query.justice_counts.views.corrections_metrics_by_month import (
    CorrectionsMetricsByMonthBigQueryViewCollector,
)
from recidiviz.calculator.query.justice_counts.views.jails_metrics_by_month import (
    JailsMetricsByMonthBigQueryViewCollector,
)

_corrections_metrics_collector = CorrectionsMetricsByMonthBigQueryViewCollector()
_jails_metrics_collector = JailsMetricsByMonthBigQueryViewCollector()

VIEW_BUILDERS_FOR_VIEWS_TO_EXPORT = [
    _corrections_metrics_collector.unified_builder,
    _jails_metrics_collector.unified_builder,
]

VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[BigQueryViewBuilder] = (
    get_table_view_builders()
    + _corrections_metrics_collector.collect_view_builders()
    + _jails_metrics_collector.collect_view_builders()
)
