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
"""A test view builder file for big_query_view_collector_test.py"""
from typing import List

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.metrics.metric_big_query_view import MetricBigQueryView
from recidiviz.tests.big_query.fake_big_query_view_builder import (
    FakeBigQueryViewBuilder,
)
from recidiviz.utils.metadata import local_project_id_override

with local_project_id_override("my-project-id"):
    GOOD_VIEW_5 = MetricBigQueryView(
        dataset_id="fake_metrics_dataset",
        view_id="fake_metric_view",
        description="My metric description",
        view_query_template="SELECT * FROM table5",
        materialized_address=BigQueryAddress(
            dataset_id="fake_metrics_dataset", table_id="fake_metric_view_materialized"
        ),
        dimensions=("dimension_col",),
        sandbox_context=None,
    )
    GOOD_VIEW_6 = MetricBigQueryView(
        dataset_id="fake_metrics_dataset",
        view_id="fake_metric_view",
        description="My metric description",
        view_query_template="SELECT * FROM table6",
        materialized_address=BigQueryAddress(
            dataset_id="fake_metrics_dataset", table_id="fake_metric_view_materialized"
        ),
        dimensions=("dimension_col",),
        sandbox_context=None,
    )


def collect_view_builder() -> List[FakeBigQueryViewBuilder]:
    return [
        FakeBigQueryViewBuilder(GOOD_VIEW_5),
        FakeBigQueryViewBuilder(GOOD_VIEW_6),
    ]
