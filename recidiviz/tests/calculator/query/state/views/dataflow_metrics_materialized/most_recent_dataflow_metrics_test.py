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
"""Tests for most_recent_dataflow_metrics.py"""

from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    make_most_recent_metric_view_builders,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class MostRecentDataflowMetricsTest(BigQueryEmulatorTestCase):
    """Tests for the generated most_recent_* views"""

    def test_only_most_recent(self) -> None:
        # Arrange
        metric_name = "test_metric"
        address = BigQueryAddress(
            dataset_id=DATAFLOW_METRICS_DATASET, table_id=metric_name
        )
        old_job_data = [
            # fmt: off
            # Job 1 - State A
            {"state_code": "A", "metric_type": "X", "job_id": "1", "date": "2022-03-01", "value": 1},
            {"state_code": "A", "metric_type": "X", "job_id": "1", "date": "2022-03-07", "value": 6},
            {"state_code": "A", "metric_type": "X", "job_id": "1", "date": "2022-04-02", "value": 3},
            # fmt: on
        ]
        new_job_data = [
            # fmt: off
            # Job 2 - State A
            {"state_code": "A", "metric_type": "X", "job_id": "2", "date": "2022-04-02", "value": 7},
            {"state_code": "A", "metric_type": "X", "job_id": "2", "date": "2022-04-07", "value": 5},
            # Job 3 - State B
            {"state_code": "B", "metric_type": "X", "job_id": "3", "date": "2022-04-02", "value": 7},
            {"state_code": "B", "metric_type": "X", "job_id": "3", "date": "2022-04-07", "value": 6},
            # fmt: on
        ]
        self.create_mock_table(
            address=address,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("metric_type", str),
                schema_field_for_type("job_id", str),
                schema_field_for_type("date", str),
                schema_field_for_type("value", int),
            ],
        )
        self.load_rows_into_table(
            address=address,
            data=old_job_data + new_job_data,
        )

        # Act / Assert
        query_builder = one(
            make_most_recent_metric_view_builders(
                metric_name=metric_name,
            )
        )
        self.run_query_test(
            query_str=query_builder.build().view_query, expected_result=new_job_data
        )
