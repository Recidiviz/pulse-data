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
from unittest import mock

import attr
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized import (
    most_recent_dataflow_metrics,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    make_most_recent_metric_view_builders,
)
from recidiviz.calculator.query.state.views.reference.supervision_location_ids_to_names import (
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.person_demographics import (
    PERSON_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.pipelines.utils.identifier_models import SupervisionLocationMixin
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

MOST_RECENT_DATAFLOW_METRICS_MODULE = most_recent_dataflow_metrics.__name__


@attr.s
class MockMetric(RecidivizMetric):
    value: int = attr.ib(default=0)

    @classmethod
    def get_description(cls) -> str:
        return "Metric"


@attr.s
class MockSupervisionLocationMetric(RecidivizMetric, SupervisionLocationMixin):
    value: int = attr.ib(default=0)

    @classmethod
    def get_description(cls) -> str:
        return "Metric with location info"


class MostRecentDataflowMetricsTest(BigQueryEmulatorTestCase):
    """Tests for the generated most_recent_* views"""

    def setUp(self) -> None:
        super().setUp()
        self.most_recent_dataflow_metrics_patcher = mock.patch(
            f"{MOST_RECENT_DATAFLOW_METRICS_MODULE}.DATAFLOW_TABLES_TO_METRICS",
            {
                "test_metric": MockMetric,
                "supervision_metric": MockSupervisionLocationMetric,
            },
        )
        self.most_recent_dataflow_metrics_patcher.start()

    def tearDown(self) -> None:
        self.most_recent_dataflow_metrics_patcher.stop()
        super().tearDown()

    def test_only_most_recent(self) -> None:
        # Arrange
        metric_name = "test_metric"
        metrics_address = BigQueryAddress(
            dataset_id=DATAFLOW_METRICS_DATASET, table_id=metric_name
        )
        old_job_data = [
            # fmt: off
            # Job 1 - State A
            {"state_code": "A", "metric_type": "X", "job_id": "1", "date": "2022-03-01", "person_id": 101, "value": 1},
            {"state_code": "A", "metric_type": "X", "job_id": "1", "date": "2022-03-07", "person_id": 101, "value": 6},
            {"state_code": "A", "metric_type": "X", "job_id": "1", "date": "2022-04-02", "person_id": 101, "value": 3},
            # fmt: on
        ]
        new_job_data = [
            # fmt: off
            # Job 2 - State A
            {"state_code": "A", "metric_type": "X", "job_id": "2", "date": "2022-04-02", "person_id": 101, "value": 7},
            {"state_code": "A", "metric_type": "X", "job_id": "2", "date": "2022-04-07", "person_id": 101, "value": 5},
            # Job 3 - State B
            {"state_code": "B", "metric_type": "X", "job_id": "3", "date": "2022-04-02", "person_id": 201, "value": 7},
            {"state_code": "B", "metric_type": "X", "job_id": "3", "date": "2022-04-07", "person_id": 201, "value": 6},
            # fmt: on
        ]
        self.create_mock_table(
            address=metrics_address,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("metric_type", str),
                schema_field_for_type("job_id", str),
                schema_field_for_type("date", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("value", int),
            ],
        )
        self.load_rows_into_table(
            address=metrics_address,
            data=old_job_data + new_job_data,
        )
        demographics_address = BigQueryAddress(
            dataset_id=SESSIONS_DATASET, table_id="person_demographics_materialized"
        )
        self.create_mock_table(
            address=demographics_address,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("prioritized_race_or_ethnicity", str),
            ],
        )
        self.load_rows_into_table(
            address=demographics_address,
            data=[
                # fmt: off
                {"state_code": "A", "person_id": 101, "prioritized_race_or_ethnicity": "BLACK"},
                {"state_code": "B", "person_id": 201, "prioritized_race_or_ethnicity": "INTERNAL_UNKNOWN"},
                # fmt: on
            ],
        )

        # Act / Assert
        query_builder = one(
            make_most_recent_metric_view_builders(
                metric_name=metric_name,
            )
        )
        self.run_query_test(
            query_str=query_builder.build().view_query,
            expected_result=[
                # fmt: off
                # Job 2 - State A
                {"state_code": "A", "metric_type": "X", "job_id": "2", "date": "2022-04-02", "person_id": 101, "value": 7, "prioritized_race_or_ethnicity": "BLACK"},
                {"state_code": "A", "metric_type": "X", "job_id": "2", "date": "2022-04-07", "person_id": 101, "value": 5, "prioritized_race_or_ethnicity": "BLACK"},
                # Job 3 - State B
                {"state_code": "B", "metric_type": "X", "job_id": "3", "date": "2022-04-02", "person_id": 201, "value": 7, "prioritized_race_or_ethnicity": None},
                {"state_code": "B", "metric_type": "X", "job_id": "3", "date": "2022-04-07", "person_id": 201, "value": 6, "prioritized_race_or_ethnicity": None},
                # fmt: on
            ],
        )

    def test_only_most_recent_supervision_location_metric(self) -> None:
        # Arrange
        metric_name = "supervision_metric"
        metrics_address = BigQueryAddress(
            dataset_id=DATAFLOW_METRICS_DATASET, table_id=metric_name
        )
        old_job_data = [
            # Job 1 - State A
            {
                "state_code": "A",
                "metric_type": "X",
                "job_id": "1",
                "date": "2022-03-01",
                "person_id": 101,
                "value": 1,
                "level_1_supervision_location_external_id": "district_1",
                "level_2_supervision_location_external_id": None,
            },
            {
                "state_code": "A",
                "metric_type": "X",
                "job_id": "1",
                "date": "2022-03-07",
                "person_id": 101,
                "value": 6,
                "level_1_supervision_location_external_id": "district_2",
                "level_2_supervision_location_external_id": None,
            },
            {
                "state_code": "A",
                "metric_type": "X",
                "job_id": "1",
                "date": "2022-04-02",
                "person_id": 101,
                "value": 3,
                "level_1_supervision_location_external_id": "district_3",
                "level_2_supervision_location_external_id": None,
            },
        ]
        new_job_data = [
            # Job 2 - State A
            {
                "state_code": "A",
                "metric_type": "X",
                "job_id": "2",
                "date": "2022-04-02",
                "person_id": 101,
                "value": 7,
                "level_1_supervision_location_external_id": "district_1",
                "level_2_supervision_location_external_id": None,
            },
            {
                "state_code": "A",
                "metric_type": "X",
                "job_id": "2",
                "date": "2022-04-07",
                "person_id": 101,
                "value": 5,
                "level_1_supervision_location_external_id": "district_2",
                "level_2_supervision_location_external_id": None,
            },
            # Job 3 - State B
            {
                "state_code": "B",
                "metric_type": "X",
                "job_id": "3",
                "date": "2022-04-02",
                "person_id": 201,
                "value": 7,
                "level_1_supervision_location_external_id": "district_1",
                "level_2_supervision_location_external_id": "region_1",
            },
            {
                "state_code": "B",
                "metric_type": "X",
                "job_id": "3",
                "date": "2022-04-07",
                "person_id": 201,
                "value": 6,
                "level_1_supervision_location_external_id": "district_2",
                "level_2_supervision_location_external_id": "region_2",
            },
        ]
        self.create_mock_table(
            address=metrics_address,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("metric_type", str),
                schema_field_for_type("job_id", str),
                schema_field_for_type("date", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("value", int),
                schema_field_for_type("level_1_supervision_location_external_id", str),
                schema_field_for_type("level_2_supervision_location_external_id", str),
            ],
        )
        self.load_rows_into_table(
            address=metrics_address,
            data=old_job_data + new_job_data,
        )
        demographics_address = PERSON_DEMOGRAPHICS_VIEW_BUILDER.table_for_query
        self.create_mock_table(
            address=demographics_address,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("prioritized_race_or_ethnicity", str),
            ],
        )
        self.load_rows_into_table(
            address=demographics_address,
            data=[
                # fmt: off
                {"state_code": "A", "person_id": 101, "prioritized_race_or_ethnicity": "BLACK"},
                {"state_code": "B", "person_id": 201, "prioritized_race_or_ethnicity": "INTERNAL_UNKNOWN"},
                # fmt: on
            ],
        )

        location_ids_address = (
            SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER.table_for_query
        )
        self.create_mock_table(
            address=location_ids_address,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("level_1_supervision_location_external_id", str),
                schema_field_for_type("level_2_supervision_location_external_id", str),
            ],
        )
        self.load_rows_into_table(
            address=location_ids_address,
            data=[
                # fmt: off
                {"state_code": "A", "level_1_supervision_location_external_id": "district_1", "level_2_supervision_location_external_id": None},
                {"state_code": "A", "level_1_supervision_location_external_id": "district_2", "level_2_supervision_location_external_id": None},
                {"state_code": "B", "level_1_supervision_location_external_id": "district_1", "level_2_supervision_location_external_id": "region_1",},
                {"state_code": "B", "level_1_supervision_location_external_id": "district_2", "level_2_supervision_location_external_id": "region_2",},
                # fmt: on
            ],
        )

        # Act / Assert
        query_builder = one(
            make_most_recent_metric_view_builders(
                metric_name=metric_name,
            )
        )
        self.run_query_test(
            query_str=query_builder.build().view_query,
            expected_result=[
                # Job 2 - State A
                {
                    "state_code": "A",
                    "metric_type": "X",
                    "job_id": "2",
                    "date": "2022-04-02",
                    "person_id": 101,
                    "value": 7,
                    "prioritized_race_or_ethnicity": "BLACK",
                    "supervising_district_external_id": "district_1",
                    "level_1_supervision_location_external_id": "district_1",
                    "level_2_supervision_location_external_id": None,
                },
                {
                    "state_code": "A",
                    "metric_type": "X",
                    "job_id": "2",
                    "date": "2022-04-07",
                    "person_id": 101,
                    "value": 5,
                    "prioritized_race_or_ethnicity": "BLACK",
                    "supervising_district_external_id": "district_2",
                    "level_1_supervision_location_external_id": "district_2",
                    "level_2_supervision_location_external_id": None,
                },
                # Job 3 - State B
                {
                    "state_code": "B",
                    "metric_type": "X",
                    "job_id": "3",
                    "date": "2022-04-02",
                    "person_id": 201,
                    "value": 7,
                    "prioritized_race_or_ethnicity": None,
                    "supervising_district_external_id": "region_1",
                    "level_1_supervision_location_external_id": "district_1",
                    "level_2_supervision_location_external_id": "region_1",
                },
                {
                    "state_code": "B",
                    "metric_type": "X",
                    "job_id": "3",
                    "date": "2022-04-07",
                    "person_id": 201,
                    "value": 6,
                    "prioritized_race_or_ethnicity": None,
                    "supervising_district_external_id": "region_2",
                    "level_1_supervision_location_external_id": "district_2",
                    "level_2_supervision_location_external_id": "region_2",
                },
            ],
        )
