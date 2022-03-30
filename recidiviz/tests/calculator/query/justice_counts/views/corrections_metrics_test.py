# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests Corrections metrics functionality."""
import datetime

import pandas as pd
from mock import Mock, patch
from pandas.testing import assert_frame_equal
from sqlalchemy.sql import sqltypes

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.justice_counts.views import (
    corrections_metrics,
    metric_calculator,
)
from recidiviz.justice_counts.dimensions import corrections, location
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.tests.big_query.fakes.fake_table_schema import MockTableSchema
from recidiviz.tests.big_query.view_test_util import BaseViewTest
from recidiviz.tests.calculator.query.justice_counts.views.metric_calculator_test import (
    METRIC_CALCULATOR_SCHEMA,
    FakeState,
    row,
)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class CorrectionsOutputViewTest(BaseViewTest):
    """Tests the Corrections output view."""

    INPUT_SCHEMA = MockTableSchema(
        {
            **METRIC_CALCULATOR_SCHEMA.data_types,
            "compare_date_partition": sqltypes.Date(),
            "compare_value": sqltypes.Numeric(),
            "state_code": sqltypes.String(255),
        }
    )

    def test_recent_population(self) -> None:
        """Tests the basic use case of calculating population"""
        # Arrange
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="source_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Source.__table__),
            mock_data=pd.DataFrame(
                [[1, "XX"], [2, "YY"], [3, "ZZ"]], columns=["id", "name"]
            ),
        )
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="report_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Report.__table__),
            mock_data=pd.DataFrame(
                [
                    [
                        1,
                        1,
                        "_",
                        "All",
                        "2021-01-01",
                        "xx.gov",
                        "MANUALLY_ENTERED",
                        "John",
                    ],
                    [
                        2,
                        2,
                        "_",
                        "All",
                        "2021-01-02",
                        "yy.gov",
                        "MANUALLY_ENTERED",
                        "Jane",
                    ],
                    [
                        3,
                        3,
                        "_",
                        "All",
                        "2021-01-02",
                        "zz.gov",
                        "MANUALLY_ENTERED",
                        "Jude",
                    ],
                ],
                columns=[
                    "id",
                    "source_id",
                    "type",
                    "instance",
                    "publish_date",
                    "url",
                    "acquisition_method",
                    "acquired_by",
                ],
            ),
        )
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="metric_calculator",
            mock_schema=self.INPUT_SCHEMA,
            mock_data=pd.DataFrame(
                [
                    row(
                        1,
                        "2021-01-01",
                        "2020-11-30",
                        (FakeState("US_XX"),),
                        ["A", "B", "A"],
                        3000,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_XX"),
                    row(
                        1,
                        "2021-01-01",
                        "2020-12-31",
                        (FakeState("US_XX"),),
                        ["B", "B", "C"],
                        4000,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_XX"),
                    row(
                        2,
                        "2021-01-01",
                        "2020-11-30",
                        (FakeState("US_YY"),),
                        ["A", "B", "A"],
                        1000,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_YY"),
                    row(
                        2,
                        "2021-01-01",
                        "2020-12-31",
                        (FakeState("US_YY"),),
                        ["B", "B", "C"],
                        1020,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_YY"),
                    row(
                        3,
                        "2021-01-01",
                        "2020-11-30",
                        (FakeState("US_ZZ"),),
                        ["A", "B", "A"],
                        400,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_ZZ"),
                    row(
                        3,
                        "2021-01-01",
                        "2020-12-31",
                        (FakeState("US_ZZ"),),
                        ["C", "C", "B"],
                        500,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_ZZ"),
                ],
                columns=self.INPUT_SCHEMA.data_types.keys(),
            ),
        )

        # Act
        dimensions = ["state_code", "metric", "year", "month"]
        prison_population_metric = metric_calculator.CalculatedMetric(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[corrections.PopulationType.PRISON],
            aggregated_dimensions={
                "state_code": metric_calculator.Aggregation(
                    dimension=location.State, comprehensive=False
                )
            },
            output_name="POP",
        )
        results = self.query_view_for_builder(
            corrections_metrics.CorrectionsOutputViewBuilder(
                dataset_id="fake-dataset",
                metric_to_calculate=prison_population_metric,
                input_view=SimpleBigQueryViewBuilder(
                    dataset_id="justice_counts",
                    view_id="metric_calculator",
                    description="metric_calculator view",
                    view_query_template="",
                ),
            ),
            data_types={"year": int, "month": int, "value": int},
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                [
                    "US_XX",
                    "POP",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-01"),
                    "INSTANT",
                    ["A", "B"],
                    3000,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-01"),
                    "INSTANT",
                    ["B", "C"],
                    4000,
                ]
                + [None] * 4,
                [
                    "US_YY",
                    "POP",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "YY",
                    "yy.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-02"),
                    "INSTANT",
                    ["A", "B"],
                    1000,
                ]
                + [None] * 4,
                [
                    "US_YY",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "YY",
                    "yy.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-02"),
                    "INSTANT",
                    ["B", "C"],
                    1020,
                ]
                + [None] * 4,
                [
                    "US_ZZ",
                    "POP",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "ZZ",
                    "zz.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-02"),
                    "INSTANT",
                    ["A", "B"],
                    400,
                ]
                + [None] * 4,
                [
                    "US_ZZ",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "ZZ",
                    "zz.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-02"),
                    "INSTANT",
                    ["B", "C"],
                    500,
                ]
                + [None] * 4,
            ],
            columns=[
                "state_code",
                "metric",
                "year",
                "month",
                "date_reported",
                "source_name",
                "source_url",
                "report_name",
                "date_published",
                "measurement_type",
                "raw_source_categories",
                "value",
                "compared_to_year",
                "compared_to_month",
                "value_change",
                "percentage_change",
            ],
        )
        expected = expected.set_index(dimensions)
        assert_frame_equal(expected, results)

    def test_comparisons(self) -> None:
        """Tests that percentage change is correct, or null when the prior value was zero"""
        # Arrange
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="source_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Source.__table__),
            mock_data=pd.DataFrame([[1, "XX"]], columns=["id", "name"]),
        )
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="report_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Report.__table__),
            mock_data=pd.DataFrame(
                [
                    [
                        1,
                        1,
                        "_",
                        "All",
                        "2021-01-01",
                        "xx.gov",
                        "MANUALLY_ENTERED",
                        "John",
                    ]
                ],
                columns=[
                    "id",
                    "source_id",
                    "type",
                    "instance",
                    "publish_date",
                    "url",
                    "acquisition_method",
                    "acquired_by",
                ],
            ),
        )
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="metric_calculator",
            mock_schema=self.INPUT_SCHEMA,
            mock_data=pd.DataFrame(
                [
                    row(1, "2021-01-01", "2022-01-01", (FakeState("US_XX"),), [], 3)
                    + (datetime.date.fromisoformat("2021-02-01"), 0, "US_XX"),
                    row(1, "2021-01-01", "2021-01-01", (FakeState("US_XX"),), [], 0)
                    + (datetime.date.fromisoformat("2020-02-01"), 2, "US_XX"),
                    row(1, "2021-01-01", "2020-01-01", (FakeState("US_XX"),), [], 2)
                    + (None, None, "US_XX"),
                ],
                columns=self.INPUT_SCHEMA.data_types.keys(),
            ),
        )

        # Act
        dimensions = ["state_code", "metric", "year", "month"]
        parole_population = metric_calculator.CalculatedMetric(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.ADMISSIONS,
            filtered_dimensions=[],
            aggregated_dimensions={
                "state_code": metric_calculator.Aggregation(
                    dimension=location.State, comprehensive=False
                )
            },
            output_name="ADMISSIONS",
        )
        results = self.query_view_for_builder(
            corrections_metrics.CorrectionsOutputViewBuilder(
                dataset_id="fake-dataset",
                metric_to_calculate=parole_population,
                input_view=SimpleBigQueryViewBuilder(
                    dataset_id="justice_counts",
                    view_id="metric_calculator",
                    description="metric_calculator view",
                    view_query_template="",
                ),
            ),
            data_types={"year": int, "month": int, "value": int},
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                [
                    "US_XX",
                    "ADMISSIONS",
                    2020,
                    1,
                    datetime.date.fromisoformat("2020-01-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-01"),
                    "INSTANT",
                    [],
                    2,
                    None,
                    None,
                    None,
                    None,
                ],
                [
                    "US_XX",
                    "ADMISSIONS",
                    2021,
                    1,
                    datetime.date.fromisoformat("2021-01-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-01"),
                    "INSTANT",
                    [],
                    0,
                    2020,
                    1,
                    -2,
                    -1.00,
                ],
                # Percentage change is None as prior value was 0
                [
                    "US_XX",
                    "ADMISSIONS",
                    2022,
                    1,
                    datetime.date.fromisoformat("2022-01-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-01"),
                    "INSTANT",
                    [],
                    3,
                    2021,
                    1,
                    3,
                    None,
                ],
            ],
            columns=[
                "state_code",
                "metric",
                "year",
                "month",
                "date_reported",
                "source_name",
                "source_url",
                "report_name",
                "date_published",
                "measurement_type",
                "raw_source_categories",
                "value",
                "compared_to_year",
                "compared_to_month",
                "value_change",
                "percentage_change",
            ],
        )
        expected = expected.set_index(dimensions)
        assert_frame_equal(expected, results)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class CorrectionsMetricsIntegrationTest(BaseViewTest):
    """Tests the Justice Counts Prison Population view."""

    def test_recent_population(self) -> None:
        """Tests the basic use case of calculating population"""
        # Arrange
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="source_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Source.__table__),
            mock_data=pd.DataFrame(
                [[1, "XX"], [2, "YY"], [3, "ZZ"]], columns=["id", "name"]
            ),
        )
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="report_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Report.__table__),
            mock_data=pd.DataFrame(
                [
                    [
                        1,
                        1,
                        "_",
                        "All",
                        "2021-01-01",
                        "xx.gov",
                        "MANUALLY_ENTERED",
                        "John",
                    ],
                    [
                        2,
                        2,
                        "_",
                        "All",
                        "2021-01-02",
                        "yy.gov",
                        "MANUALLY_ENTERED",
                        "Jane",
                    ],
                    [
                        3,
                        3,
                        "_",
                        "All",
                        "2021-01-02",
                        "zz.gov",
                        "MANUALLY_ENTERED",
                        "Jude",
                    ],
                ],
                columns=[
                    "id",
                    "source_id",
                    "type",
                    "instance",
                    "publish_date",
                    "url",
                    "acquisition_method",
                    "acquired_by",
                ],
            ),
        )
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="report_table_definition_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(
                schema.ReportTableDefinition.__table__
            ),
            mock_data=pd.DataFrame(
                [
                    [
                        1,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["global/location/state", "metric/population/type"],
                        ["US_XX", "PRISON"],
                        [],
                    ],
                    [
                        2,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["global/location/state"],
                        ["US_YY"],
                        ["metric/population/type"],
                    ],
                    [
                        3,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["global/location/state", "metric/population/type"],
                        ["US_ZZ", "PRISON"],
                        ["global/gender"],
                    ],
                ],
                columns=[
                    "id",
                    "system",
                    "metric_type",
                    "measurement_type",
                    "filtered_dimensions",
                    "filtered_dimension_values",
                    "aggregated_dimensions",
                ],
            ),
        )
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="report_table_instance_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(
                schema.ReportTableInstance.__table__
            ),
            mock_data=pd.DataFrame(
                [
                    [1, 1, 1, "2020-11-30", "2020-12-01", None],
                    [2, 1, 1, "2020-12-31", "2021-01-01", None],
                    [3, 2, 2, "2020-11-30", "2020-12-01", None],
                    [4, 2, 2, "2020-12-31", "2021-01-01", None],
                    [5, 3, 3, "2020-11-30", "2020-12-01", None],
                    [6, 3, 3, "2020-12-31", "2021-01-01", None],
                ],
                columns=[
                    "id",
                    "report_id",
                    "report_table_definition_id",
                    "time_window_start",
                    "time_window_end",
                    "methodology",
                ],
            ),
        )
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="cell_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Cell.__table__),
            mock_data=pd.DataFrame(
                [
                    [1, 1, [], 3000],
                    [2, 2, [], 4000],
                    [3, 3, ["PRISON"], 1000],
                    [4, 3, ["PAROLE"], 5000],
                    [5, 4, ["PRISON"], 1020],
                    [6, 4, ["PAROLE"], 5020],
                    [7, 5, ["FEMALE"], 100],
                    [8, 5, ["MALE"], 300],
                    [9, 6, ["FEMALE"], 150],
                    [10, 6, ["MALE"], 350],
                ],
                columns=[
                    "id",
                    "report_table_instance_id",
                    "aggregated_dimension_values",
                    "value",
                ],
            ),
        )

        # Act
        dimensions = ["state_code", "metric", "year", "month"]
        prison_population_metric = metric_calculator.CalculatedMetric(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[corrections.PopulationType.PRISON],
            aggregated_dimensions={
                "state_code": metric_calculator.Aggregation(
                    dimension=location.State, comprehensive=False
                )
            },
            output_name="POP",
        )
        with patch.object(corrections_metrics, "METRICS", [prison_population_metric]):
            view_collector = (
                corrections_metrics.CorrectionsMetricsBigQueryViewCollector()
            )

            for view in view_collector.collect_view_builders():
                self.create_view(view)

            monthly_results = self.query_view_for_builder(
                view_collector.monthly_unified_builder,
                data_types={"year": int, "month": int, "value": int},
                dimensions=dimensions,
            )
            annual_results = self.query_view_for_builder(
                view_collector.annual_unified_builder,
                data_types={"year": int, "month": int, "value": int},
                dimensions=dimensions,
            )

        # Assert
        monthly_expected = pd.DataFrame(
            [
                [
                    "US_XX",
                    "POP",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-01"),
                    "INSTANT",
                    [],
                    3000,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-01"),
                    "INSTANT",
                    [],
                    4000,
                ]
                + [None] * 4,
                [
                    "US_YY",
                    "POP",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "YY",
                    "yy.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-02"),
                    "INSTANT",
                    [],
                    1000,
                ]
                + [None] * 4,
                [
                    "US_YY",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "YY",
                    "yy.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-02"),
                    "INSTANT",
                    [],
                    1020,
                ]
                + [None] * 4,
                [
                    "US_ZZ",
                    "POP",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "ZZ",
                    "zz.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-02"),
                    "INSTANT",
                    [],
                    400,
                ]
                + [None] * 4,
                [
                    "US_ZZ",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "ZZ",
                    "zz.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-02"),
                    "INSTANT",
                    [],
                    500,
                ]
                + [None] * 4,
            ],
            columns=[
                "state_code",
                "metric",
                "year",
                "month",
                "date_reported",
                "source_name",
                "source_url",
                "report_name",
                "date_published",
                "measurement_type",
                "raw_source_categories",
                "value",
                "compared_to_year",
                "compared_to_month",
                "value_change",
                "percentage_change",
            ],
        )
        monthly_expected = monthly_expected.set_index(dimensions)
        assert_frame_equal(monthly_expected, monthly_results)

        annual_expected = pd.DataFrame(
            [
                [
                    "US_XX",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-01"),
                    "INSTANT",
                    [],
                    4000,
                ]
                + [None] * 4,
                [
                    "US_YY",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "YY",
                    "yy.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-02"),
                    "INSTANT",
                    [],
                    1020,
                ]
                + [None] * 4,
                [
                    "US_ZZ",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "ZZ",
                    "zz.gov",
                    "_",
                    datetime.date.fromisoformat("2021-01-02"),
                    "INSTANT",
                    [],
                    500,
                ]
                + [None] * 4,
            ],
            columns=[
                "state_code",
                "metric",
                "year",
                "month",
                "date_reported",
                "source_name",
                "source_url",
                "report_name",
                "date_published",
                "measurement_type",
                "raw_source_categories",
                "value",
                "compared_to_year",
                "compared_to_month",
                "value_change",
                "percentage_change",
            ],
        )
        annual_expected = annual_expected.set_index(dimensions)
        assert_frame_equal(annual_expected, annual_results)
