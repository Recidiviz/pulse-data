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
"""Tests Jails metrics functionality."""
import datetime

from mock import Mock, patch

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy.sql import sqltypes

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.justice_counts.views import (
    jails_metrics,
    metric_calculator,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.tests.calculator.query.justice_counts.views.metric_calculator_test import (
    METRIC_CALCULATOR_SCHEMA,
    FakeState,
    row,
)
from recidiviz.tests.big_query.view_test_util import (
    BaseViewTest,
    MockTableSchema,
)
from recidiviz.tools.justice_counts import manual_upload


@patch("recidiviz.common.fips.validate_county_code", Mock(return_value=None))
@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class JailsOutputViewTest(BaseViewTest):
    """Tests the Jails output view."""

    INPUT_SCHEMA = MockTableSchema(
        {
            **METRIC_CALCULATOR_SCHEMA.data_types,
            "compare_date_partition": sqltypes.Date(),
            "compare_value": sqltypes.Numeric(),
            "state_code": sqltypes.String(255),
            "county_code": sqltypes.String(255),
            "percentage_covered_county": sqltypes.Float(),
            "percentage_covered_population": sqltypes.Float(),
        }
    )

    def test_county_population(self) -> None:
        """Tests the basic use case of calculating county population"""
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
                        (FakeState("US_XX"), manual_upload.County("US_XX_ALPHA")),
                        [],
                        3000,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_XX", "US_XX_ALPHA", None, None),
                    row(
                        1,
                        "2021-01-01",
                        "2020-11-30",
                        (FakeState("US_XX"), manual_upload.County("US_XX_BETA")),
                        [],
                        1000,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_XX", "US_XX_BETA", None, None),
                    row(
                        1,
                        "2021-01-01",
                        "2020-12-31",
                        (FakeState("US_XX"), manual_upload.County("US_XX_ALPHA")),
                        [],
                        4000,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_XX", "US_XX_ALPHA", None, None),
                    row(
                        1,
                        "2021-01-01",
                        "2020-12-31",
                        (FakeState("US_XX"), manual_upload.County("US_XX_BETA")),
                        [],
                        1500,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_XX", "US_XX_BETA", None, None),
                    row(
                        1,
                        "2021-01-01",
                        "2020-11-30",
                        (FakeState("US_YY"),),
                        ["US_YY_ALPHA", "US_YY_BETA"],
                        12000,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_YY", "", 0.12, 0.5),
                    row(
                        1,
                        "2021-01-01",
                        "2020-12-31",
                        (FakeState("US_YY"),),
                        ["US_YY_ALPHA", "US_YY_BETA"],
                        13000,
                        measurement_type="INSTANT",
                    )
                    + (None, None, "US_YY", "", 0.12, 0.5),
                ],
                columns=self.INPUT_SCHEMA.data_types.keys(),
            ),
        )

        # Act
        dimensions = ["state_code", "county_code", "metric", "year", "month"]
        results = self.query_view(
            jails_metrics.JailOutputViewBuilder(
                dataset_id="fake-dataset",
                metric_name="POP",
                aggregations={
                    "state_code": metric_calculator.Aggregation(
                        dimension=manual_upload.State, comprehensive=False
                    ),
                    "county_code": metric_calculator.Aggregation(
                        dimension=manual_upload.County, comprehensive=False
                    ),
                },
                value_column="value",
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
                    "US_XX_ALPHA",
                    "POP",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    3000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    "US_XX_ALPHA",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    4000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    "US_XX_BETA",
                    "POP",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    1000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    "US_XX_BETA",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    1500,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_YY",
                    "",
                    "POP",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    12000,
                ]
                + [None] * 4
                + [0.12, 0.5]
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_YY",
                    "",
                    "POP",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    13000,
                ]
                + [None] * 4
                + [0.12, 0.5]
                + [datetime.date.fromisoformat("2021-01-01")],
            ],
            columns=[
                "state_code",
                "county_code",
                "metric",
                "year",
                "month",
                "date_reported",
                "measurement_type",
                "value",
                "compared_to_year",
                "compared_to_month",
                "value_change",
                "percentage_change",
                "percentage_covered_county",
                "percentage_covered_population",
                "date_published",
            ],
        )
        expected = expected.set_index(dimensions)
        assert_frame_equal(expected, results)


@patch("recidiviz.common.fips.validate_county_code", Mock(return_value=None))
@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class JailsMetricsIntegrationTest(BaseViewTest):
    """Tests the Jails output view."""

    def test_county_population(self) -> None:
        """Tests the basic use case of calculating county population"""
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
                        ["US_XX", "JAIL"],
                        ["global/location/county", "global/location/county-fips"],
                    ],
                    [
                        2,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["metric/population/type"],
                        ["JAIL"],
                        [
                            "global/location/state",
                            "global/location/county",
                            "global/location/county-fips",
                        ],
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
                    [1, 1, ["US_XX_ALPHA", "00001"], 3000],
                    [2, 1, ["US_XX_BETA", "00002"], 1000],
                    [3, 2, ["US_XX_ALPHA", "00001"], 4000],
                    [4, 2, ["US_XX_BETA", "00002"], 1500],
                    [5, 3, ["US_YY", "US_YY_ALPHA", "01001"], 10000],
                    [6, 3, ["US_ZZ", "US_ZZ_ALPHA", "02001"], 20000],
                    [7, 4, ["US_YY", "US_YY_ALPHA", "01001"], 12000],
                    [8, 4, ["US_ZZ", "US_ZZ_ALPHA", "02001"], 22000],
                ],
                columns=[
                    "id",
                    "report_table_instance_id",
                    "aggregated_dimension_values",
                    "value",
                ],
            ),
        )
        self.create_mock_bq_table(
            dataset_id="external_reference",
            table_id="county_resident_populations",
            mock_schema=MockTableSchema(
                data_types={
                    "fips": sqltypes.String(255),
                    "year": sqltypes.Integer,
                    "population": sqltypes.Integer,
                }
            ),
            mock_data=pd.DataFrame(
                [
                    ["00001", 2010, 600_000],
                    ["00001", 2020, 800_000],
                    ["00002", 2010, 300_000],
                    ["00002", 2020, 500_000],
                    ["01001", 2010, 1_000_000],
                    ["01001", 2020, 1_200_000],
                    ["02001", 2010, 2_000_000],
                    ["02001", 2020, 2_200_000],
                ],
                columns=["fips", "year", "population"],
            ),
        )

        # Act
        dimensions = ["state_code", "county_code", "metric", "year", "month"]
        results = self.query_view_chain(
            jails_metrics.JailsMetricsBigQueryViewCollector().collect_view_builders(),
            data_types={"year": int, "month": int, "value": int},
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                [
                    "US_XX",
                    "US_XX_ALPHA",
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    375,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    "US_XX_ALPHA",
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    500,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    "US_XX_ALPHA",
                    "POPULATION_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    3000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    "US_XX_ALPHA",
                    "POPULATION_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    4000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    "US_XX_BETA",
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    200,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    "US_XX_BETA",
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    300,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    "US_XX_BETA",
                    "POPULATION_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    1000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    "US_XX_BETA",
                    "POPULATION_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    1500,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    np.nan,
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    307,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_XX",
                    np.nan,
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    423,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-01")],
                [
                    "US_YY",
                    "US_YY_ALPHA",
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    833,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_YY",
                    "US_YY_ALPHA",
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    1000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_YY",
                    "US_YY_ALPHA",
                    "POPULATION_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    10000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_YY",
                    "US_YY_ALPHA",
                    "POPULATION_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    12000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_YY",
                    np.nan,
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    833,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_YY",
                    np.nan,
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    1000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_ZZ",
                    "US_ZZ_ALPHA",
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    909,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_ZZ",
                    "US_ZZ_ALPHA",
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    1000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_ZZ",
                    "US_ZZ_ALPHA",
                    "POPULATION_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    20000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_ZZ",
                    "US_ZZ_ALPHA",
                    "POPULATION_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    22000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_ZZ",
                    np.nan,
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    11,
                    datetime.date.fromisoformat("2020-11-30"),
                    "INSTANT",
                    909,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
                [
                    "US_ZZ",
                    np.nan,
                    "INCARCERATION_RATE_JAIL",
                    2020,
                    12,
                    datetime.date.fromisoformat("2020-12-31"),
                    "INSTANT",
                    1000,
                ]
                + [None] * 6
                + [datetime.date.fromisoformat("2021-01-02")],
            ],
            columns=[
                "state_code",
                "county_code",
                "metric",
                "year",
                "month",
                "date_reported",
                "measurement_type",
                "value",
                "compared_to_year",
                "compared_to_month",
                "value_change",
                "percentage_change",
                "percentage_covered_county",
                "percentage_covered_population",
                "date_published",
            ],
        )
        expected = expected.set_index(dimensions)
        assert_frame_equal(expected, results)
