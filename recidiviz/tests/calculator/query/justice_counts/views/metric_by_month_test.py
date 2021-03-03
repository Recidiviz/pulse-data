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
"""Tests various metrics that can be created from the metric_by_month template."""

from mock import Mock, patch
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.calculator.query.justice_counts.views import metric_by_month
from recidiviz.tests.calculator.query.view_test_util import (
    BaseViewTest,
    MockTableSchema,
)
from recidiviz.tools.justice_counts import manual_upload

_npd = np.datetime64


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class PrisonPopulationViewTest(BaseViewTest):
    """Tests the Justice Counts Prison Population view."""

    def test_recent_population(self) -> None:
        """Tests the basic use case of calculating population from various table definitions"""
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
        prison_population_metric = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[manual_upload.PopulationType.PRISON],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="POP",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=prison_population_metric
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
                    _npd("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    3000,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    4000,
                ]
                + [None] * 4,
                [
                    "US_YY",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "YY",
                    "yy.gov",
                    "_",
                    [],
                    1000,
                ]
                + [None] * 4,
                [
                    "US_YY",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "YY",
                    "yy.gov",
                    "_",
                    [],
                    1020,
                ]
                + [None] * 4,
                [
                    "US_ZZ",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "ZZ",
                    "zz.gov",
                    "_",
                    [],
                    400,
                ]
                + [None] * 4,
                [
                    "US_ZZ",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "ZZ",
                    "zz.gov",
                    "_",
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

    def test_multiple_filters(self) -> None:
        """Tests performing a query with multiple filters (population_type=SUPERVISION and supervision_type=PAROLE"""
        # Arrange
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="source_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Source.__table__),
            mock_data=pd.DataFrame(
                [[1, "AA"], [2, "XX"], [3, "YY"], [4, "ZZ"]], columns=["id", "name"]
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
                        "aa.gov",
                        "MANUALLY_ENTERED",
                        "John",
                    ],
                    [
                        2,
                        2,
                        "_",
                        "All",
                        "2021-01-02",
                        "xx.gov",
                        "MANUALLY_ENTERED",
                        "Jane",
                    ],
                    [
                        3,
                        3,
                        "_",
                        "All",
                        "2021-01-02",
                        "yy.gov",
                        "MANUALLY_ENTERED",
                        "Jane",
                    ],
                    [
                        4,
                        4,
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
                # Filtered by one filter, aggregated by the other
                [
                    [
                        1,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["global/location/state", "metric/population/type"],
                        ["US_AA", "SUPERVISION"],
                        ["metric/supervision/type", "metric/supervision/type/raw"],
                    ],
                    # Aggregated by both filters
                    [
                        2,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["global/location/state"],
                        ["US_XX"],
                        [
                            "metric/population/type",
                            "metric/supervision/type",
                            "metric/supervision/type/raw",
                        ],
                    ],
                    # Filtered by both filters
                    [
                        3,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        [
                            "global/location/state",
                            "metric/population/type",
                            "metric/supervision/type",
                        ],
                        ["US_YY", "SUPERVISION", "PAROLE"],
                        ["global/gender"],
                    ],
                    # Filtered incorrectly
                    [
                        4,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        [
                            "global/location/state",
                            "metric/population/type",
                            "metric/supervision/type",
                        ],
                        ["US_ZZ", "SUPERVISION", "PROBATION"],
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
                    [7, 4, 4, "2020-11-30", "2020-12-01", None],
                    [8, 4, 4, "2020-12-31", "2021-01-01", None],
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
                    [1, 1, ["PAROLE", "Parole"], 3000],
                    [2, 1, ["PROBATION", "Probation"], 1000],
                    [3, 2, ["PAROLE", "Parole"], 4000],
                    [4, 2, ["PROBATION", "Probation"], 1500],
                    [5, 3, ["PRISON", "", "Prison"], 1000],
                    [6, 3, ["SUPERVISION", "PAROLE", "Parole"], 5000],
                    [7, 3, ["SUPERVISION", "PROBATION", "Probation"], 3000],
                    [8, 4, ["PRISON", "", "Prison"], 1001],
                    [9, 4, ["SUPERVISION", "PAROLE", "Parole"], 5001],
                    [10, 4, ["SUPERVISION", "PROBATION", "Probation"], 3001],
                    [9, 5, ["FEMALE"], 100],
                    [10, 5, ["MALE"], 300],
                    [11, 6, ["FEMALE"], 150],
                    [12, 6, ["MALE"], 350],
                    [12, 7, ["FEMALE"], 800],
                    [14, 7, ["MALE"], 2000],
                    [15, 8, ["FEMALE"], 850],
                    [16, 8, ["MALE"], 2050],
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
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[
                manual_upload.PopulationType.SUPERVISION,
                manual_upload.SupervisionType.PAROLE,
            ],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="POP",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
            ),
            data_types={"year": int, "month": int, "value": int},
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                [
                    "US_AA",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "AA",
                    "aa.gov",
                    "_",
                    ["Parole"],
                    3000,
                ]
                + [None] * 4,
                [
                    "US_AA",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "AA",
                    "aa.gov",
                    "_",
                    ["Parole"],
                    4000,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    ["Parole"],
                    5000,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    ["Parole"],
                    5001,
                ]
                + [None] * 4,
                [
                    "US_YY",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "YY",
                    "yy.gov",
                    "_",
                    [],
                    400,
                ]
                + [None] * 4,
                [
                    "US_YY",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "YY",
                    "yy.gov",
                    "_",
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

    def test_aggregated(self) -> None:
        """Tests aggregating a dimension other than state code (in this case gender) and keeping it in the output"""
        # Arrange
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="source_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Source.__table__),
            mock_data=pd.DataFrame(
                [[1, "XX"], [2, "YY"], [3, "ZZ"], [4, "XA"]], columns=["id", "name"]
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
                    [
                        4,
                        4,
                        "_",
                        "All",
                        "2021-01-02",
                        "xa.gov",
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
                # Matches
                [
                    [
                        1,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["global/location/state", "metric/population/type"],
                        ["US_XX", "PRISON"],
                        ["global/gender", "global/gender/raw"],
                    ],
                    # Not aggregated
                    [
                        2,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["global/location/state", "metric/population/type"],
                        ["US_YY", "PRISON"],
                        [],
                    ],
                    # Filtered instead, not comprehensive
                    [
                        3,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        [
                            "global/location/state",
                            "metric/population/type",
                            "global/gender",
                        ],
                        ["US_ZZ", "PRISON", "FEMALE"],
                        [],
                    ],
                    # Matches - aggregated further
                    [
                        4,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["global/location/state", "metric/population/type"],
                        ["US_XA", "PRISON"],
                        [
                            "global/gender",
                            "global/gender/raw",
                            "source/XA/facility/raw",
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
                    [5, 3, 3, "2020-11-30", "2020-12-01", None],
                    [6, 3, 3, "2020-12-31", "2021-01-01", None],
                    [7, 4, 4, "2020-11-30", "2020-12-01", None],
                    [8, 4, 4, "2020-12-31", "2021-01-01", None],
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
                    [1, 1, ["MALE", "Male"], 3000],
                    [2, 1, ["FEMALE", "Female"], 1000],
                    [3, 2, ["MALE", "Male"], 4000],
                    [4, 2, ["FEMALE", "Female"], 1500],
                    [5, 3, [], 1000],
                    [6, 4, [], 1001],
                    [7, 5, [], 100],
                    [8, 6, [], 300],
                    [9, 7, ["MALE", "Male", "Onsite"], 200],
                    [10, 7, ["MALE", "Male", "Offsite"], 20],
                    [11, 7, ["FEMALE", "Female", "Onsite"], 100],
                    [12, 7, ["FEMALE", "Female", "Offsite"], 10],
                    [13, 8, ["MALE", "Male", "Onsite"], 300],
                    [14, 8, ["MALE", "Male", "Offsite"], 30],
                    [15, 8, ["FEMALE", "Female", "Onsite"], 200],
                    [16, 8, ["FEMALE", "Female", "Offsite"], 20],
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
        dimensions = ["state_code", "gender", "metric", "year", "month"]
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[manual_upload.PopulationType.PRISON],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                ),
                "gender": metric_by_month.Aggregation(
                    dimension=manual_upload.Gender, comprehensive=True
                ),
            },
            output_name="POP",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
            ),
            data_types={"year": int, "month": int, "value": int},
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                [
                    "US_XA",
                    "FEMALE",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "XA",
                    "xa.gov",
                    "_",
                    ["Female", "Offsite", "Onsite"],
                    110,
                ]
                + [None] * 4,
                [
                    "US_XA",
                    "FEMALE",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "XA",
                    "xa.gov",
                    "_",
                    ["Female", "Offsite", "Onsite"],
                    220,
                ]
                + [None] * 4,
                [
                    "US_XA",
                    "MALE",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "XA",
                    "xa.gov",
                    "_",
                    ["Male", "Offsite", "Onsite"],
                    220,
                ]
                + [None] * 4,
                [
                    "US_XA",
                    "MALE",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "XA",
                    "xa.gov",
                    "_",
                    ["Male", "Offsite", "Onsite"],
                    330,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "FEMALE",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    ["Female"],
                    1000,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "FEMALE",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    ["Female"],
                    1500,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "MALE",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    ["Male"],
                    3000,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "MALE",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    ["Male"],
                    4000,
                ]
                + [None] * 4,
            ],
            columns=[
                "state_code",
                "gender",
                "metric",
                "year",
                "month",
                "date_reported",
                "source_name",
                "source_url",
                "report_name",
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

    def test_multiple_sources_same_data(self) -> None:
        """Tests prioritization of sources -- when multiple provide matching data it picks the most recent"""
        # Arrange
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="source_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Source.__table__),
            mock_data=pd.DataFrame(
                [[1, "XX"], [2, "XX Courts"]], columns=["id", "name"]
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
                        "courts.xx.gov",
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
                        ["US_XX", "SUPERVISION"],
                        [],
                    ]
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
                # Source 1 reports data in the middle of the month
                [
                    [1, 1, 1, "2020-11-15", "2020-11-16", None],
                    [2, 1, 1, "2020-12-15", "2020-12-16", None],
                    # Source 2 reports at the start and end of the month. The end of the month values should be used.
                    [3, 2, 1, "2020-11-01", "2020-11-02", None],
                    [4, 2, 1, "2020-11-30", "2020-12-01", None],
                    [5, 2, 1, "2020-12-01", "2020-12-02", None],
                    [6, 2, 1, "2020-12-31", "2021-01-01", None],
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
                    [1, 1, [], 995],
                    [2, 2, [], 1005],
                    [3, 3, [], 991],
                    [4, 4, [], 1000],
                    [5, 5, [], 1001],
                    [6, 6, [], 1010],
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
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[manual_upload.PopulationType.SUPERVISION],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="POP",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
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
                    _npd("2020-11-30"),
                    "XX Courts",
                    "courts.xx.gov",
                    "_",
                    [],
                    1000,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "XX Courts",
                    "courts.xx.gov",
                    "_",
                    [],
                    1010,
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

    def test_multiple_tables_same_source(self) -> None:
        """Tests prioritization of data within a source when multiple tables provide matching data

        It should pick the one with the fewest dimensions to aggregate away.
        """
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
                        ["US_XX", "SUPERVISION"],
                        ["metric/supervision/type"],
                    ],
                    # Grouped by supervision type, which is unnecessary
                    [
                        2,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["global/location/state", "metric/population/type"],
                        ["US_XX", "SUPERVISION"],
                        [],
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
                    [3, 1, 2, "2020-11-30", "2020-12-01", None],
                    [4, 1, 2, "2020-12-31", "2021-01-01", None],
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
                    [1, 1, ["PAROLE"], 450],
                    [2, 1, ["PROBATION"], 450],
                    [3, 2, ["PAROLE"], 900],
                    [4, 2, ["PROBATION"], 900],
                    [5, 3, [], 1000],
                    [6, 4, [], 2000],
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
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[manual_upload.PopulationType.SUPERVISION],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="POP",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
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
                    _npd("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    1000,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    2000,
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

    def test_aggregate_with_nulls(self) -> None:
        """Tests that aggregations work correctly with null dimension values (mapped to empty string in BQ)"""
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
                        ["global/race", "global/ethnicity"],
                    ]
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
                [[1, 1, 1, "2020-11-30", "2020-12-01", None]],
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
                    [1, 1, ["BLACK", ""], 101],
                    [2, 1, ["WHITE", ""], 102],
                    [3, 1, ["", "HISPANIC"], 103],
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
        dimensions = ["state_code", "race", "metric", "year", "month"]
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[manual_upload.PopulationType.PRISON],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                ),
                "race": metric_by_month.Aggregation(
                    dimension=manual_upload.Race, comprehensive=True
                ),
            },
            output_name="POP",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
            ),
            data_types={"year": int, "month": int, "value": int},
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                [
                    "US_XX",
                    "",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    103,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "BLACK",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    101,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "WHITE",
                    "POP",
                    2020,
                    11,
                    _npd("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    102,
                ]
                + [None] * 4,
            ],
            columns=[
                "state_code",
                "race",
                "metric",
                "year",
                "month",
                "date_reported",
                "source_name",
                "source_url",
                "report_name",
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

    def test_delta_less_than_month(self) -> None:
        """Tests that delta metrics covering windows less than a month are summed within a report"""
        # Arrange
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="source_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Source.__table__),
            mock_data=pd.DataFrame(
                [[1, "XX"], [2, "XX Courts"]], columns=["id", "name"]
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
                        "2021-01-02",
                        "xx.gov",
                        "MANUALLY_ENTERED",
                        "John",
                    ],
                    [
                        2,
                        2,
                        "_",
                        "All",
                        "2021-01-01",
                        "courts.xx.gov",
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
                        "ADMISSIONS",
                        "DELTA",
                        ["global/location/state"],
                        ["US_XX"],
                        [],
                    ]
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
                # November - matches month
                [
                    [1, 1, 1, "2020-11-01", "2020-11-15", None],
                    [2, 1, 1, "2020-11-15", "2020-12-01", None],
                    # December - reports on Mondays, doesn't align with month, skipped
                    [3, 1, 1, "2020-11-29", "2020-12-06", None],
                    [4, 1, 1, "2020-12-06", "2020-12-13", None],
                    [5, 1, 1, "2020-12-13", "2020-12-20", None],
                    [6, 1, 1, "2020-12-20", "2020-12-27", None],
                    # January - doesn't cover month, also skipped
                    [7, 1, 1, "2020-12-27", "2021-01-03", None],
                    # February - overlapping, all still get counted
                    [8, 1, 1, "2021-02-01", "2021-02-21", None],
                    [9, 1, 1, "2021-02-07", "2021-03-01", None],
                    [10, 2, 1, "2021-02-07", "2021-03-01", None],
                ],  # different source should not be summed
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
                    [1, 1, [], 1],
                    [2, 2, [], 2],
                    [3, 3, [], 4],
                    [4, 4, [], 8],
                    [5, 5, [], 16],
                    [6, 6, [], 32],
                    [7, 7, [], 64],
                    [8, 8, [], 128],
                    [9, 9, [], 256],
                    [10, 10, [], 512],
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
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.ADMISSIONS,
            filtered_dimensions=[],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="ADMISSIONS",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
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
                    11,
                    _npd("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    3,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "ADMISSIONS",
                    2021,
                    2,
                    _npd("2021-02-28"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    384,
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

    def test_delta_more_than_month(self) -> None:
        """Tests that delta metrics covering windows more than a month are accounted to the month of their end date"""
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
            table_id="report_table_definition_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(
                schema.ReportTableDefinition.__table__
            ),
            mock_data=pd.DataFrame(
                [
                    [
                        1,
                        "CORRECTIONS",
                        "ADMISSIONS",
                        "DELTA",
                        ["global/location/state"],
                        ["US_XX"],
                        [],
                    ]
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
                # Quarters, attributed to month of end date
                [
                    [1, 1, 1, "2020-01-01", "2020-04-01", None],
                    [2, 1, 1, "2020-04-01", "2020-07-01", None],
                    [3, 1, 1, "2020-07-01", "2020-10-01", None],
                    [4, 1, 1, "2020-10-01", "2021-01-01", None],
                    # Same for years
                    [5, 1, 1, "2019-01-01", "2020-01-01", None],
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
                    [1, 1, [], 1],
                    [2, 2, [], 2],
                    [3, 3, [], 4],
                    [4, 4, [], 8],
                    [5, 5, [], 16],
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
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.ADMISSIONS,
            filtered_dimensions=[],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="ADMISSIONS",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
            ),
            data_types={"year": int, "month": int, "value": int},
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [],
            columns=[
                "state_code",
                "metric",
                "year",
                "month",
                "date_reported",
                "source_name",
                "source_url",
                "report_name",
                "raw_source_categories",
                "value",
                "compared_to_year",
                "compared_to_month",
                "value_change",
                "percentage_change",
            ],
        )
        expected = expected.astype({"year": int, "month": int, "value": int})
        expected = expected.set_index(dimensions)
        assert_frame_equal(expected, results)

    def test_average_less_than_month(self) -> None:
        """Tests that for average metrics covering windows less than a month, the last one in each month is used"""
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
                        "AVERAGE",
                        ["global/location/state"],
                        ["US_XX"],
                        [],
                    ]
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
                # November - split in halves
                [
                    [1, 1, 1, "2020-11-01", "2020-11-15", None],
                    [2, 1, 1, "2020-11-15", "2020-12-01", None],
                    # December - reports on Mondays
                    [3, 1, 1, "2020-11-29", "2020-12-06", None],
                    [4, 1, 1, "2020-12-06", "2020-12-13", None],
                    [5, 1, 1, "2020-12-13", "2020-12-20", None],
                    [6, 1, 1, "2020-12-20", "2020-12-27", None],
                    [7, 1, 1, "2020-12-27", "2021-01-03", None],
                    # February - overlapping
                    [8, 1, 1, "2021-02-01", "2021-02-21", None],
                    [9, 1, 1, "2021-02-07", "2021-03-01", None],
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
                    [1, 1, [], 1],
                    [2, 2, [], 2],
                    [3, 3, [], 4],
                    [4, 4, [], 8],
                    [5, 5, [], 16],
                    [6, 6, [], 32],
                    [7, 7, [], 64],
                    [8, 8, [], 128],
                    [9, 9, [], 256],
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
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="POP",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
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
                    _npd("2020-11-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    2,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-26"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    32,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2021,
                    1,
                    _npd("2021-01-02"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    64,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2021,
                    2,
                    _npd("2021-02-28"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    256,
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

    def test_average_more_than_month(self) -> None:
        """Tests that average metrics covering windows more than a month are accounted to the month of their end date"""
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
                        "AVERAGE",
                        ["global/location/state"],
                        ["US_XX"],
                        [],
                    ]
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
                # Quarters, attributed to month of end date
                [
                    [1, 1, 1, "2020-01-01", "2020-04-01", None],
                    [2, 1, 1, "2020-04-01", "2020-07-01", None],
                    [3, 1, 1, "2020-07-01", "2020-10-01", None],
                    [4, 1, 1, "2020-10-01", "2021-01-01", None],
                    # Same for years
                    [5, 1, 1, "2019-01-01", "2020-01-01", None],
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
                    [1, 1, [], 1],
                    [2, 2, [], 2],
                    [3, 3, [], 4],
                    [4, 4, [], 8],
                    [5, 5, [], 16],
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
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="POP",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
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
                    2019,
                    12,
                    _npd("2019-12-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    16,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    3,
                    _npd("2020-03-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    1,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    6,
                    _npd("2020-06-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    2,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    9,
                    _npd("2020-09-30"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    4,
                ]
                + [None] * 4,
                [
                    "US_XX",
                    "POP",
                    2020,
                    12,
                    _npd("2020-12-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    8,
                    2019,
                    12,
                    -8,
                    -0.5,
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
        """Tests comparison logic -- compares to most recent point that is at least a year older"""
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
                        ["global/location/state"],
                        ["US_XX"],
                        [],
                    ]
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
                    [1, 1, 1, "2022-01-01", "2022-01-02", None],  # Start of 2022-01
                    [2, 1, 1, "2021-01-01", "2021-01-02", None],  # Start of 2021-01
                    [3, 1, 1, "2020-01-31", "2020-02-01", None],  # End of 2020-01
                    [
                        4,
                        1,
                        1,
                        "2019-02-01",
                        "2019-02-02",
                        None,
                    ],  # Start of 2019-02 -- not quite a year back
                    [
                        5,
                        1,
                        1,
                        "2018-02-01",
                        "2018-02-02",
                        None,
                    ],  # Start of 2018-02 -- exactly a year prior
                    [6, 1, 1, "2017-03-01", "2017-03-02", None],
                ],  # Start of 2017-03 -- still not quite
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
                    [1, 1, [], 3],
                    [2, 2, [], 2],
                    [3, 3, [], 2],
                    [4, 4, [], 4],
                    [5, 5, [], 8],
                    [6, 6, [], 16],
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
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="POP",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
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
                    2017,
                    3,
                    _npd("2017-03-01"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    16,
                    None,
                    None,
                    None,
                    None,
                ],
                [
                    "US_XX",
                    "POP",
                    2018,
                    2,
                    _npd("2018-02-01"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    8,
                    None,
                    None,
                    None,
                    None,
                ],
                [
                    "US_XX",
                    "POP",
                    2019,
                    2,
                    _npd("2019-02-01"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    4,
                    2018,
                    2,
                    -4,
                    -0.50,
                ],
                [
                    "US_XX",
                    "POP",
                    2020,
                    1,
                    _npd("2020-01-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    2,
                    2018,
                    2,
                    -6,
                    -0.75,
                ],
                [
                    "US_XX",
                    "POP",
                    2021,
                    1,
                    _npd("2021-01-01"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    2,
                    2020,
                    1,
                    0,
                    0.00,
                ],
                [
                    "US_XX",
                    "POP",
                    2022,
                    1,
                    _npd("2022-01-01"),
                    "XX",
                    "xx.gov",
                    "_",
                    [],
                    3,
                    2021,
                    1,
                    1,
                    0.50,
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

    def test_comparisons_zero(self) -> None:
        """Tests that percentage change is null when the prior value was zero"""
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
            table_id="report_table_definition_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(
                schema.ReportTableDefinition.__table__
            ),
            mock_data=pd.DataFrame(
                [
                    [
                        1,
                        "CORRECTIONS",
                        "ADMISSIONS",
                        "DELTA",
                        ["global/location/state"],
                        ["US_XX"],
                        [],
                    ]
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
                    [1, 1, 1, "2022-01-01", "2022-02-01", None],
                    [2, 1, 1, "2021-01-01", "2021-02-01", None],
                    [3, 1, 1, "2020-01-01", "2020-02-01", None],
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
                [[1, 1, [], 3], [2, 2, [], 0], [3, 3, [], 2]],
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
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.ADMISSIONS,
            filtered_dimensions=[],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="ADMISSIONS",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
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
                    _npd("2020-01-31"),
                    "XX",
                    "xx.gov",
                    "_",
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
                    _npd("2021-01-31"),
                    "XX",
                    "xx.gov",
                    "_",
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
                    _npd("2022-01-31"),
                    "XX",
                    "xx.gov",
                    "_",
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

    def test_collapsed_dimensions(self) -> None:
        """Tests that percentage change is null when the prior value was zero"""
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
            table_id="report_table_definition_materialized",
            mock_schema=MockTableSchema.from_sqlalchemy_table(
                schema.ReportTableDefinition.__table__
            ),
            mock_data=pd.DataFrame(
                [
                    [
                        1,
                        "CORRECTIONS",
                        "ADMISSIONS",
                        "DELTA",
                        ["global/location/state"],
                        ["US_XX"],
                        ["metric/admission/type", "metric/admission/type/raw"],
                    ]
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
                    [1, 1, 1, "2021-01-01", "2021-02-01", None],
                    [2, 1, 1, "2020-01-01", "2020-02-01", None],
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
                    [1, 1, ["NEW_COMMITMENT", "A"], 3],
                    [2, 1, ["NEW_COMMITMENT", "B"], 2],
                    [3, 1, ["FROM_SUPERVISION", "C"], 1],
                    [4, 2, ["NEW_COMMITMENT", "B"], 6],
                    [5, 2, ["NEW_COMMITMENT", "D"], 4],
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
        parole_population = metric_by_month.CalculatedMetricByMonth(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.ADMISSIONS,
            filtered_dimensions=[manual_upload.AdmissionType.NEW_COMMITMENT],
            aggregated_dimensions={
                "state_code": metric_by_month.Aggregation(
                    dimension=manual_upload.State, comprehensive=False
                )
            },
            output_name="ADMISSIONS",
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id="fake-dataset", metric_to_calculate=parole_population
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
                    _npd("2020-01-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    ["B", "D"],
                    10,
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
                    _npd("2021-01-31"),
                    "XX",
                    "xx.gov",
                    "_",
                    ["A", "B"],
                    5,
                    2020,
                    1,
                    -5,
                    -0.5,
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
