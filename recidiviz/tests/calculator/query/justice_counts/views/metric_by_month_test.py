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

import datetime
from typing import List, Optional, Tuple

import attr
from mock import Mock, patch
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy.sql import sqltypes

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common import date
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.calculator.query.justice_counts.views import metric_by_month
from recidiviz.tests.calculator.query.view_test_util import (
    BaseViewTest,
    MockTableSchema,
)
from recidiviz.tools.justice_counts import manual_upload

_npd = np.datetime64


@attr.s(frozen=True)
class TestState(manual_upload.State):
    # Change the type to str so that is supports any value.
    state_code: str = attr.ib()  # type: ignore[assignment]

    @property
    def dimension_value(self) -> str:
        return self.state_code


def row(
    source_and_report_id: int,
    start_date_str: str,
    dimensions: Tuple[manual_upload.Dimension, ...],
    raw_source_categories: List[str],
    value: int,
    end_date_str: Optional[str] = None,
) -> Tuple[int, str, List[int], _npd, _npd, _npd, str, str, List[str], int]:
    """Builds an expected output row for MetricByMonth.

    Makes a few assumptions that helps keep our tests from being even more verbose:
    - Source and Report id match (one report for each source)
    - If end date is not provided, defaults to the first day of the following month
    """
    start_date = datetime.date.fromisoformat(start_date_str)
    end_date = (
        datetime.date.fromisoformat(end_date_str)
        if end_date_str is not None
        else date.first_day_of_next_month(start_date)
    )

    dimension_strings = []
    for dimension in dimensions:
        dimension_value = dimension.dimension_value
        if not dimension_value:
            dimension_value = '\\"\\"'
        dimension_strings.append(
            f'"({dimension.dimension_identifier()},{dimension_value})"'
        )

    return (
        source_and_report_id,
        "_",
        [source_and_report_id],
        _npd(
            date.first_day_of_month(end_date - datetime.timedelta(days=1)).isoformat()
        ),
        _npd(start_date.isoformat()),
        _npd(end_date.isoformat()),
        f'{{{",".join(dimension_strings)}}}',
        "|".join(
            dimension.dimension_value
            for dimension in sorted(dimensions, key=lambda x: x.dimension_identifier())
        ),
        raw_source_categories,
        value,
    )


METRIC_BY_MONTH_SCHEMA = MockTableSchema(
    {
        "source_id": sqltypes.Integer(),
        "report_type": sqltypes.String(255),
        "report_ids": sqltypes.ARRAY(sqltypes.Integer),
        "start_of_month": sqltypes.Date(),
        "time_window_start": sqltypes.Date(),
        "time_window_end": sqltypes.Date(),
        "dimensions": sqltypes.String(255),
        "dimensions_string": sqltypes.String(255),
        "collapsed_dimension_values": sqltypes.ARRAY(sqltypes.String(255)),
        "value": sqltypes.Numeric(),
    }
)


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class MetricByMonthViewTest(BaseViewTest):
    """Tests the Justice Counts metric by month view."""

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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                row(1, "2020-11-30", (TestState("US_XX"),), [], 3000),
                row(1, "2020-12-31", (TestState("US_XX"),), [], 4000),
                row(2, "2020-11-30", (TestState("US_YY"),), [], 1000),
                row(2, "2020-12-31", (TestState("US_YY"),), [], 1020),
                row(3, "2020-11-30", (TestState("US_ZZ"),), [], 400),
                row(3, "2020-12-31", (TestState("US_ZZ"),), [], 500),
            ],
            columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                row(1, "2020-11-30", (TestState("US_AA"),), ["Parole"], 3000),
                row(1, "2020-12-31", (TestState("US_AA"),), ["Parole"], 4000),
                row(2, "2020-11-30", (TestState("US_XX"),), ["Parole"], 5000),
                row(2, "2020-12-31", (TestState("US_XX"),), ["Parole"], 5001),
                row(3, "2020-11-30", (TestState("US_YY"),), [], 400),
                row(3, "2020-12-31", (TestState("US_YY"),), [], 500),
            ],
            columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
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
                [[1, "XX"], [2, "YY"], [3, "ZZ"], [4, "XA"], [5, "BJS"], [6, "FED"]],
                columns=["id", "name"],
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
                    [
                        5,
                        5,
                        "_",
                        "All",
                        "2021-01-02",
                        "bjs.gov",
                        "MANUALLY_ENTERED",
                        "Jude",
                    ],
                    [
                        6,
                        6,
                        "_",
                        "All",
                        "2021-01-02",
                        "fed.gov",
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
                    # Not aggregated by gender
                    [
                        2,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["global/location/state", "metric/population/type"],
                        ["US_YY", "PRISON"],
                        [],
                    ],
                    # Filtered by gender instead, not comprehensive
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
                    # Matches - aggregated by state
                    [
                        5,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["metric/population/type"],
                        ["PRISON"],
                        ["global/location/state", "global/gender", "global/gender/raw"],
                    ],
                    # No state dimension
                    [
                        6,
                        "CORRECTIONS",
                        "POPULATION",
                        "INSTANT",
                        ["metric/population/type"],
                        ["PRISON"],
                        ["global/gender", "global/gender/raw"],
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
                    [9, 5, 5, "2020-11-30", "2020-12-01", None],
                    [10, 5, 5, "2020-12-31", "2021-01-01", None],
                    [11, 6, 6, "2020-11-30", "2020-12-01", None],
                    [12, 6, 6, "2020-12-31", "2021-01-01", None],
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
                    [17, 9, ["US_XB", "MALE", "Male"], 1],
                    [18, 9, ["US_XB", "FEMALE", "Female"], 2],
                    [19, 9, ["US_XC", "MALE", "Male"], 3],
                    [20, 9, ["US_XC", "FEMALE", "Female"], 4],
                    [21, 10, ["US_XB", "MALE", "Male"], 5],
                    [22, 10, ["US_XB", "FEMALE", "Female"], 6],
                    [23, 10, ["US_XC", "MALE", "Male"], 7],
                    [24, 10, ["US_XC", "FEMALE", "Female"], 8],
                    [25, 11, ["MALE", "Male"], 2_000_000],
                    [26, 11, ["FEMALE", "Female"], 200_000],
                    [27, 12, ["MALE", "Male"], 3_000_000],
                    [28, 12, ["FEMALE", "Female"], 300_000],
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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                row(
                    4,
                    "2020-11-30",
                    (TestState("US_XA"), manual_upload.Gender("FEMALE")),
                    ["Female", "Offsite", "Female", "Onsite"],
                    110,
                ),
                row(
                    4,
                    "2020-12-31",
                    (TestState("US_XA"), manual_upload.Gender("FEMALE")),
                    ["Female", "Offsite", "Female", "Onsite"],
                    220,
                ),
                row(
                    5,
                    "2020-11-30",
                    (TestState("US_XB"), manual_upload.Gender("FEMALE")),
                    ["Female"],
                    2,
                ),
                row(
                    5,
                    "2020-12-31",
                    (TestState("US_XB"), manual_upload.Gender("FEMALE")),
                    ["Female"],
                    6,
                ),
                row(
                    5,
                    "2020-11-30",
                    (TestState("US_XC"), manual_upload.Gender("FEMALE")),
                    ["Female"],
                    4,
                ),
                row(
                    5,
                    "2020-12-31",
                    (TestState("US_XC"), manual_upload.Gender("FEMALE")),
                    ["Female"],
                    8,
                ),
                row(
                    1,
                    "2020-11-30",
                    (TestState("US_XX"), manual_upload.Gender("FEMALE")),
                    ["Female"],
                    1000,
                ),
                row(
                    1,
                    "2020-12-31",
                    (TestState("US_XX"), manual_upload.Gender("FEMALE")),
                    ["Female"],
                    1500,
                ),
                row(
                    4,
                    "2020-11-30",
                    (TestState("US_XA"), manual_upload.Gender("MALE")),
                    ["Male", "Offsite", "Male", "Onsite"],
                    220,
                ),
                row(
                    4,
                    "2020-12-31",
                    (TestState("US_XA"), manual_upload.Gender("MALE")),
                    ["Male", "Offsite", "Male", "Onsite"],
                    330,
                ),
                row(
                    5,
                    "2020-11-30",
                    (TestState("US_XB"), manual_upload.Gender("MALE")),
                    ["Male"],
                    1,
                ),
                row(
                    5,
                    "2020-12-31",
                    (TestState("US_XB"), manual_upload.Gender("MALE")),
                    ["Male"],
                    5,
                ),
                row(
                    5,
                    "2020-11-30",
                    (TestState("US_XC"), manual_upload.Gender("MALE")),
                    ["Male"],
                    3,
                ),
                row(
                    5,
                    "2020-12-31",
                    (TestState("US_XC"), manual_upload.Gender("MALE")),
                    ["Male"],
                    7,
                ),
                row(
                    1,
                    "2020-11-30",
                    (TestState("US_XX"), manual_upload.Gender("MALE")),
                    ["Male"],
                    3000,
                ),
                row(
                    1,
                    "2020-12-31",
                    (TestState("US_XX"), manual_upload.Gender("MALE")),
                    ["Male"],
                    4000,
                ),
            ],
            columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                row(2, "2020-11-30", (TestState("US_XX"),), [], 1000),
                row(2, "2020-12-31", (TestState("US_XX"),), [], 1010),
            ],
            columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                row(1, "2020-11-30", (TestState("US_XX"),), [], 1000),
                row(1, "2020-12-31", (TestState("US_XX"),), [], 2000),
            ],
            columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                row(
                    1,
                    "2020-11-30",
                    (TestState("US_XX"), manual_upload.Race("")),
                    [],
                    103,
                ),
                row(
                    1,
                    "2020-11-30",
                    (TestState("US_XX"), manual_upload.Race("BLACK")),
                    [],
                    101,
                ),
                row(
                    1,
                    "2020-11-30",
                    (TestState("US_XX"), manual_upload.Race("WHITE")),
                    [],
                    102,
                ),
            ],
            columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
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
                    # different source should not be summed
                    [10, 2, 1, "2021-02-07", "2021-03-01", None],
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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                row(1, "2020-11-01", (TestState("US_XX"),), [], 3),
                row(1, "2021-02-01", (TestState("US_XX"),), [], 384),
            ],
            columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame([], columns=METRIC_BY_MONTH_SCHEMA.data_types.keys())
        expected = expected.astype(
            {"time_window_start": _npd, "time_window_end": _npd, "value": int}
        )
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
                [
                    # November - split in halves
                    [1, 1, 1, "2020-11-01", "2020-11-15", None],
                    [2, 1, 1, "2020-11-15", "2020-12-01", None],  # pick this
                    # December - reports on Mondays
                    [3, 1, 1, "2020-11-29", "2020-12-06", None],
                    [4, 1, 1, "2020-12-06", "2020-12-13", None],
                    [5, 1, 1, "2020-12-13", "2020-12-20", None],
                    [6, 1, 1, "2020-12-20", "2020-12-27", None],  # pick this
                    [7, 1, 1, "2020-12-27", "2021-01-03", None],  # pick this
                    # February - overlapping
                    [8, 1, 1, "2021-02-01", "2021-02-21", None],
                    [9, 1, 1, "2021-02-07", "2021-03-01", None],  # pick this
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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                row(
                    1,
                    "2020-11-15",
                    (TestState("US_XX"),),
                    [],
                    2,
                    end_date_str="2020-12-01",
                ),
                row(
                    1,
                    "2020-12-20",
                    (TestState("US_XX"),),
                    [],
                    32,
                    end_date_str="2020-12-27",
                ),
                row(
                    1,
                    "2020-12-27",
                    (TestState("US_XX"),),
                    [],
                    64,
                    end_date_str="2021-01-03",
                ),
                row(
                    1,
                    "2021-02-07",
                    (TestState("US_XX"),),
                    [],
                    256,
                    end_date_str="2021-03-01",
                ),
            ],
            columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
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
                [
                    # Quarters, attributed to month of end date
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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                row(
                    1,
                    "2019-01-01",
                    (TestState("US_XX"),),
                    [],
                    16,
                    end_date_str="2020-01-01",
                ),
                row(
                    1,
                    "2020-01-01",
                    (TestState("US_XX"),),
                    [],
                    1,
                    end_date_str="2020-04-01",
                ),
                row(
                    1,
                    "2020-04-01",
                    (TestState("US_XX"),),
                    [],
                    2,
                    end_date_str="2020-07-01",
                ),
                row(
                    1,
                    "2020-07-01",
                    (TestState("US_XX"),),
                    [],
                    4,
                    end_date_str="2020-10-01",
                ),
                row(
                    1,
                    "2020-10-01",
                    (TestState("US_XX"),),
                    [],
                    8,
                    end_date_str="2021-01-01",
                ),
            ],
            columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
        )
        expected = expected.set_index(dimensions)
        assert_frame_equal(expected, results)

    def test_collapsed_dimensions(self) -> None:
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
        dimensions = ["dimensions_string", "start_of_month"]
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
            data_types={
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
            },
            dimensions=dimensions,
        )

        # Assert
        expected = pd.DataFrame(
            [
                row(
                    1,
                    "2020-01-01",
                    (TestState("US_XX"),),
                    ["B", "D"],
                    10,
                ),
                row(
                    1,
                    "2021-01-01",
                    (TestState("US_XX"),),
                    ["A", "B"],
                    5,
                ),
            ],
            columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
        )
        expected = expected.set_index(dimensions)
        assert_frame_equal(expected, results)

    def test_comparisons(self) -> None:
        """Tests comparison logic -- compares to most recent point that is at least a year older"""
        # Arrange
        # Start of 2022-01
        row_2022 = row(
            1,
            "2022-01-01",
            (TestState("US_XX"),),
            [],
            3,
            end_date_str="2022-01-02",
        )
        # Start of 2021-01
        row_2021 = row(
            1,
            "2021-01-01",
            (TestState("US_XX"),),
            [],
            2,
            end_date_str="2021-01-02",
        )
        # End of 2020-01
        row_2020 = row(1, "2020-01-31", (TestState("US_XX"),), [], 2)
        # Start of 2019-02 -- not quite a year prior
        row_2019 = row(
            1,
            "2019-02-01",
            (TestState("US_XX"),),
            [],
            4,
            end_date_str="2019-02-02",
        )
        # Start of 2018-02 -- exactly a year prior
        row_2018 = row(
            1,
            "2018-02-01",
            (TestState("US_XX"),),
            [],
            8,
            end_date_str="2018-02-02",
        )
        # Start of 2017-03 -- still not quite
        row_2017 = row(
            1,
            "2017-03-01",
            (TestState("US_XX"),),
            [],
            16,
            end_date_str="2017-03-02",
        )
        self.create_mock_bq_table(
            dataset_id="justice_counts",
            table_id="metric_by_month",
            mock_schema=METRIC_BY_MONTH_SCHEMA,
            mock_data=pd.DataFrame(
                [row_2022, row_2021, row_2020, row_2019, row_2018, row_2017],
                columns=METRIC_BY_MONTH_SCHEMA.data_types.keys(),
            ),
        )

        # Act
        dimensions = ["dimensions_string", "start_of_month"]
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
            metric_by_month.CompareToPriorYearViewBuilder(
                dataset_id="fake-dataset",
                metric_to_calculate=parole_population,
                input_view=SimpleBigQueryViewBuilder(
                    dataset_id="justice_counts",
                    view_id="metric_by_month",
                    description="metric_by_month view",
                    view_query_template="",
                ),
            ),
            data_types={
                "start_of_month": _npd,
                "time_window_start": _npd,
                "time_window_end": _npd,
                "value": int,
                "compare_start_of_month": _npd,
            },
            dimensions=dimensions,
        )
        # The query excludes the ordinal column but that clause gets dropped
        # because postgres doesn't support it so we exclude it manually here.
        results = results.drop("ordinal", axis=1)

        # Assert
        expected = pd.DataFrame(
            [
                row_2017 + (None, None),
                row_2018 + (None, None),
                row_2019 + (_npd("2018-02-01"), 8),
                row_2020 + (_npd("2018-02-01"), 8),
                row_2021 + (_npd("2020-01-01"), 2),
                row_2022 + (_npd("2021-01-01"), 2),
            ],
            columns=[
                *METRIC_BY_MONTH_SCHEMA.data_types.keys(),
                "compare_start_of_month",
                "compare_value",
            ],
        )
        expected = expected.set_index(dimensions)
        assert_frame_equal(expected, results)
