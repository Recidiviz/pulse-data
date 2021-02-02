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
from recidiviz.tests.calculator.query.view_test_util import BaseViewTest, MockTableSchema
from recidiviz.tools.justice_counts import manual_upload


@patch('recidiviz.utils.metadata.project_id', Mock(return_value='t'))
class PrisonPopulationViewTest(BaseViewTest):
    """Tests the Justice Counts Prison Population view."""

    def test_recent_population(self):
        # Arrange
        self.create_mock_bq_table(
            dataset_id='justice_counts', table_id='report_materialized',
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Report.__table__),
            mock_data=pd.DataFrame(
                [[1, 1, 'Summary', 'All', '2021-01-01', 'https://www.xx.gov/doc', 'MANUALLY_ENTERED', 'John'],
                 [2, 2, 'Summary', 'All', '2021-01-02', 'https://www.doc.yy.gov', 'MANUALLY_ENTERED', 'Jane'],
                 [3, 3, 'Summary', 'All', '2021-01-02', 'https://doc.zz.gov', 'MANUALLY_ENTERED', 'Jude']],
                columns=['id', 'source_id', 'type', 'instance', 'publish_date', 'url', 'acquisition_method',
                         'acquired_by']))
        self.create_mock_bq_table(
            dataset_id='justice_counts', table_id='report_table_definition_materialized',
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.ReportTableDefinition.__table__),
            mock_data=pd.DataFrame(
                [[1, 'CORRECTIONS', 'POPULATION', 'INSTANT', ['global/location/state', 'metric/population/type'],
                  ['US_XX', 'PRISON'], []],
                 [2, 'CORRECTIONS', 'POPULATION', 'INSTANT', ['global/location/state'], ['US_YY'],
                  ['metric/population/type']],
                 [3, 'CORRECTIONS', 'POPULATION', 'INSTANT', ['global/location/state', 'metric/population/type'],
                  ['US_ZZ', 'PRISON'], ['global/gender']]],
                columns=['id', 'system', 'metric_type', 'measurement_type', 'filtered_dimensions',
                         'filtered_dimension_values', 'aggregated_dimensions']))
        self.create_mock_bq_table(
            dataset_id='justice_counts', table_id='report_table_instance_materialized',
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.ReportTableInstance.__table__),
            mock_data=pd.DataFrame(
                [[1, 1, 1, '2020-11-30', '2020-12-01', None],
                 [2, 1, 1, '2020-12-31', '2021-01-01', None],
                 [3, 2, 2, '2020-11-30', '2020-12-01', None],
                 [4, 2, 2, '2020-12-31', '2021-01-01', None],
                 [5, 3, 3, '2020-11-30', '2020-12-01', None],
                 [6, 3, 3, '2020-12-31', '2021-01-01', None]],
                columns=['id', 'report_id', 'report_table_definition_id', 'time_window_start', 'time_window_end',
                         'methodology']))
        self.create_mock_bq_table(
            dataset_id='justice_counts', table_id='cell_materialized',
            mock_schema=MockTableSchema.from_sqlalchemy_table(schema.Cell.__table__),
            mock_data=pd.DataFrame(
                [[1, 1, [], 3000],
                 [2, 2, [], 4000],
                 [3, 3, ['PRISON'], 1000],
                 [4, 3, ['PAROLE'], 5000],
                 [5, 4, ['PRISON'], 1020],
                 [6, 4, ['PAROLE'], 5020],
                 [7, 5, ['FEMALE'], 100],
                 [8, 5, ['MALE'], 300],
                 [9, 6, ['FEMALE'], 150],
                 [10, 6, ['MALE'], 350]],
                columns=['id', 'report_table_instance_id', 'aggregated_dimension_values', 'value']))

        # Act
        dimensions = ['state_code', 'metric', 'year', 'month']
        prison_population_metric = metric_by_month.CalculatedMetricByMonth(
           system=schema.System.CORRECTIONS,
           metric=schema.MetricType.POPULATION,
           filtered_dimensions=[manual_upload.PopulationType.PRISON],
           aggregated_dimensions={'state_code': metric_by_month.Aggregation(
               dimension=manual_upload.State, comprehensive=False)},
           output_name='POPULATION_PRISON'
        )
        results = self.query_view(
            metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id='fake-dataset', metric_to_calculate=prison_population_metric),
            data_types={'year': int, 'month': int, 'value': int},
            dimensions=dimensions)

        # Assert
        expected = pd.DataFrame(
                [['US_XX', 'POPULATION_PRISON', 2020, 11, np.datetime64('2020-11-30'), 3000, None, None, None, None],
                 ['US_XX', 'POPULATION_PRISON', 2020, 12, np.datetime64('2020-12-31'), 4000, None, None, None, None],
                 ['US_YY', 'POPULATION_PRISON', 2020, 11, np.datetime64('2020-11-30'), 1000, None, None, None, None],
                 ['US_YY', 'POPULATION_PRISON', 2020, 12, np.datetime64('2020-12-31'), 1020, None, None, None, None],
                 ['US_ZZ', 'POPULATION_PRISON', 2020, 11, np.datetime64('2020-11-30'), 400, None, None, None, None],
                 ['US_ZZ', 'POPULATION_PRISON', 2020, 12, np.datetime64('2020-12-31'), 500, None, None, None, None]],
                columns=['state_code', 'metric', 'year', 'month', 'date_reported', 'value', 'compared_to_year',
                         'compared_to_month', 'value_change', 'percentage_change'])
        expected = expected.set_index(dimensions)
        assert_frame_equal(expected, results)
