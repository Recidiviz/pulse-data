# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for aggregate_views.py."""

import datetime
from unittest import TestCase

import pandas as pd

import recidiviz.common.constants.aggregate.enum_canonical_strings as enum_strings
from recidiviz.calculator.query.county.views.state_aggregates import \
    combined_state_aggregate
from recidiviz.persistence.database.base_schema import \
    JailsBase
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import \
    CaFacilityAggregate, FlFacilityAggregate
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.utils import fakes


class TestAggregateView(TestCase):
    """Test for combining aggregate reports into a BQ view."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(JailsBase)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def test_toQuery(self) -> None:
        # Arrange
        ca_data = pd.DataFrame({
            'jurisdiction_name': ['Alameda Sheriff\'s Dept.',
                                  'Alameda Sheriff\'s Dept.'],
            'facility_name': ['Glen Dyer Jail', 'Santa Rita Jail'],
            'average_daily_population': [379, 2043],
            'unsentenced_male_adp': [356, 1513],
            'unsentenced_female_adp': [0, 192],
            'sentenced_male_adp': [23, 301],
            'sentenced_female_adp': [0, 37],
            'report_date': 2 * [datetime.date(2017, 1, 31)],
            'fips': ['06001', '06001'],
            'aggregation_window': 2 * [enum_strings.monthly_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        })
        dao.write_df(CaFacilityAggregate, ca_data)

        fl_data = pd.DataFrame({
            'facility_name': [
                'Alachua CSO, Department of the Jail',
                'Alachua County Work Release',
                'Baker County Detention Center',
                'Bay County Jail and Annex',
                'Bradford County Jail'],
            'average_daily_population': [749., 50, 478, 1015, 141],
            'number_felony_pretrial': [463., 4, 81, 307, 50],
            'number_misdemeanor_pretrial': [45., 1, 15, 287, 9],
            'fips': ['12001', '12001', '12003', '12005', '12007'],
            'report_date': 5 * [datetime.date(2017, 1, 31)],
            'aggregation_window': 5 * [enum_strings.monthly_granularity],
            'report_frequency': 5 * [enum_strings.monthly_granularity]
        })
        dao.write_df(FlFacilityAggregate, fl_data)

        # Act
        # pylint: disable=protected-access
        query = SessionFactory.for_schema_base(JailsBase).query(
            combined_state_aggregate._UNIONED_STATEMENT)
        result = query.all()

        # Assert
        expected_custodial = [379, 2043, 749, 50, 478, 1015, 141]
        result_custodial = [row.custodial for row in result]
        self.assertCountEqual(result_custodial, expected_custodial)

        expected_felony_pretrial = [None, None, 463, 4, 81, 307, 50]
        result_felony_pretrial = [row.felony_pretrial for row in result]
        self.assertCountEqual(result_felony_pretrial, expected_felony_pretrial)

        # In CA: expected_male = sentenced_male_adp + unsentenced_male_adp
        expected_male = [23 + 356, 301 + 1513, None, None, None, None, None]
        result_male = [row.male for row in result]
        self.assertCountEqual(result_male, expected_male)
