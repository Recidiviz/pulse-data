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

"""Tests for aggregate/dao.py."""

import datetime
from unittest import TestCase

import pandas as pd
from more_itertools import one
from sqlalchemy import func

from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import (
    FlCountyAggregate,
    FlFacilityAggregate,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.utils import fakes

DATE_SCRAPED = datetime.date(year=2019, month=1, day=1)


class TestDao(TestCase):
    """Test that the methods in dao.py correctly read from the SQL database."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testWriteDf(self):
        # Arrange
        subject = pd.DataFrame(
            {
                "county_name": ["Alachua", "Baker", "Bay", "Bradford", "Brevard"],
                "county_population": [257062, 26965, 176016, 27440, 568919],
                "average_daily_population": [799, 478, 1015, 141, 1547],
                "date_reported": [
                    pd.NaT,
                    pd.NaT,
                    datetime.datetime(year=2017, month=9, day=1),
                    pd.NaT,
                    pd.NaT,
                ],
                "fips": ["00000", "00001", "00002", "00003", "00004"],
                "report_date": 5 * [DATE_SCRAPED],
                "aggregation_window": 5 * [enum_strings.monthly_granularity],
                "report_frequency": 5 * [enum_strings.monthly_granularity],
            }
        )

        # Act
        dao.write_df(FlCountyAggregate, subject)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(FlCountyAggregate).filter(
                FlCountyAggregate.county_name == "Bay"
            )
            result = one(query.all())

            self.assertEqual(result.county_name, "Bay")
            self.assertEqual(result.county_population, 176016)
            self.assertEqual(result.average_daily_population, 1015)
            self.assertEqual(
                result.date_reported, datetime.date(year=2017, month=9, day=1)
            )
            self.assertEqual(result.fips, "00002")
            self.assertEqual(result.report_date, DATE_SCRAPED)
            self.assertEqual(
                result.aggregation_window, enum_strings.monthly_granularity
            )

    def testWriteDf_doesNotOverrideMatchingColumnNames(self):
        # Arrange
        subject = pd.DataFrame(
            {
                "county_name": ["Alachua", "Baker", "Bay", "Bradford", "Brevard"],
                "county_population": [257062, 26965, 176016, 27440, 568919],
                "average_daily_population": [799, 478, 1015, 141, 1547],
                "date_reported": [
                    pd.NaT,
                    pd.NaT,
                    datetime.datetime(year=2017, month=9, day=1),
                    pd.NaT,
                    pd.NaT,
                ],
                "fips": ["00000", "00001", "00002", "00003", "00004"],
                "report_date": 5 * [DATE_SCRAPED],
                "aggregation_window": 5 * [enum_strings.monthly_granularity],
                "report_frequency": 5 * [enum_strings.monthly_granularity],
            }
        )
        dao.write_df(FlCountyAggregate, subject)

        subject = pd.DataFrame(
            {
                "facility_name": ["One", "Two", "Three", "Four", "Five"],
                "average_daily_population": [13, 14, 15, 16, 17],
                "number_felony_pretrial": [23, 24, 25, 26, 27],
                "number_misdemeanor_pretrial": 5 * [pd.NaT],
                "fips": ["10000", "10111", "10222", "10333", "10444"],
                "report_date": 5 * [DATE_SCRAPED],
                "aggregation_window": 5 * [enum_strings.monthly_granularity],
                "report_frequency": 5 * [enum_strings.monthly_granularity],
            }
        )

        # Act
        dao.write_df(FlFacilityAggregate, subject)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(FlCountyAggregate).filter(
                FlCountyAggregate.county_name == "Bay"
            )
            result = one(query.all())

            fips_not_overridden_by_facility_table = "00002"
            self.assertEqual(result.county_name, "Bay")
            self.assertEqual(result.fips, fips_not_overridden_by_facility_table)

    def testWriteDf_rowsWithSameColumnsThatMustBeUnique_onlyWritesOnce(self):
        # Arrange
        shared_fips = "12345"
        subject = pd.DataFrame(
            {
                "county_name": ["Alachua", "Baker"],
                "county_population": [257062, 26965],
                "average_daily_population": [799, 478],
                "date_reported": [pd.NaT, pd.NaT],
                "fips": 2 * [shared_fips],
                "report_date": 2 * [DATE_SCRAPED],
                "aggregation_window": 2 * [enum_strings.monthly_granularity],
                "report_frequency": 2 * [enum_strings.monthly_granularity],
            }
        )

        # Act
        dao.write_df(FlCountyAggregate, subject)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(FlCountyAggregate)
            self.assertEqual(len(query.all()), 1)

    def testWriteDf_OverlappingData_WritesNewAndIgnoresDuplicateRows(self):
        # Arrange
        initial_df = pd.DataFrame(
            {
                "county_name": ["Alachua", "Baker", "Bay", "Bradford", "Brevard"],
                "county_population": [257062, 26965, 176016, 27440, 568919],
                "average_daily_population": [799, 478, 1015, 141, 1547],
                "date_reported": [
                    pd.NaT,
                    pd.NaT,
                    datetime.datetime(year=2017, month=9, day=1),
                    pd.NaT,
                    pd.NaT,
                ],
                "fips": ["00000", "00001", "00002", "00003", "00004"],
                "report_date": 5 * [DATE_SCRAPED],
                "aggregation_window": 5 * [enum_strings.monthly_granularity],
                "report_frequency": 5 * [enum_strings.monthly_granularity],
            }
        )
        dao.write_df(FlCountyAggregate, initial_df)

        subject = pd.DataFrame(
            {
                "county_name": ["Alachua", "NewCounty", "Baker"],
                "county_population": [0, 1000000000, 0],
                "average_daily_population": [0, 50, 0],
                "date_reported": [pd.NaT, pd.NaT, pd.NaT],
                "fips": ["00000", "01000", "00002"],
                "report_date": 3 * [DATE_SCRAPED],
                "aggregation_window": 3 * [enum_strings.monthly_granularity],
                "report_frequency": 3 * [enum_strings.monthly_granularity],
            }
        )

        # Act
        dao.write_df(FlCountyAggregate, subject)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(func.sum(FlCountyAggregate.county_population))
            result = one(one(query.all()))

            # This sum includes intial_df + NewCounty and ignores other changes in
            # the subject (eg. county_population = 0 for 'Alachua')
            expected_sum_county_populations = 1001056402
            self.assertEqual(result, expected_sum_county_populations)
