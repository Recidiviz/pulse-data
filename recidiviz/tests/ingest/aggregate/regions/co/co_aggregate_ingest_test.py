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
"""Tests for co_aggregate_ingest.py."""
from typing import Dict
from unittest import TestCase

import numpy as np
import pandas as pd
from more_itertools import one
from pandas.testing import assert_frame_equal
from sqlalchemy import func
from sqlalchemy.orm import DeclarativeMeta

from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate.regions.co import co_aggregate_ingest
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import CoFacilityAggregate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes


class TestCoAggregateIngest(TestCase):
    """Test that co_aggregate_ingest correctly parses the CO PDF."""

    parsed_csv: Dict[DeclarativeMeta, pd.DataFrame] = {}

    @classmethod
    def setUpClass(cls) -> None:
        # Cache the parsed csv between tests since it's expensive to compute
        cls.parsed_csv = co_aggregate_ingest.parse(
            fixtures.as_filepath("HB19-1297Data.csv")
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testParse_ParsesHeadAndTail(self) -> None:
        result = self.parsed_csv[CoFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame(
            {
                "report_date": [
                    pd.Timestamp(year=2020, month=1, day=1),
                    pd.Timestamp(year=2020, month=1, day=1),
                ],
                "county": ["Adams", "Alamosa"],
                "fips": ["08001", "08003"],
                "qtryear": [2020, 2020],
                "qtr": [1, 1],
                "jms": ["Central Square", "eForce"],
                "capacity": [1271, 163],
                "beds": [1678, 72],
                "deaths": [0, 0],
                "bookings": [4887, 380],
                "releases": [4961, 387],
                "male_number_of_inmates": [787.0, 58.0],
                "female_number_of_inmates": [169.0, 14.0],
                "other_gender_number_of_inmates": [0.0, 0.0],
                "black_number_of_inmates": [124.0, 0.0],
                "native_american_number_of_inmates": [2.0, 0.0],
                "other_race_number_of_inmates": [9.0, 0.0],
                "white_number_of_inmates": [821.0, 72.0],
                "unknown_race_number_of_inmates": [0.0, 0.0],
                "non_hispanic_number_of_inmates": [644.0, 16.0],
                "hispanic_number_of_inmates": [312.0, 55.0],
                "male_sentenced": [267.0, 19.0],
                "female_sentenced": [54.0, 3.0],
                "other_gender_sentenced": [0.0, 0.0],
                "black_sentenced": [37.0, 0.0],
                "native_american_sentenced": [2.0, 0.0],
                "other_race_sentenced": [0.0, 0.0],
                "white_sentenced": [282.0, 22.0],
                "unknown_race_sentenced": [0.0, 0.0],
                "non_hispanic_sentenced": [191.0, 3.0],
                "hispanic_sentenced": [130.0, 18.0],
                "male_unsentenced_hold": [29.0, 1.0],
                "female_unsentenced_hold": [6.0, 0.0],
                "other_gender_unsentenced_hold": [0.0, 0.0],
                "black_unsentenced_hold": [3.0, 0.0],
                "native_american_unsentenced_hold": [0.0, 0.0],
                "other_race_unsentenced_hold": [1.0, 0.0],
                "white_unsentenced_hold": [31.0, 1.0],
                "unknown_race_unsentenced_hold": [0.0, 0.0],
                "non_hispanic_unsentenced_hold": [22.0, 0.0],
                "hispanic_unsentenced_hold": [13.0, 1.0],
                "male_unsentenced_no_hold": [491.0, 38.0],
                "female_unsentenced_no_hold": [109.0, 11.0],
                "other_gender_unsentenced_no_hold": [0.0, 0.0],
                "black_unsentenced_no_hold": [84.0, 0.0],
                "native_american_unsentenced_no_hold": [0.0, 0.0],
                "other_race_unsentenced_no_hold": [8.0, 0.0],
                "white_unsentenced_no_hold": [508.0, 49.0],
                "unknown_race_unsentenced_no_hold": [0.0, 0.0],
                "non_hispanic_unsentenced_no_hold": [431.0, 13.0],
                "hispanic_unsentenced_no_hold": [169.0, 36.0],
                "male_unsentenced_no_hold_felonies": [312.0, 28.0],
                "female_unsentenced_no_hold_felonies": [66.0, 2.0],
                "other_gender_unsentenced_no_hold_felonies": [0.0, 0.0],
                "black_unsentenced_no_hold_felonies": [52.0, 0.0],
                "native_american_unsentenced_no_hold_felonies": [0.0, 0.0],
                "other_race_unsentenced_no_hold_felonies": [5.0, 0.0],
                "white_unsentenced_no_hold_felonies": [321.0, 30.0],
                "unknown_race_unsentenced_no_hold_felonies": [0.0, 0.0],
                "non_hispanic_unsentenced_no_hold_felonies": [249.0, 8.0],
                "hispanic_unsentenced_no_hold_felonies": [129.0, 22.0],
                "male_unsentenced_no_hold_misdemeanors": [179.0, 10.0],
                "female_unsentenced_no_hold_misdemeanors": [43.0, 9.0],
                "other_gender_unsentenced_no_hold_misdemeanors": [0.0, 0.0],
                "black_unsentenced_no_hold_misdemeanors": [32.0, 0.0],
                "native_american_unsentenced_no_hold_misdemeanors": [0.0, 0.0],
                "other_race_unsentenced_no_hold_misdemeanors": [3.0, 0.0],
                "white_unsentenced_no_hold_misdemeanors": [187.0, 19.0],
                "unknown_race_unsentenced_no_hold_misdemeanors": [0.0, 0.0],
                "non_hispanic_unsentenced_no_hold_misdemeanors": [182.0, 5.0],
                "hispanic_unsentenced_no_hold_misdemeanors": [40.0, 14.0],
                "male_municipal_charge": [16.0, np.nan],
                "female_municipal_charge": [4.0, np.nan],
                "other_gender_municipal_charge": [0.0, np.nan],
                "black_municipal_charge": [5.0, np.nan],
                "native_american_municipal_charge": [0.0, np.nan],
                "other_race_municipal_charge": [0.0, np.nan],
                "white_municipal_charge": [15.0, np.nan],
                "unknown_race_municipal_charge": [0.0, np.nan],
                "non_hispanic_municipal_charge": [10.0, np.nan],
                "hispanic_municipal_charge": [10.0, np.nan],
                "male_administrative_segregation": [32.0, np.nan],
                "female_administrative_segregation": [12.0, np.nan],
                "other_gender_administrative_segregation": [0.0, np.nan],
                "black_administrative_segregation": [7.0, np.nan],
                "native_american_administrative_segregation": [0.0, np.nan],
                "other_race_administrative_segregation": [0.0, np.nan],
                "white_administrative_segregation": [37.0, np.nan],
                "unknown_race_administrative_segregation": [0.0, np.nan],
                "non_hispanic_administrative_segregation": [29.0, np.nan],
                "hispanic_administrative_segregation": [15.0, np.nan],
                "male_competency_evaluation": [10.0, np.nan],
                "female_competency_evaluation": [0.0, np.nan],
                "other_gender_competency_evaluation": [0.0, np.nan],
                "black_competency_evaluation": [1.0, np.nan],
                "native_american_competency_evaluation": [0.0, np.nan],
                "other_race_competency_evaluation": [0.0, np.nan],
                "white_competency_evaluation": [9.0, np.nan],
                "unknown_race_competency_evaluation": [0.0, np.nan],
                "non_hispanic_competency_evaluation": [8.0, np.nan],
                "hispanic_competency_evaluation": [2.0, np.nan],
                "male_average_daily_population": [0.0, 68.0],
                "female_average_daily_population": [0.0, 18.0],
                "other_gender_average_daily_population": [0.0, 0.0],
                "black_average_daily_population": [0.0, 0.0],
                "native_american_average_daily_population": [0.0, 0.0],
                "other_race_average_daily_population": [0.0, 0.0],
                "white_average_daily_population": [0.0, 0.0],
                "unknown_race_average_daily_population": [0.0, 86.0],
                "non_hispanic_average_daily_population": [0.0, 0.0],
                "hispanic_average_daily_population": [0.0, 0.0],
                "male_average_los_felonies": [34.0, np.nan],
                "female_average_los_felonies": [28.0, np.nan],
                "other_gender_average_los_felonies": [0.0, np.nan],
                "black_average_los_felonies": [33.0, np.nan],
                "native_american_average_los_felonies": [29.0, np.nan],
                "other_race_average_los_felonies": [31.0, np.nan],
                "white_average_los_felonies": [31.0, np.nan],
                "unknown_race_average_los_felonies": [0.0, np.nan],
                "non_hispanic_average_los_felonies": [18.0, np.nan],
                "hispanic_average_los_felonies": [18.0, np.nan],
                "male_felony_releases": [5981.0, np.nan],
                "female_felony_releases": [1886.0, np.nan],
                "other_gender_felony_releases": [0.0, np.nan],
                "black_felony_releases": [795.0, np.nan],
                "native_american_felony_releases": [20.0, np.nan],
                "other_race_felony_releases": [78.0, np.nan],
                "white_felony_releases": [6974.0, np.nan],
                "unknown_race_felony_releases": [0.0, np.nan],
                "non_hispanic_felony_releases": [5480.0, np.nan],
                "hispanic_felony_releases": [2387.0, np.nan],
                "male_average_los_misdemeanors": [18.0, np.nan],
                "female_average_los_misdemeanors": [8.0, np.nan],
                "other_gender_average_los_misdemeanors": [0.0, np.nan],
                "black_average_los_misdemeanors": [41.0, np.nan],
                "native_american_average_los_misdemeanors": [16.0, np.nan],
                "other_race_average_los_misdemeanors": [12.0, np.nan],
                "white_average_los_misdemeanors": [14.0, np.nan],
                "unknown_race_average_los_misdemeanors": [0.0, np.nan],
                "non_hispanic_average_los_misdemeanors": [8.0, np.nan],
                "hispanic_average_los_misdemeanors": [8.0, np.nan],
                "male_misdemeanor_releases": [8669.0, np.nan],
                "female_misdemeanor_releases": [3308.0, np.nan],
                "other_gender_misdemeanor_releases": [0.0, np.nan],
                "black_misdemeanor_releases": [1139.0, np.nan],
                "native_american_misdemeanor_releases": [27.0, np.nan],
                "other_race_misdemeanor_releases": [18.0, np.nan],
                "white_misdemeanor_releases": [10793.0, np.nan],
                "unknown_race_misdemeanor_releases": [0.0, np.nan],
                "non_hispanic_misdemeanor_releases": [7650.0, np.nan],
                "hispanic_misdemeanor_releases": [4327.0, np.nan],
                "male_homeless": [39.0, np.nan],
                "female_homeless": [14.0, np.nan],
                "other_gender_homeless": [0.0, np.nan],
                "black_homeless": [11.0, np.nan],
                "native_american_homeless": [1.0, np.nan],
                "other_race_homeless": [1.0, np.nan],
                "white_homeless": [40.0, np.nan],
                "unknown_race_homeless": [0.0, np.nan],
                "non_hispanic_homeless": [26.0, np.nan],
                "hispanic_homeless": [27.0, np.nan],
                "na_message": [
                    "**Average Daily Population**: not broken out by gender, "
                    "race or ethnicity., ",
                    "**Administrative Segregation**: Not able to capture "
                    "information, **Competency Evaluation**: Not able to "
                    "capture information, **Average LOS Felonies**: Not able "
                    "to caputre information, **Felony Releases**: Not able to "
                    "caputre information, **Average LOS Misdemeanors**: Not "
                    "able to caputre information, **Misdemeanor Releases**: "
                    "Not able to caputre information, **Homeless**: Not able "
                    "to caputre information, ",
                ],
                "aggregation_window": [
                    enum_strings.daily_granularity,
                    enum_strings.daily_granularity,
                ],
                "report_frequency": [
                    enum_strings.quarterly_granularity,
                    enum_strings.quarterly_granularity,
                ],
            }
        )
        assert_frame_equal(result.head(n=2), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame(
            {
                "report_date": [
                    pd.Timestamp(year=2021, month=4, day=1),
                    pd.Timestamp(year=2021, month=4, day=1),
                ],
                "county": ["Weld", "Yuma"],
                "fips": ["08123", "08125"],
                "qtryear": [2021, 2021],
                "qtr": [2, 2],
                "jms": ["Spillman", "eForce"],
                "capacity": [763, 42],
                "beds": [954, 42],
                "deaths": [0, 0],
                "bookings": [1466, 44],
                "releases": [1476, 48],
                "male_number_of_inmates": [518.0, 20.0],
                "female_number_of_inmates": [90.0, 1.0],
                "other_gender_number_of_inmates": [0.0, 0.0],
                "black_number_of_inmates": [29.0, 1.0],
                "native_american_number_of_inmates": [4.0, 0.0],
                "other_race_number_of_inmates": [5.0, 0.0],
                "white_number_of_inmates": [570.0, 20.0],
                "unknown_race_number_of_inmates": [0.0, 0.0],
                "non_hispanic_number_of_inmates": [326.0, 13.0],
                "hispanic_number_of_inmates": [244.0, 6.0],
                "male_sentenced": [91.0, 4.0],
                "female_sentenced": [20.0, 0.0],
                "other_gender_sentenced": [0.0, 0.0],
                "black_sentenced": [5.0, 1.0],
                "native_american_sentenced": [0.0, 0.0],
                "other_race_sentenced": [2.0, 0.0],
                "white_sentenced": [104.0, 3.0],
                "unknown_race_sentenced": [0.0, 0.0],
                "non_hispanic_sentenced": [53.0, 3.0],
                "hispanic_sentenced": [48.0, 1.0],
                "male_unsentenced_hold": [75.0, np.nan],
                "female_unsentenced_hold": [14.0, np.nan],
                "other_gender_unsentenced_hold": [0.0, np.nan],
                "black_unsentenced_hold": [3.0, np.nan],
                "native_american_unsentenced_hold": [1.0, np.nan],
                "other_race_unsentenced_hold": [0.0, np.nan],
                "white_unsentenced_hold": [85.0, np.nan],
                "unknown_race_unsentenced_hold": [0.0, np.nan],
                "non_hispanic_unsentenced_hold": [54.0, np.nan],
                "hispanic_unsentenced_hold": [30.0, np.nan],
                "male_unsentenced_no_hold": [352.0, 14.0],
                "female_unsentenced_no_hold": [56.0, 1.0],
                "other_gender_unsentenced_no_hold": [0.0, 0.0],
                "black_unsentenced_no_hold": [21.0, 0.0],
                "native_american_unsentenced_no_hold": [3.0, 0.0],
                "other_race_unsentenced_no_hold": [3.0, 0.0],
                "white_unsentenced_no_hold": [381.0, 15.0],
                "unknown_race_unsentenced_no_hold": [0.0, 0.0],
                "non_hispanic_unsentenced_no_hold": [219.0, 10.0],
                "hispanic_unsentenced_no_hold": [166.0, 3.0],
                "male_unsentenced_no_hold_felonies": [324.0, 8.0],
                "female_unsentenced_no_hold_felonies": [52.0, 1.0],
                "other_gender_unsentenced_no_hold_felonies": [0.0, 0.0],
                "black_unsentenced_no_hold_felonies": [18.0, 0.0],
                "native_american_unsentenced_no_hold_felonies": [2.0, 0.0],
                "other_race_unsentenced_no_hold_felonies": [2.0, 0.0],
                "white_unsentenced_no_hold_felonies": [354.0, 9.0],
                "unknown_race_unsentenced_no_hold_felonies": [0.0, 0.0],
                "non_hispanic_unsentenced_no_hold_felonies": [195.0, 7.0],
                "hispanic_unsentenced_no_hold_felonies": [158.0, 2.0],
                "male_unsentenced_no_hold_misdemeanors": [28.0, 6.0],
                "female_unsentenced_no_hold_misdemeanors": [4.0, 0.0],
                "other_gender_unsentenced_no_hold_misdemeanors": [0.0, 0.0],
                "black_unsentenced_no_hold_misdemeanors": [3.0, 0.0],
                "native_american_unsentenced_no_hold_misdemeanors": [1.0, 0.0],
                "other_race_unsentenced_no_hold_misdemeanors": [1.0, 0.0],
                "white_unsentenced_no_hold_misdemeanors": [27.0, 6.0],
                "unknown_race_unsentenced_no_hold_misdemeanors": [0.0, 0.0],
                "non_hispanic_unsentenced_no_hold_misdemeanors": [24.0, 3.0],
                "hispanic_unsentenced_no_hold_misdemeanors": [8.0, 1.0],
                "male_municipal_charge": [np.nan, np.nan],
                "female_municipal_charge": [np.nan, np.nan],
                "other_gender_municipal_charge": [np.nan, np.nan],
                "black_municipal_charge": [np.nan, np.nan],
                "native_american_municipal_charge": [np.nan, np.nan],
                "other_race_municipal_charge": [np.nan, np.nan],
                "white_municipal_charge": [np.nan, np.nan],
                "unknown_race_municipal_charge": [np.nan, np.nan],
                "non_hispanic_municipal_charge": [np.nan, np.nan],
                "hispanic_municipal_charge": [np.nan, np.nan],
                "male_administrative_segregation": [25.0, np.nan],
                "female_administrative_segregation": [3.0, np.nan],
                "other_gender_administrative_segregation": [0.0, np.nan],
                "black_administrative_segregation": [3.0, np.nan],
                "native_american_administrative_segregation": [0.0, np.nan],
                "other_race_administrative_segregation": [0.0, np.nan],
                "white_administrative_segregation": [25.0, np.nan],
                "unknown_race_administrative_segregation": [0.0, np.nan],
                "non_hispanic_administrative_segregation": [21.0, np.nan],
                "hispanic_administrative_segregation": [4.0, np.nan],
                "male_competency_evaluation": [np.nan, np.nan],
                "female_competency_evaluation": [np.nan, np.nan],
                "other_gender_competency_evaluation": [np.nan, np.nan],
                "black_competency_evaluation": [np.nan, np.nan],
                "native_american_competency_evaluation": [np.nan, np.nan],
                "other_race_competency_evaluation": [np.nan, np.nan],
                "white_competency_evaluation": [np.nan, np.nan],
                "unknown_race_competency_evaluation": [np.nan, np.nan],
                "non_hispanic_competency_evaluation": [np.nan, np.nan],
                "hispanic_competency_evaluation": [np.nan, np.nan],
                "male_average_daily_population": [84.0, 19.0],
                "female_average_daily_population": [448.0, 2.0],
                "other_gender_average_daily_population": [1.0, 0.0],
                "black_average_daily_population": [30.0, 1.0],
                "native_american_average_daily_population": [3.0, 0.0],
                "other_race_average_daily_population": [3.0, 1.0],
                "white_average_daily_population": [496.0, 19.0],
                "unknown_race_average_daily_population": [1.0, 0.0],
                "non_hispanic_average_daily_population": [288.0, 14.0],
                "hispanic_average_daily_population": [214.0, 5.0],
                "male_average_los_felonies": [61.64, np.nan],
                "female_average_los_felonies": [41.06, np.nan],
                "other_gender_average_los_felonies": [2.05, np.nan],
                "black_average_los_felonies": [55.39, np.nan],
                "native_american_average_los_felonies": [62.87, np.nan],
                "other_race_average_los_felonies": [73.9, np.nan],
                "white_average_los_felonies": [56.69, np.nan],
                "unknown_race_average_los_felonies": [38.87, np.nan],
                "non_hispanic_average_los_felonies": [55.13, np.nan],
                "hispanic_average_los_felonies": [60.21, np.nan],
                "male_felony_releases": [2685.0, 80.0],
                "female_felony_releases": [816.0, 12.0],
                "other_gender_felony_releases": [1.0, 0.0],
                "black_felony_releases": [180.0, 2.0],
                "native_american_felony_releases": [21.0, 0.0],
                "other_race_felony_releases": [37.0, 0.0],
                "white_felony_releases": [3262.0, 88.0],
                "unknown_race_felony_releases": [2.0, 2.0],
                "non_hispanic_felony_releases": [1941.0, 55.0],
                "hispanic_felony_releases": [1346.0, 31.0],
                "male_average_los_misdemeanors": [12.93, np.nan],
                "female_average_los_misdemeanors": [8.06, np.nan],
                "other_gender_average_los_misdemeanors": [5.58, np.nan],
                "black_average_los_misdemeanors": [10.79, np.nan],
                "native_american_average_los_misdemeanors": [13.09, np.nan],
                "other_race_average_los_misdemeanors": [6.23, np.nan],
                "white_average_los_misdemeanors": [11.76, np.nan],
                "unknown_race_average_los_misdemeanors": [15.38, np.nan],
                "non_hispanic_average_los_misdemeanors": [10.83, np.nan],
                "hispanic_average_los_misdemeanors": [13.71, np.nan],
                "male_misdemeanor_releases": [2577.0, 103.0],
                "female_misdemeanor_releases": [879.0, 29.0],
                "other_gender_misdemeanor_releases": [2.0, 0.0],
                "black_misdemeanor_releases": [162.0, 4.0],
                "native_american_misdemeanor_releases": [21.0, 0.0],
                "other_race_misdemeanor_releases": [26.0, 0.0],
                "white_misdemeanor_releases": [3241.0, 123.0],
                "unknown_race_misdemeanor_releases": [8.0, 5.0],
                "non_hispanic_misdemeanor_releases": [1944.0, 87.0],
                "hispanic_misdemeanor_releases": [1281.0, 41.0],
                "male_homeless": [93.0, np.nan],
                "female_homeless": [7.0, np.nan],
                "other_gender_homeless": [0.0, np.nan],
                "black_homeless": [6.0, np.nan],
                "native_american_homeless": [0.0, np.nan],
                "other_race_homeless": [3.0, np.nan],
                "white_homeless": [91.0, np.nan],
                "unknown_race_homeless": [0.0, np.nan],
                "non_hispanic_homeless": [61.0, np.nan],
                "hispanic_homeless": [31.0, np.nan],
                "na_message": [
                    "",
                    "**Average LOS Felonies**: JMS does not report "
                    "these data by Gender/Race/Ethnicity, "
                    "**Average LOS Misdemeanors**: JMS does not "
                    "report these data by Gender/Race/Ethnicity, ",
                ],
                "aggregation_window": [
                    enum_strings.daily_granularity,
                    enum_strings.daily_granularity,
                ],
                "report_frequency": [
                    enum_strings.quarterly_granularity,
                    enum_strings.quarterly_granularity,
                ],
            },
            index=range(292, 294),
        )
        assert_frame_equal(result.tail(n=2), expected_tail, check_names=False)

    def testWrite_CalculatesSum(self) -> None:
        # Act
        for table, df in self.parsed_csv.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_database(self.database_key).query(
            func.sum(CoFacilityAggregate.male_number_of_inmates)
        )
        result = one(one(query.all()))

        expected_sum_male = 45933
        self.assertEqual(result, expected_sum_male)
