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
"""Tests for refreshing the county resident population data"""

import os
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from recidiviz.tools.datasets import refresh_county_resident_populations


def get_fixture_path(name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "fixtures", name)


class RefreshCountyResidentPopulationTest(unittest.TestCase):
    """Tests for refreshing the county resident population data"""

    def test_generates_df(self) -> None:
        # Act
        df = refresh_county_resident_populations.fetch_population_df(
            get_fixture_path("2019_census_populations.xlsx")
        )
        df = refresh_county_resident_populations.transform_population_df(df)

        # Assert
        assert_frame_equal(
            df,
            pd.DataFrame(
                [
                    ["01005", 2010, 27_327],
                    ["01007", 2010, 22_870],
                    ["01005", 2011, 27_341],
                    ["01007", 2011, 22_745],
                    ["01005", 2012, 27_169],
                    ["01007", 2012, 22_667],
                    ["01005", 2013, 26_937],
                    ["01007", 2013, 22_521],
                    ["01005", 2014, 26_755],
                    ["01007", 2014, 22_553],
                ],
                columns=["fips", "year", "population"],
            ),
        )

    def test_generates_adult_df(self) -> None:
        # Act
        df = refresh_county_resident_populations.transform_adult_population_df(
            get_fixture_path("2019_adult_population.txt")
        )

        # Assert
        assert_frame_equal(
            df,
            pd.DataFrame(
                [
                    ["01001", 2010, 36_343],
                    ["01003", 2010, 117_547],
                    ["01005", 2010, 18_514],
                    ["56041", 2019, 12_462],
                    ["56043", 2019, 4_677],
                    ["56045", 2019, 4_291],
                ],
                columns=["fips", "year", "population"],
            ),
        )
