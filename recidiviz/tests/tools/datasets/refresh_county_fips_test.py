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
"""Tests for refreshing the county fips file"""

import os
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from recidiviz.tools.datasets import refresh_county_fips


def get_fixture_path(name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "fixtures", name)


class RefreshCountyFipsTest(unittest.TestCase):
    """Tests for refreshing the county fips file"""

    def test_generates_2014_df(self) -> None:
        # Act
        df = refresh_county_fips.generate_2014_fips_df(
            get_fixture_path("2014_fips_source.txt")
        )

        # Assert
        assert_frame_equal(
            df,
            pd.DataFrame(
                [
                    ["AL", "01", "005", "Barbour County", "01005"],
                    ["AL", "01", "007", "Bibb County", "01007"],
                    ["PR", "72", "153", "Yauco Municipio", "72153"],
                    ["UM", "74", "300", "Midway Islands", "74300"],
                    ["VI", "78", "010", "St. Croix Island", "78010"],
                    ["VI", "78", "020", "St. John Island", "78020"],
                    ["VI", "78", "030", "St. Thomas Island", "78030"],
                ],
                columns=[
                    "state_abbrev",
                    "state_code",
                    "county_code",
                    "county_name",
                    "fips",
                ],
            ),
        )

    def test_generates_2018_df(self) -> None:
        # Act
        df = refresh_county_fips.generate_2018_fips_df(
            get_fixture_path("2018_fips_source.xlsx")
        )

        # Assert
        assert_frame_equal(
            df,
            pd.DataFrame(
                [
                    ["AL", "01", "005", "Barbour County", "01005"],
                    ["AL", "01", "007", "Bibb County", "01007"],
                    ["PR", "72", "151", "Yabucoa Municipio", "72151"],
                    ["PR", "72", "153", "Yauco Municipio", "72153"],
                ],
                columns=[
                    "state_abbrev",
                    "state_code",
                    "county_code",
                    "county_name",
                    "fips",
                ],
            ),
        )

    def test_union(self) -> None:
        # Arrange
        df1 = pd.DataFrame(
            [
                # Overlapping
                ["AL", "01", "005", "Barbour County", "01005"],
                ["AL", "01", "007", "Bibb County", "01007"],
                # Different spelling
                ["NM", "35", "013", "Dona Ana County", "35013"],
                # New
                ["UM", "74", "300", "Midway Islands", "74300"],
                ["VI", "78", "010", "St. Croix Island", "78010"],
            ],
            columns=[
                "state_abbrev",
                "state_code",
                "county_code",
                "county_name",
                "fips",
            ],
        )
        df2 = pd.DataFrame(
            [
                # Overlapping
                ["AL", "01", "005", "Barbour County", "01005"],
                ["AL", "01", "007", "Bibb County", "01007"],
                # New
                ["SD", "46", "102", "Oglala Lakota County", "46102"],
                # Different spelling
                ["NM", "35", "013", "Doña Ana County", "35013"],
            ],
            columns=[
                "state_abbrev",
                "state_code",
                "county_code",
                "county_name",
                "fips",
            ],
        )

        # Act
        union_df = refresh_county_fips.union_fips_dfs([df1, df2])

        # Assert
        assert_frame_equal(
            union_df,
            pd.DataFrame(
                [
                    ["AL", "01", "005", "Barbour County", "01005"],
                    ["AL", "01", "007", "Bibb County", "01007"],
                    ["NM", "35", "013", "Doña Ana County", "35013"],
                    ["SD", "46", "102", "Oglala Lakota County", "46102"],
                    ["UM", "74", "300", "Midway Islands", "74300"],
                    ["VI", "78", "010", "St. Croix Island", "78010"],
                ],
                columns=[
                    "state_abbrev",
                    "state_code",
                    "county_code",
                    "county_name",
                    "fips",
                ],
            ),
        )

    def test_unknowns(self) -> None:
        # Arrange
        df = pd.DataFrame(
            [
                ["AL", "01", "005", "Barbour County", "01005"],
                ["AL", "01", "007", "Bibb County", "01007"],
                ["NM", "35", "013", "Doña Ana County", "35013"],
            ],
            columns=[
                "state_abbrev",
                "state_code",
                "county_code",
                "county_name",
                "fips",
            ],
        )

        # Act
        unknowns_df = refresh_county_fips.add_unknowns(df)

        # Assert
        assert_frame_equal(
            unknowns_df,
            pd.DataFrame(
                [
                    ["AL", "01", "005", "Barbour County", "01005"],
                    ["AL", "01", "007", "Bibb County", "01007"],
                    ["NM", "35", "013", "Doña Ana County", "35013"],
                    ["AL", "01", "999", "AL Unknown", "01999"],
                    ["NM", "35", "999", "NM Unknown", "35999"],
                ],
                columns=[
                    "state_abbrev",
                    "state_code",
                    "county_code",
                    "county_name",
                    "fips",
                ],
            ),
        )
