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
"""Script to create query logic for pivoting tables"""

from recidiviz.utils.string import StrictStringFormatter

UNPIVOT_TEMPLATE = """
{column_name} AS (
  SELECT
    fips,
    day,
    data_source,
    {column_name} AS count,
    '{gender}' AS gender,
    '{race}' AS race
  FROM
    `recidiviz-123.census_views.combined_stitch_drop_overlapping`
),
"""

PIVOT_TEMPLATE = "SUM(IF(gender = '{gender}' AND race = '{race}', person_count, null)) AS {column_name},"
PIVOT_TEMPLATE_JUST_GENDER = (
    "SUM(IF(gender = '{gender}', person_count, null)) AS {column_name},"
)
PIVOT_TEMPLATE_JUST_RACE = (
    "SUM(IF(race = '{race}', person_count, null)) AS {column_name},"
)

GENDERS = ["MALE", "FEMALE", "UNKNOWN_GENDER"]
RACES = [
    "ASIAN",
    "BLACK",
    "NATIVE_AMERICAN",
    "LATINO",
    "WHITE",
    "OTHER",
    "UNKNOWN_RACE",
]


def main() -> None:
    """Script to create query logic for pivoting tables"""
    for gender in GENDERS:
        print(
            StrictStringFormatter().format(
                PIVOT_TEMPLATE_JUST_GENDER,
                gender=_to_db_gender(gender),
                column_name=gender.lower(),
            )
        )
    for race in RACES:
        print(
            StrictStringFormatter().format(
                PIVOT_TEMPLATE_JUST_RACE,
                race=_to_db_race(race),
                column_name=race.lower(),
            )
        )

    for gender in GENDERS:
        for race in RACES:
            print_pivot_query(gender, race)

    print("\n---------------\n")

    for gender in GENDERS:
        for race in RACES:
            print_unpivot_query(gender, race)

    for gender in GENDERS:
        for race in RACES:
            print("SELECT * FROM " + _to_column_name(gender, race))
            print("UNION ALL")


def print_pivot_query(gender: str, race: str) -> None:
    print(
        StrictStringFormatter().format(
            PIVOT_TEMPLATE,
            gender=_to_db_gender(gender),
            race=_to_db_race(race),
            column_name=_to_column_name(gender, race),
        )
    )


def print_unpivot_query(gender: str, race: str) -> None:
    column_name = gender.lower() + "_" + race.lower()
    print(
        StrictStringFormatter().format(
            UNPIVOT_TEMPLATE, column_name=column_name, gender=gender, race=race
        )
    )


def _to_db_race(race: str) -> str:
    if race == "NATIVE_AMERICAN":
        return "AMERICAN_INDIAN_ALASKAN_NATIVE"
    if race == "LATINO":
        return "HISPANIC"
    if race == "UNKNOWN_RACE":
        return "EXTERNAL_UNKNOWN"
    return race


def _to_db_gender(gender: str) -> str:
    if gender == "UNKNOWN_GENDER":
        return "EXTERNAL_UNKNOWN"
    return gender


def _to_column_name(gender: str, race: str) -> str:
    return gender.lower() + "_" + race.lower()


if __name__ == "__main__":
    main()
