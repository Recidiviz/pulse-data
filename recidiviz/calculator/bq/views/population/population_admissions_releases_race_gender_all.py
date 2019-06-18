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
"""Total population, admissions, releases by day-fips and race-gender."""
# pylint: disable=line-too-long

from recidiviz.calculator.bq.views import bqview
from recidiviz.calculator.bq.views import view_config

from recidiviz.calculator.bq.views.population.population_admissions_releases_race_gender import \
    POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW

from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.VIEWS_DATASET

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW_NAME = 'population_admissions_releases_race_gender_all'

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_DESCRIPTION = \
"""
Combines 'OTHER' and 'EXTERNAL_UNKNOWN' Race into 'OTHER/UNKNOWN'.

Create fake 'ALL' enums for Race and Gender fields.

Gender: 'ALL' sums Male, Female, and Unknown for each race.
'EXTERNAL_UNKNOWN' is mapped to 'UNKNOWN'.

Similarly, Race: 'ALL' sums every race for each gender.

DO NOT sum along race or gender, or you will double-count by including 'ALL'.
"""

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_QUERY = \
"""
/*{description}*/

WITH

RaceGender AS (
  SELECT
    day,
    fips,
    county_name,
    state,
    IF(race = 'OTHER' OR race = 'EXTERNAL_UNKNOWN', 'OTHER/UNKNOWN', race) AS race,
    IF(gender = 'EXTERNAL_UNKNOWN', 'UNKNOWN', gender) AS gender,
    person_count,
    admitted,
    released
  FROM `{project_id}.{views_dataset}.{population_admissions_releases_race_gender_view}`
),

AllRace AS (
  SELECT
    day,
    fips,
    county_name,
    state,
    'ALL' AS race,
    gender,
    SUM(person_count) AS person_count,
    SUM(admitted) AS admitted,
    SUM(released) AS released
  FROM RaceGender
  GROUP BY day, fips, county_name, state, gender
),

AllGender AS (
  SELECT
    day,
    fips,
    county_name,
    state,
    race,
    'ALL' AS gender,
    SUM(person_count) AS person_count,
    SUM(admitted) AS admitted,
    SUM(released) AS released
    FROM RaceGender
  GROUP BY day, fips, county_name, state, race
),

TotalJailPop AS (
  SELECT
    day,
    fips,
    SUM(person_count) AS total_jail_person_count
    FROM RaceGender
  GROUP BY day, fips
)

SELECT AllRaceGender.*, TotalJailPop.total_jail_person_count FROM (
  SELECT * FROM AllRace
  UNION ALL
  SELECT * FROM AllGender
  UNION ALL
  SELECT * FROM RaceGender
) AllRaceGender
LEFT JOIN
  TotalJailPop
ON
  AllRaceGender.fips = TotalJailPop.fips AND AllRaceGender.day = TotalJailPop.day
ORDER BY day DESC, fips, race, gender
""".format(
    description=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_DESCRIPTION,
    project_id=PROJECT_ID,
    views_dataset=VIEWS_DATASET,
    population_admissions_releases_race_gender_view=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW.view_id
)

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW = bqview.BigQueryView(
    view_id=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW_NAME,
    view_query=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_QUERY
)

if __name__ == '__main__':
    print(POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW.view_id)
    print(POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW.view_query)
