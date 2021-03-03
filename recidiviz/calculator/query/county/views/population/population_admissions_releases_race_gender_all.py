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

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config

from recidiviz.calculator.query.county.views.population.population_admissions_releases_race_gender import (
    POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW_NAME = (
    "population_admissions_releases_race_gender_all"
)

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_DESCRIPTION = """
Combines 'OTHER' and 'EXTERNAL_UNKNOWN' Race into 'OTHER/UNKNOWN'.

Create fake 'ALL' enums for Race and Gender fields.

Gender: 'ALL' sums Male, Female, and Unknown for each race.
'EXTERNAL_UNKNOWN' is mapped to 'UNKNOWN'.

Similarly, Race: 'ALL' sums every race for each gender.

DO NOT sum along race or gender, or you will double-count by including 'ALL'.
"""

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_QUERY_TEMPLATE = """
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
"""

POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW_NAME,
    view_query_template=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_QUERY_TEMPLATE,
    description=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_DESCRIPTION,
    views_dataset=dataset_config.VIEWS_DATASET,
    population_admissions_releases_race_gender_view=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW_BUILDER.view_id,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW_BUILDER.build_and_print()
