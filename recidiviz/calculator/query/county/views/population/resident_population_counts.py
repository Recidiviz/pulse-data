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
"""Resident population counts by race and gender for each year-fips."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.vera import vera_view_constants
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RESIDENT_POPULATION_COUNTS_VIEW_NAME = "resident_population_counts"

RESIDENT_POPULATION_COUNTS_DESCRIPTION = """
Creates a resident population count table from
Vera's Incarceration Trends dataset. For every
year-fips-race-gender combination, there will be a `resident_pop` column.
Additionally, a `total_resident_pop` column is created that sums the entire
resident population for each year-fips combination.

Combines 'OTHER' and 'EXTERNAL_UNKNOWN' Race into 'OTHER/UNKNOWN'.

Create fake 'ALL' enums for Race and Gender fields.

Gender: 'ALL' sums Male, Female, and Unknown for each race.

Similarly, Race: 'ALL' sums every race for each gender.

DO NOT sum along race or gender, or you will double-count by including 'ALL'.
"""

RESIDENT_POPULATION_COUNTS_QUERY_TEMPLATE = """
/*{description}*/

WITH

RaceGender AS (
  SELECT
    year,
    fips,
    IF(race = 'OTHER' OR race = 'EXTERNAL_UNKNOWN', 'OTHER/UNKNOWN', race) AS race,
    gender,
    resident_pop
  FROM `{project_id}.{vera_dataset}.{iob_race_gender_pop_table}`
),

AllRace AS (
  SELECT
    year,
    fips,
    'ALL' AS race,
    gender,
    SUM(resident_pop) AS resident_pop
  FROM RaceGender
  GROUP BY year, fips, gender
),

AllGender AS (
  SELECT
    year,
    fips,
    race,
    'ALL' AS gender,
    SUM(resident_pop) AS resident_pop
    FROM RaceGender
  GROUP BY year, fips, race
),

TotalPop AS (
  SELECT
    year,
    fips,
    SUM(resident_pop) AS total_resident_pop
    FROM RaceGender
  GROUP BY year, fips
)

SELECT CombinedRaceGender.*, TotalPop.total_resident_pop FROM (
  SELECT * FROM AllRace
  UNION ALL
  SELECT * FROM AllGender
  UNION ALL
  SELECT * FROM RaceGender
) CombinedRaceGender
LEFT JOIN
  TotalPop
ON
  CombinedRaceGender.year = TotalPop.year AND CombinedRaceGender.fips = TotalPop.fips

ORDER BY year DESC, fips, race, gender
"""

RESIDENT_POPULATION_COUNTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=RESIDENT_POPULATION_COUNTS_VIEW_NAME,
    view_query_template=RESIDENT_POPULATION_COUNTS_QUERY_TEMPLATE,
    description=RESIDENT_POPULATION_COUNTS_DESCRIPTION,
    vera_dataset=vera_view_constants.VERA_DATASET,
    iob_race_gender_pop_table=vera_view_constants.IOB_RACE_GENDER_POP_TABLE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RESIDENT_POPULATION_COUNTS_VIEW_BUILDER.build_and_print()
