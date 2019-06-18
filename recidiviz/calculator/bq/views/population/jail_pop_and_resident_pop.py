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

from recidiviz.calculator.bq.views.population.population_admissions_releases_race_gender_all import \
    POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW
from recidiviz.calculator.bq.views.population.resident_population_counts import \
    RESIDENT_POPULATION_COUNTS_VIEW

from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.VIEWS_DATASET

# Exclude all data <= CUTOFF_YEAR.
CUTOFF_YEAR = 1999

JAIL_POP_AND_RESIDENT_POP_VIEW_NAME = 'jail_pop_and_resident_pop'

JAIL_POP_AND_RESIDENT_POP_DESCRIPTION = \
"""
Combines jail population and resident population counts into one table.
Joined based on fips-year-race-gender combinations.

All years <= {cutoff_year} are excluded.
""".format(
    cutoff_year=CUTOFF_YEAR
)

JAIL_POP_AND_RESIDENT_POP_QUERY = \
"""
/*{description}*/

SELECT PopulationRaceGender.day,
  PopulationRaceGender.fips,
  PopulationRaceGender.county_name,
  PopulationRaceGender.state,
  PopulationRaceGender.race,
  PopulationRaceGender.gender,
  PopulationRaceGender.person_count,
  PopulationRaceGender.admitted,
  PopulationRaceGender.released,
  PopulationRaceGender.total_jail_person_count,
  ResidentPopulation.resident_pop,
  ResidentPopulation.total_resident_pop
FROM
  `{project_id}.{views_dataset}.{population_admissions_releases_race_gender_all_view}` PopulationRaceGender
LEFT JOIN
  `{project_id}.{views_dataset}.{resident_population_counts_view}` ResidentPopulation
ON
  PopulationRaceGender.fips = ResidentPopulation.fips
    AND EXTRACT(YEAR FROM PopulationRaceGender.day) = ResidentPopulation.year
    AND PopulationRaceGender.race = ResidentPopulation.race
    AND PopulationRaceGender.gender = ResidentPopulation.gender
WHERE EXTRACT(YEAR FROM day) > {cutoff_year}
""".format(
    description=JAIL_POP_AND_RESIDENT_POP_DESCRIPTION,
    project_id=PROJECT_ID,
    views_dataset=VIEWS_DATASET,
    population_admissions_releases_race_gender_all_view=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW.view_id,
    resident_population_counts_view=RESIDENT_POPULATION_COUNTS_VIEW.view_id,
    cutoff_year=CUTOFF_YEAR
)

JAIL_POP_AND_RESIDENT_POP_VIEW = bqview.BigQueryView(
    view_id=JAIL_POP_AND_RESIDENT_POP_VIEW_NAME,
    view_query=JAIL_POP_AND_RESIDENT_POP_QUERY
)

if __name__ == '__main__':
    print(JAIL_POP_AND_RESIDENT_POP_VIEW.view_id)
    print(JAIL_POP_AND_RESIDENT_POP_VIEW.view_query)
