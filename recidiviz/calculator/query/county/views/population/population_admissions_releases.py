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
"""Total population, admissions, releases by day-fips."""

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.county import view_config

from recidiviz.calculator.query.county.views.population.population_admissions_releases_race_gender import \
    POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW

from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.VIEWS_DATASET

POPULATION_ADMISSIONS_RELEASES_VIEW_NAME = 'population_admissions_releases'

POPULATION_ADMISSIONS_RELEASES_DESCRIPTION = \
"""
For each day-fips combination,
compute the total population, admissions, and releases.
"""

POPULATION_ADMISSIONS_RELEASES_QUERY = \
"""
/*{description}*/
SELECT
  day,
  fips,
  county_name,
  state,
  SUM(person_count) AS person_count,
  SUM(admitted) AS admitted,
  SUM(released) AS released
FROM
  `{project_id}.{views_dataset}.{population_admissions_releases_race_gender_view}`
GROUP BY day, fips, state, county_name
ORDER BY day DESC, fips
""".format(
    description=POPULATION_ADMISSIONS_RELEASES_DESCRIPTION,
    project_id=PROJECT_ID,
    views_dataset=VIEWS_DATASET,
    population_admissions_releases_race_gender_view=POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW.view_id
)

POPULATION_ADMISSIONS_RELEASES_VIEW = bqview.BigQueryView(
    view_id=POPULATION_ADMISSIONS_RELEASES_VIEW_NAME,
    view_query=POPULATION_ADMISSIONS_RELEASES_QUERY
)

if __name__ == '__main__':
    print(POPULATION_ADMISSIONS_RELEASES_VIEW.view_id)
    print(POPULATION_ADMISSIONS_RELEASES_VIEW.view_query)
