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
"""View that combines county name, state name, and FIPS from Vera's ITP data."""

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.county.views.vera import vera_view_constants

from recidiviz.utils import metadata


PROJECT_ID = metadata.project_id()

VERA_DATASET = vera_view_constants.VERA_DATASET
INCARCERATION_TRENDS_TABLE = vera_view_constants.INCARCERATION_TRENDS_TABLE

COUNTY_NAMES_VIEW_NAME = 'county_names'

COUNTY_NAMES_VIEW_DESCRIPTION = \
"""
A view that contains all unique combinations of
fips, state name, and county name from
Vera's Incarceration Trends dataset.
"""

COUNTY_NAMES_VIEW_QUERY = \
"""
/*{description}*/
SELECT
  SUBSTR(CAST(yfips AS STRING), 5, 5) AS fips,
  state,
  county_name
FROM `{project_id}.{vera_dataset}.{incarceration_trends_table}`
GROUP BY fips, state, county_name
ORDER BY fips
""".format(
    description=COUNTY_NAMES_VIEW_DESCRIPTION,
    project_id=PROJECT_ID,
    vera_dataset=VERA_DATASET,
    incarceration_trends_table=INCARCERATION_TRENDS_TABLE
)

COUNTY_NAMES_VIEW = BigQueryView(
    view_id=COUNTY_NAMES_VIEW_NAME,
    view_query=COUNTY_NAMES_VIEW_QUERY
)

if __name__ == '__main__':
    print(COUNTY_NAMES_VIEW.view_id)
    print(COUNTY_NAMES_VIEW.view_query)
