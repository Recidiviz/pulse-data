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
"""Revocation rate by county of residence in the last 60 days."""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

REVOCATION_RATE_BY_COUNTY_60_DAYS_VIEW_NAME = 'revocation_rate_by_county_60_days'

REVOCATION_RATE_BY_COUNTY_60_DAYS_DESCRIPTION = """
 Revocation rates in the last 60 days by county of residence.
 """

REVOCATION_RATE_BY_COUNTY_60_DAYS_QUERY = \
    """
    /*{description}*/
        
    SELECT IFNULL(rev.state_code, pop.state_code) as state_code, IFNULL(rev.county_code, pop.county_code) as county_code,
    IFNULL(revocation_count, 0) as revocation_count, pop.population_count, IEEE_DIVIDE(IFNULL(revocation_count, 0), population_count) as revocation_rate
    FROM
    (SELECT state_code, county_code, count(*) as revocation_count FROM
    `{project_id}.{views_dataset}.persons_on_supervision_60_days_with_county_and_revocation` 
    WHERE revoked = TRUE
    GROUP BY state_code, county_code) rev
    FULL OUTER JOIN
    (SELECT state_code, county_code, count(*) as population_count FROM
    `{project_id}.{views_dataset}.persons_on_supervision_60_days_with_county_and_revocation` 
    GROUP BY state_code, county_code) pop
    ON rev.state_code = pop.state_code AND rev.county_code = pop.county_code 
    ORDER BY county_code
    """.format(
        description=REVOCATION_RATE_BY_COUNTY_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

REVOCATION_RATE_BY_COUNTY_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=REVOCATION_RATE_BY_COUNTY_60_DAYS_VIEW_NAME,
    view_query=REVOCATION_RATE_BY_COUNTY_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(REVOCATION_RATE_BY_COUNTY_60_DAYS_VIEW.view_id)
    print(REVOCATION_RATE_BY_COUNTY_60_DAYS_VIEW.view_query)
