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
"""Active supervision population in the last 60 days by age."""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

SUPERVISION_POPULATION_BY_AGE_60_DAYS_VIEW_NAME = \
    'supervision_population_by_age_60_days'

SUPERVISION_POPULATION_BY_AGE_60_DAYS_DESCRIPTION = """
 All individuals who have been on active supervision within the last 60 days,
 broken down by age.
"""

SUPERVISION_POPULATION_BY_AGE_60_DAYS_QUERY = \
    """
    /*{description}*/

    SELECT state_code,
    CASE
      WHEN age IS NULL THEN ''
      WHEN age > 40 THEN '40+'
      WHEN age >= 35 THEN '35-39'
      WHEN age >= 30 THEN '30-34'
      WHEN age >= 25 THEN '25-29'
      ELSE '0-24'
    END AS age_bucket,
    count(*) AS count
    FROM `{project_id}.{views_dataset}.persons_on_supervision_60_days` persons_on_supervision
    JOIN `{project_id}.{views_dataset}.persons_with_age` persons_with_age 
    ON persons_with_age.person_id = persons_on_supervision.person_id
    GROUP BY state_code, age_bucket
    """.format(
        description=
        SUPERVISION_POPULATION_BY_AGE_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

SUPERVISION_POPULATION_BY_AGE_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_POPULATION_BY_AGE_60_DAYS_VIEW_NAME,
    view_query=SUPERVISION_POPULATION_BY_AGE_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_POPULATION_BY_AGE_60_DAYS_VIEW.view_id)
    print(SUPERVISION_POPULATION_BY_AGE_60_DAYS_VIEW.view_query)
