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
"""Active supervision population in the last 60 days by race and ethnicity."""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW_NAME = \
    'supervision_population_by_race_and_ethnicity_60_days'

SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_DESCRIPTION = """
 Individuals who have been on active supervision within the last 60 days,
 broken down by the race and ethnicities of the group.
"""

SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_QUERY = \
    """
    /*{description}*/

    SELECT state_code, race_or_ethnicity, count(*) as count FROM
    (SELECT state_code, sup.person_id, race_or_ethnicity FROM
    `{project_id}.{views_dataset}.persons_on_supervision_60_days` sup
    JOIN
    (SELECT person_id, race_or_ethnicity
    FROM `{project_id}.{views_dataset}.state_person_race_and_ethnicity`) race
    ON sup.person_id = race.person_id)
    GROUP BY state_code, race_or_ethnicity
    """.format(
        description=
        SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW_NAME,
    view_query=SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW.view_id)
    print(SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW.view_query)
