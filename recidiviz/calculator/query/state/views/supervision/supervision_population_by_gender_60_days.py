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
"""Active supervision population in the last 60 days by gender."""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.query import export_config, bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

SUPERVISION_POPULATION_BY_GENDER_60_DAYS_VIEW_NAME = \
    'supervision_population_by_gender_60_days'

SUPERVISION_POPULATION_BY_GENDER_60_DAYS_DESCRIPTION = """
 All individuals who have been on active supervision within the last 60 days,
 broken down by gender.
"""

SUPERVISION_POPULATION_BY_GENDER_60_DAYS_QUERY = \
    """
    /*{description}*/

    SELECT state_code, gender, count(*) AS count
    FROM `{project_id}.{views_dataset}.persons_on_supervision_60_days` persons_on_supervision
    JOIN `{project_id}.{base_dataset}.state_person` state_persons
    ON state_persons.person_id = persons_on_supervision.person_id
    GROUP BY state_code, gender
    """.format(
        description=
        SUPERVISION_POPULATION_BY_GENDER_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
        views_dataset=VIEWS_DATASET,
    )

SUPERVISION_POPULATION_BY_GENDER_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_POPULATION_BY_GENDER_60_DAYS_VIEW_NAME,
    view_query=SUPERVISION_POPULATION_BY_GENDER_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_POPULATION_BY_GENDER_60_DAYS_VIEW.view_id)
    print(SUPERVISION_POPULATION_BY_GENDER_60_DAYS_VIEW.view_query)
