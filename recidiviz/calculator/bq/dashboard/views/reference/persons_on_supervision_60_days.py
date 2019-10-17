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
"""All individuals who have been on active supervision within the last
60 days.
"""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.bq import bqview, export_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

PERSONS_ON_SUPERVISION_60_DAYS_VIEW_NAME = 'persons_on_supervision_60_days'

PERSONS_ON_SUPERVISION_60_DAYS_DESCRIPTION = \
    """All individuals who have been on active supervision within the last 60 days."""

PERSONS_ON_SUPERVISION_60_DAYS_QUERY = \
    """
/*{description}*/
SELECT person_id, state_code
    FROM `{project_id}.{base_dataset}.state_supervision_period`
    WHERE termination_date IS NULL
    OR termination_date BETWEEN (DATE_ADD(CURRENT_DATE(), INTERVAL -60 DAY)) AND CURRENT_DATE()
    AND start_date <= CURRENT_DATE()
    GROUP BY person_id, state_code
""".format(
        description=PERSONS_ON_SUPERVISION_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

PERSONS_ON_SUPERVISION_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=PERSONS_ON_SUPERVISION_60_DAYS_VIEW_NAME,
    view_query=PERSONS_ON_SUPERVISION_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(PERSONS_ON_SUPERVISION_60_DAYS_VIEW.view_id)
    print(PERSONS_ON_SUPERVISION_60_DAYS_VIEW.view_query)
