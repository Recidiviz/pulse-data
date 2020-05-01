# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""A view revealing when sequences of state incarceration periods for a given person exhibit that an "open"
incarceration period, i.e. one which has no release date yet, is followed by another incarceration period with an
admission date."""

# pylint: disable=trailing-whitespace
from recidiviz.calculator.query import bqview, export_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_NAME = 'incarceration_admission_after_open_period'

INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_DESCRIPTION = """ Incarceration admissions after open periods """

INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_QUERY = \
    """
    /*{description}*/
    WITH periods_with_next_admission AS (
      SELECT external_id, person_id, state_code as region_code, admission_date, release_date,
      LEAD(admission_date) OVER (
        PARTITION BY state_code, person_id order by admission_date ASC, COALESCE(release_date, DATE(3000, 01, 01)) ASC
      ) AS next_admission_date,
      FROM `{project_id}.{state_dataset}.state_incarceration_period`
      WHERE admission_date IS NOT NULL AND external_id IS NOT NULL
    )
    SELECT * FROM periods_with_next_admission
    WHERE release_date IS NULL AND next_admission_date IS NOT NULL
""".format(
        description=INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        state_dataset=BASE_DATASET,
    )

INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW = bqview.BigQueryView(
    view_id=INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_NAME,
    view_query=INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_QUERY
)

if __name__ == '__main__':
    print(INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW.view_id)
    print(INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW.view_query)
