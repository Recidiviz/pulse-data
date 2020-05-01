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

"""A view revealing when incarceration periods have null admission dates."""

# pylint: disable=trailing-whitespace
from recidiviz.calculator.query import bqview, export_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

INCARCERATION_ADMISSION_NULLS_VIEW_NAME = 'incarceration_admission_nulls'

INCARCERATION_ADMISSION_NULLS_DESCRIPTION = """ Incarceration admissions with null admission dates """

INCARCERATION_ADMISSION_NULLS_QUERY = \
    """
    /*{description}*/
    SELECT *, state_code as region_code
    FROM `{project_id}.{state_dataset}.state_incarceration_period`
    WHERE admission_date IS NULL
    AND external_id is NOT NULL
    ORDER BY region_code, external_id, admission_date
""".format(
        description=INCARCERATION_ADMISSION_NULLS_DESCRIPTION,
        project_id=PROJECT_ID,
        state_dataset=BASE_DATASET,
    )

INCARCERATION_ADMISSION_NULLS_VIEW = bqview.BigQueryView(
    view_id=INCARCERATION_ADMISSION_NULLS_VIEW_NAME,
    view_query=INCARCERATION_ADMISSION_NULLS_QUERY
)

if __name__ == '__main__':
    print(INCARCERATION_ADMISSION_NULLS_VIEW.view_id)
    print(INCARCERATION_ADMISSION_NULLS_VIEW.view_query)
