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
"""View for the incarceration admissions in the last 60 days"""
from recidiviz.calculator.bq import bqview, export_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

INCARCERATION_ADMISSIONS_60_DAYS_VIEW_NAME = 'incarceration_admissions_60_days'

INCARCERATION_ADMISSIONS_60_DAYS_DESCRIPTION = \
    """ Incarceration admissions in last 60 days.
    This includes new admissions and admissions for supervision revocation.
    Excludes all other reasons for admission.
    """

INCARCERATION_ADMISSIONS_60_DAYS_QUERY = \
    """
/*{description}*/

SELECT * FROM `{project_id}.{base_dataset}.state_incarceration_period`
WHERE admission_reason in
    ('NEW_ADMISSION', 'PAROLE_REVOCATION', 'PROBATION_REVOCATION')
AND admission_date BETWEEN (DATE_ADD(CURRENT_DATE(), INTERVAL -60 DAY))
    AND CURRENT_DATE()
""".format(
        description=INCARCERATION_ADMISSIONS_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
    )

INCARCERATION_ADMISSIONS_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=INCARCERATION_ADMISSIONS_60_DAYS_VIEW_NAME,
    view_query=INCARCERATION_ADMISSIONS_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(INCARCERATION_ADMISSIONS_60_DAYS_VIEW.view_id)
    print(INCARCERATION_ADMISSIONS_60_DAYS_VIEW.view_query)
