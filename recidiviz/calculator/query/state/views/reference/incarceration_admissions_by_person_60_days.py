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
# pylint: disable=trailing-whitespace, line-too-long
"""View for the incarceration admissions in the last 60 days, deduped by
 person."""
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_VIEW_NAME = 'incarceration_admissions_by_person_60_days'

INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_DESCRIPTION = \
    """ Incarceration admissions in last 60 days.
    This includes new admissions and admissions for supervision revocation.
    Excludes all other reasons for admission.
    
    In the rare case that someone has more than one new admission in the 60-day
    window, the earliest admission is chosen.
    """

INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_QUERY = \
    """
/*{description}*/

SELECT * EXCEPT (rownum) FROM
(SELECT *, row_number() OVER (PARTITION BY person_id ORDER BY admission_date) AS rownum
FROM
(SELECT * FROM 
`{project_id}.{views_dataset}.incarceration_admissions_deduped`
WHERE admission_date BETWEEN (DATE_ADD(CURRENT_DATE(), INTERVAL -60 DAY))
AND CURRENT_DATE()))
where rownum = 1

""".format(
        description=INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_VIEW = bqview.BigQueryView(
    view_id=INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_VIEW_NAME,
    view_query=INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_QUERY
)

if __name__ == '__main__':
    print(INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_VIEW.view_id)
    print(INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_VIEW.view_query)
