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
"""Admissions minus releases (net change in incarcerated population)"""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_NAME = 'admissions_versus_releases_by_month'

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_DESCRIPTION = """ Monthly admissions versus releases """

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_QUERY = \
    """
/*{description}*/

SELECT IFNULL(adm.state_code, rel.state_code) as state_code, IFNULL(adm.year, rel.year) as year, IFNULL(adm.month, rel.month) as month, IFNULL(admissions, 0) - IFNULL(releases, 0) as population_change
FROM
(SELECT state_code, EXTRACT(YEAR from start_date) as year, EXTRACT(MONTH from start_date) as month, IFNULL(admission_count, 0) as admissions
FROM
(SELECT state_code, DATE_TRUNC(admission_date, month) as start_date, DATE_ADD(DATE_ADD(DATE_TRUNC(admission_date, month), INTERVAL 1 MONTH), INTERVAL -1 DAY) as end_date, count(*) as admission_count
FROM `{project_id}.{views_dataset}.incarceration_admissions_by_person_and_month`
GROUP BY state_code, start_date, end_date
ORDER BY end_date desc)) adm
FULL OUTER JOIN
(SELECT state_code, EXTRACT(YEAR from start_date) as year, EXTRACT(MONTH from start_date) as month, IFNULL(release_count, 0) as releases
FROM
(SELECT state_code, DATE_TRUNC(release_date, month) as start_date, DATE_ADD(DATE_ADD(DATE_TRUNC(release_date, month), INTERVAL 1 MONTH), INTERVAL -1 DAY) as end_date, count(*) as release_count
FROM `{project_id}.{views_dataset}.incarceration_releases_by_person_and_month`
GROUP BY state_code, start_date, end_date
ORDER BY end_date desc)) rel 
ON adm.year = rel.year AND adm.month = rel.month
WHERE IFNULL(adm.year, rel.year) > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
ORDER BY year, month ASC 
""".format(
        description=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_NAME,
    view_query=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW.view_id)
    print(ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW.view_query)
