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
"""Successful and unsuccessful terminations of supervision by month."""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW_NAME = 'supervision_termination_by_type_by_month'

SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_DESCRIPTION = """
 Supervision termination by type and by month.
 The counts of supervision that were projected to end in a given month and
 that have ended by now, broken down by whether or not the
 supervision ended because of a revocation or successful completion.
"""

SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_QUERY = \
    """
    /*{description}*/

    SELECT IFNULL(rev.state_code, term.state_code) as state_code, IFNULL(rev.projected_year, term.projected_year) AS projected_year, IFNULL(rev.projected_month, term.projected_month) AS projected_month, term.count AS successful_termination, rev.count AS revocation_termination
    FROM
    (SELECT state_code, projected_year, projected_month, IFNULL(count, 0) AS count
    FROM
    (SELECT state_code, projected_year, projected_month, count(*) as count
    FROM `{project_id}.{views_dataset}.supervision_success_by_person`
    WHERE termination_success = FALSE
    GROUP BY state_code, projected_year, projected_month HAVING projected_year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR)))) rev
    FULL OUTER JOIN
    (SELECT state_code, projected_year, projected_month, IFNULL(count, 0) AS count
    FROM
    (SELECT state_code, projected_year, projected_month, count(*) as count
    FROM `{project_id}.{views_dataset}.supervision_success_by_person`
    WHERE termination_success = TRUE
    GROUP BY state_code, projected_year, projected_month HAVING projected_year > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR)))) term
    ON rev.projected_year = term.projected_year and rev.projected_month = term.projected_month
    ORDER BY projected_year, projected_month ASC
    """.format(
        description=SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW_NAME,
    view_query=SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW.view_id)
    print(SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW.view_query)
