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
"""Supervision terminations collapsed by person and the month of termination.
If a person has multiple supervision periods that end in a given month, the
the earliest start date and the latest termination date of the periods is taken.
"""

# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.bq import bqview
from recidiviz.calculator.bq.dashboard.views import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET


SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END = 'supervision_termination_earliest_start_latest_end'

SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END_DESCRIPTION = \
    """ Supervision terminations collapsed by person and the month of termination.
    If a person has multiple supervision periods that end in a given month, the
    the earliest start date and the latest termination date of the periods is taken.
    """

SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END_QUERY = \
    """
    /*{description}*/
    
    SELECT start.state_code, start.person_id, start_date, termination_date, start.termination_year, start.termination_month
    FROM 
    (SELECT state_code, person_id, start_date, termination_year, termination_month
    FROM
    (SELECT state_code, person_id, start_date, start_year, termination_date, termination_year, termination_month,
    row_number() over (PARTITION BY person_id, termination_year, termination_month ORDER BY start_date asc) as rownum
    FROM
    `{project_id}.{views_dataset}.supervision_terminations_10_years`)
    WHERE rownum = 1) start
    JOIN
    (SELECT state_code, person_id, termination_date, termination_year, termination_month
    FROM
    (SELECT state_code, person_id, start_date, start_year, termination_date, termination_year, termination_month,
    row_number() over (PARTITION BY person_id, termination_year, termination_month ORDER BY termination_date desc) as rownum
    FROM
    `{project_id}.{views_dataset}.supervision_terminations_10_years`)
    WHERE rownum = 1) term
    ON start.state_code = term.state_code AND start.person_id = term.person_id 
    AND start.termination_year = term.termination_year AND start.termination_month = term.termination_month
    ORDER BY person_id
""".format(
        description=SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END,
    view_query=SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END_VIEW.view_id)
    print(SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END_VIEW.view_query)
