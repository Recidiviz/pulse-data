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
"""A person's second and last LSIR score of a supervision period that has
been terminated, with the month and year of termination.

Note: We are looking at the second LSIR score during the supervision period
because the first one is often considered to be "ignorable" by officers in the
field.
"""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_VIEW_NAME = 'second_and_last_lsir_scores_by_supervision_period_month'

SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_DESCRIPTION = \
    """A person's second and last LSIR score of a supervision period that has
been terminated, with the month and year of termination."""

SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_QUERY = \
    """
    /*{description}*/

    SELECT state_code, second.person_id, second.second_score, second.second_date, latest.last_score, latest.last_date, second.termination_year, second.termination_month, (latest.last_score - second.second_score) as score_change
    FROM
    (SELECT state_code,  termination_year, termination_month, person_id, assessment_date as second_date, assessment_score as second_score
    FROM
    (SELECT state_code, termination_year, termination_month, person_id, assessment_date, assessment_score, row_number() over (PARTITION BY person_id, termination_year, termination_month) as rownum
    FROM `{project_id}.{views_dataset}.lsir_scores_by_person_period_and_date_month`) as x where x.rownum = 2) second
    JOIN      
    (SELECT scores.termination_year, scores.termination_month, scores.person_id, last.last_date, scores.assessment_score as last_score
    FROM
    (SELECT termination_year, termination_month, person_id, max(assessment_date) as last_date
    FROM `{project_id}.{views_dataset}.lsir_scores_by_person_period_and_date_month`
    GROUP BY person_id, termination_year, termination_month) last
    INNER JOIN `{project_id}.{views_dataset}.lsir_scores_by_person_period_and_date_month` scores
    ON last.person_id = scores.person_id and last.termination_year = scores.termination_year and last.termination_month = scores.termination_month AND last.last_date = scores.assessment_date) latest
    ON second.person_id = latest.person_id and second.termination_year = latest.termination_year and second.termination_month = latest.termination_month
    WHERE second.second_date != latest.last_date
    ORDER BY person_id
    """.format(
        description=SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_VIEW = bqview.BigQueryView(
    view_id=SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_VIEW_NAME,
    view_query=SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_QUERY
)

if __name__ == '__main__':
    print(SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_VIEW.view_id)
    print(SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_VIEW.view_query)
