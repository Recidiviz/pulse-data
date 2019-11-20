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
"""LSIR scores by person with supervision period start and end data."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.calculator.query import export_config, bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

LSIR_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_VIEW_NAME = 'lsir_scores_by_person_period_and_date_month'

LSIR_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_DESCRIPTION = \
    """LSIR scores by person with supervision period start and end data."""

LSI_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_QUERY = \
    """
    /*{description}*/

    SELECT sp.state_code, sp.start_date, EXTRACT(YEAR FROM sp.start_date) as start_year, sp.termination_date, EXTRACT(YEAR FROM sp.termination_date) as termination_year,
    EXTRACT(MONTH from sp.termination_date) as termination_month, sp.person_id, ass.assessment_date, ass.assessment_score
    FROM
    `{project_id}.{views_dataset}.supervision_termination_earliest_start_latest_end` sp 
    JOIN `{project_id}.{base_dataset}.state_assessment` ass ON sp.person_id = ass.person_id 
    WHERE sp.termination_date is not null AND ass.assessment_type = 'LSIR'
    AND ass.assessment_date BETWEEN sp.start_date AND sp.termination_date
    ORDER BY sp.person_id, sp.start_date asc, ass.assessment_date ASC
    """.format(
        description=LSIR_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        base_dataset=BASE_DATASET,
        views_dataset=VIEWS_DATASET,
    )

LSIR_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_VIEW = bqview.BigQueryView(
    view_id=LSIR_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_VIEW_NAME,
    view_query=LSI_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_QUERY
)

if __name__ == '__main__':
    print(LSIR_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_VIEW.view_id)
    print(LSIR_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_VIEW.view_query)
