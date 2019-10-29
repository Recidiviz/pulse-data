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
"""The average change in LSIR score by month of scheduled supervision
termination.

Per ND-specific request, compares the LSIR score at termination to the second
LSIR score of the person's supervision."""
# pylint: disable=line-too-long, trailing-whitespace

from recidiviz.calculator.bq import bqview
from recidiviz.calculator.bq.dashboard.views import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW_NAME = 'average_change_lsir_score_by_month'

AVERAGE_CHANGE_LSIR_SCORE_MONTH_DESCRIPTION = """
    The average change in LSIR score by month of scheduled supervision termination.
    Compares the LSIR score at termination to the second LSIR score of the person's supervision.
"""

AVERAGE_CHANGE_LSIR_SCORE_MONTH_QUERY = \
    """
    /*{description}*/

    SELECT state_code, termination_year, termination_month, AVG(score_change) AS average_change
    FROM `{project_id}.{views_dataset}.second_and_last_lsir_scores_by_supervision_period_month`
    GROUP BY state_code, termination_year, termination_month
    ORDER BY state_code, termination_year, termination_month ASC
    """.format(
        description=AVERAGE_CHANGE_LSIR_SCORE_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
    )

AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW = bqview.BigQueryView(
    view_id=AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW_NAME,
    view_query=AVERAGE_CHANGE_LSIR_SCORE_MONTH_QUERY
)

if __name__ == '__main__':
    print(AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW.view_id)
    print(AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW.view_query)
