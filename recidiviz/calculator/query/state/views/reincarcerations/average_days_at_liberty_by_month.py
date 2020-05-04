# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
#
# This program is free software: you can redistribute it AND/or modify
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
"""Average days at liberty for reincarcerations by month."""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW_NAME = 'avg_days_at_liberty_by_month'

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_DESCRIPTION = \
    """Average days at liberty for reincarcerations by month """

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_QUERY = \
    """
    /*{description}*/
    SELECT
      state_code, year, month,
      COUNT(DISTINCT person_id) AS returns,
      AVG(days_at_liberty) AS avg_liberty
    FROM `{project_id}.{metrics_dataset}.recidivism_count_metrics`
    JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
      USING (state_code, year, month, metric_period_months, job_id)
    WHERE methodology = 'PERSON'
      AND person_id IS NOT NULL
      AND metric_period_months = 1
      AND month IS NOT NULL
      AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
      AND job.metric_type = 'RECIDIVISM_COUNT'
      -- TODO (#3123): enforce positive days at liberty earlier in the pipeline
      AND days_at_liberty >= 0
    GROUP BY state_code, year, month
    ORDER BY state_code, year, month
    """.format(
        description=AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        reference_dataset=REFERENCE_DATASET,
    )

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW = BigQueryView(
    view_id=AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW_NAME,
    view_query=AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW.view_id)
    print(AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW.view_query)
