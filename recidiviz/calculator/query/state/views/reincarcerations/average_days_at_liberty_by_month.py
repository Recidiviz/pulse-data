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
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW_NAME = 'avg_days_at_liberty_by_month'

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_DESCRIPTION = \
    """Average days at liberty for reincarcerations by month """

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_QUERY = \
    """
    /*{description}*/
    SELECT 
      state_code, 
      EXTRACT(YEAR FROM start_date) as year, EXTRACT(MONTH FROM start_date) as month, 
      returns, 
      avg_liberty
    FROM `{project_id}.{metrics_dataset}.recidivism_liberty_metrics`
    JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
      USING (state_code, job_id)
    WHERE methodology = 'PERSON'
      AND age_bucket IS NULL
      AND stay_length_bucket IS NULL
      AND race IS NULL
      AND ethnicity IS NULL
      AND gender IS NULL
      AND release_facility IS NULL
      AND return_type IS NULL
      AND from_supervision_type IS NULL
      AND source_violation_type IS NULL
      AND county_of_residence IS NULL
      AND EXTRACT(YEAR FROM start_date) >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
      AND EXTRACT(MONTH FROM start_date) = EXTRACT(MONTH FROM end_date)
      AND job.metric_type = 'RECIDIVISM_LIBERTY'
    ORDER BY state_code, year, month
    """.format(
        description=AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
    )

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW_NAME,
    view_query=AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW.view_id)
    print(AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW.view_query)
