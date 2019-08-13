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
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.bq import bqview
from recidiviz.calculator.bq.dashboard.views import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET

DAYS_AT_LIBERTY_BY_MONTH_VIEW_NAME = 'avg_days_at_liberty_by_month'

DAYS_AT_LIBERTY_BY_MONTH_DESCRIPTION = """ Average days at liberty for reincarcerations by month """

DAYS_AT_LIBERTY_BY_MONTH_QUERY = \
    """
    /*{description}*/

    SELECT state_code, EXTRACT(YEAR FROM start_date) as year, EXTRACT(MONTH FROM start_date) as month, returns, avg_liberty
    FROM `{project_id}.{metrics_dataset}.recidivism_liberty_metrics`
    WHERE methodology = 'PERSON' AND age_bucket is null AND stay_length_bucket is null AND race is null AND ethnicity is null AND gender is null AND release_facility is null AND return_type is null AND from_supervision_type is null AND source_violation_type is null 
    AND job_id in
    -- Get only from the most recent job
    (SELECT job_id FROM `{project_id}.{views_dataset}.most_recent_calculate_job`)
    AND EXTRACT(YEAR FROM start_date) > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
    -- Get only month buckets
    AND EXTRACT(MONTH FROM start_date) = EXTRACT(MONTH FROM end_date)
    ORDER BY year, month
    """.format(
        description=DAYS_AT_LIBERTY_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
    )

DAYS_AT_LIBERTY_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=DAYS_AT_LIBERTY_BY_MONTH_VIEW_NAME,
    view_query=DAYS_AT_LIBERTY_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(DAYS_AT_LIBERTY_BY_MONTH_VIEW.view_id)
    print(DAYS_AT_LIBERTY_BY_MONTH_VIEW.view_query)
