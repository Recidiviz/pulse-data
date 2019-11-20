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
"""Reincarcerations by month."""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET

REINCARCERATIONS_BY_MONTH_VIEW_NAME = 'reincarcerations_by_month'

REINCARCERATIONS_BY_MONTH_DESCRIPTION = """ Reincarcerations by month """

REINCARCERATIONS_BY_MONTH_QUERY = \
    """
    /*{description}*/
    
    SELECT state_code, EXTRACT(YEAR FROM start_date) as year, EXTRACT(MONTH FROM start_date) as month, returns
    FROM `{project_id}.{metrics_dataset}.recidivism_count_metrics`
    WHERE methodology = 'PERSON' and age_bucket is null and stay_length_bucket is null and race is null and ethnicity is null and gender is null and release_facility is null and return_type is null and from_supervision_type is null and source_violation_type is null and job_id in
    -- Get only from the most recent job
    (SELECT job_id FROM `{project_id}.{views_dataset}.most_recent_calculate_job`)
    AND EXTRACT(YEAR FROM start_date) > EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
    -- Get only month buckets
    AND EXTRACT(MONTH FROM start_date) = EXTRACT(MONTH FROM end_date)
    ORDER BY year, month
    """.format(
        description=REINCARCERATIONS_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
        )

REINCARCERATIONS_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=REINCARCERATIONS_BY_MONTH_VIEW_NAME,
    view_query=REINCARCERATIONS_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(REINCARCERATIONS_BY_MONTH_VIEW.view_id)
    print(REINCARCERATIONS_BY_MONTH_VIEW.view_query)
