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
# pylint: disable=trailing-whitespace

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
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
    SELECT * FROM (
      SELECT 
        state_code, year as projected_year, month as projected_month, 
        successful_completion_count as successful_termination, 
        (projected_completion_count - successful_completion_count) as revocation_termination, 
        IFNULL(supervision_type, 'ALL') as supervision_type, 
        IFNULL(supervising_district_external_id, 'ALL') as district 
      FROM `{project_id}.{metrics_dataset}.supervision_success_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND metric_period_months = 1
        AND month IS NOT NULL
        AND supervising_officer_external_id IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL 
        AND year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
        AND job.metric_type = 'SUPERVISION_SUCCESS'
    )
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
    ORDER BY state_code, projected_year, projected_month, district, supervision_type
    """.format(
        description=SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
        metrics_dataset=METRICS_DATASET,
    )

SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW_NAME,
    view_query=SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW.view_id)
    print(SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW.view_query)
