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
"""Most recent calculate job_id for the most recent metric date for daily metrics, by metric type and state code."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

MOST_RECENT_DAILY_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_NAME = \
    'most_recent_daily_job_id_by_metric_and_state_code'

MOST_RECENT_DAILY_JOB_ID_BY_METRIC_AND_STATE_CODE_DESCRIPTION = \
    """ Job ID of the most recent calculate job for the most recent metric date for daily metrics, by metric and state
    code.

    All job_ids begin with the format: 'YYYY-MM-DD_HH_MM_SS', so ordering by job_id gives us the most recent job. This
    format is true of both jobs run locally and run on Dataflow.
    """

MOST_RECENT_DAILY_JOB_ID_BY_METRIC_AND_STATE_CODE_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH all_job_ids AS (
      (SELECT DISTINCT job_id, state_code, date_of_stay as metric_date, metric_type
      FROM `{project_id}.{metrics_dataset}.incarceration_population_metrics`)
      UNION ALL
      (SELECT DISTINCT job_id, state_code, date_of_supervision as metric_date, metric_type
      FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`)
      UNION ALL
      (SELECT DISTINCT job_id, state_code, date_of_participation as metric_date, metric_type
      FROM `{project_id}.{metrics_dataset}.program_participation_metrics`)
      UNION ALL
      (SELECT DISTINCT job_id, state_code, revocation_admission_date as metric_date, metric_type
      FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`)
    ), ranked_job_ids AS (
      SELECT *, row_number() OVER (PARTITION BY state_code, metric_type ORDER BY metric_date DESC, job_id DESC) AS recency_rank
      FROM all_job_ids
    )
    
    SELECT * FROM ranked_job_ids WHERE recency_rank = 1
    ORDER BY metric_type, state_code
    """

MOST_RECENT_DAILY_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    view_id=MOST_RECENT_DAILY_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_NAME,
    should_materialize=True,
    view_query_template=MOST_RECENT_DAILY_JOB_ID_BY_METRIC_AND_STATE_CODE_QUERY_TEMPLATE,
    description=MOST_RECENT_DAILY_JOB_ID_BY_METRIC_AND_STATE_CODE_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        MOST_RECENT_DAILY_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_BUILDER.build_and_print()
