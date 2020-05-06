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
"""Most recent calculate job_id by metric and state code."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import dataset_config

MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_NAME = \
    'most_recent_job_id_by_metric_and_state_code'

MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_DESCRIPTION = \
    """ Job ID of the most recent calculate job by metric and state code.
    
    All job_ids begin with the format: 'YYYY-MM-DD_HH_MM_SS', so ordering
    by job_id gives us the most recent job. This format is true of both jobs
    run locally and run on Dataflow.
    """

MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT metric_type, state_code, year, month, metric_period_months, job_id
    FROM (
        SELECT *, row_number() OVER (PARTITION BY state_code, year, month, metric_period_months, metric_type ORDER BY job_id DESC) AS recency_rank
        FROM (
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'RECIDIVISM_COUNT' as metric_type
            FROM `{project_id}.{metrics_dataset}.recidivism_count_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, NULL AS year, NULL AS month, NULL AS metric_period_months, state_code, 'RECIDIVISM_RATE' as metric_type
            FROM `{project_id}.{metrics_dataset}.recidivism_rate_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'INCARCERATION_ADMISSION' as metric_type
            FROM `{project_id}.{metrics_dataset}.incarceration_admission_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'INCARCERATION_POPULATION' as metric_type
            FROM `{project_id}.{metrics_dataset}.incarceration_population_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'INCARCERATION_RELEASE' as metric_type
            FROM `{project_id}.{metrics_dataset}.incarceration_release_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'SUPERVISION_ASSESSMENT_CHANGE' as metric_type
            FROM `{project_id}.{metrics_dataset}.terminated_supervision_assessment_score_change_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'SUPERVISION_POPULATION' as metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'SUPERVISION_REVOCATION' as metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_revocation_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'SUPERVISION_REVOCATION_ANALYSIS' as metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'SUPERVISION_REVOCATION_VIOLATION' as metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_revocation_violation_type_analysis_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'SUPERVISION_SUCCESS' as metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_success_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'SUCCESSFUL_SENTENCE_DAYS_SERVED' as metric_type
            FROM `{project_id}.{metrics_dataset}.successful_supervision_sentence_days_served_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, 'PROGRAM_REFERRAL' as metric_type
            FROM `{project_id}.{metrics_dataset}.program_referral_metrics`)
        )
    )
    WHERE recency_rank = 1
    ORDER BY metric_type, state_code, year, month, metric_period_months
    """

MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW = BigQueryView(
    dataset_id=dataset_config.REFERENCE_TABLES_DATASET,
    view_id=MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_NAME,
    view_query_template=MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_QUERY_TEMPLATE,
    description=MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
)

if __name__ == '__main__':
    print(MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW.view_id)
    print(MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW.view_query)
