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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

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
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.recidivism_count_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, NULL AS year, NULL AS month, NULL AS metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.recidivism_rate_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.incarceration_admission_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.incarceration_population_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.incarceration_release_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_termination_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_case_compliance_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_downgrade_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_revocation_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_revocation_violation_type_analysis_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_success_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.successful_supervision_sentence_days_served_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.program_referral_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.program_participation_metrics`)
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_out_of_state_population_metrics`) 
            UNION ALL
            (SELECT DISTINCT job_id, year, month, metric_period_months, state_code, metric_type
            FROM `{project_id}.{metrics_dataset}.supervision_start_metrics`)
        )
    )
    WHERE recency_rank = 1
    ORDER BY metric_type, state_code, year, month, metric_period_months
    """

MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_NAME,
    should_materialize=True,
    view_query_template=MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_QUERY_TEMPLATE,
    description=MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_BUILDER.build_and_print()
