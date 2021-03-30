# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Supervisees with pending FTR by PO by day."""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PENDING_FTR_BY_PO_BY_DAY_VIEW_NAME = "pending_ftr_by_po_by_day"

PENDING_FTR_BY_PO_BY_DAY_DESCRIPTION = """
    Supervisees with pending FTR by PO by day
 """

PENDING_FTR_BY_PO_BY_DAY_QUERY_TEMPLATE = """
    /*{description}*/
    # Get all of the job ids in program referral metrics.
    WITH pending_ftr AS (
        WITH all_job_ids AS (
          SELECT
            DISTINCT job_id,
            state_code,
            created_on,
            metric_type,
          FROM `{project_id}.{metrics_dataset}.program_referral_metrics`
        ),
        # Rank the job ids by recency.
        ranked_job_ids_by_day AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY state_code, metric_type, created_on ORDER BY job_id DESC) AS day_recency_rank
          FROM all_job_ids
        ),
        # Get the most recent run for each of the days.
        job_id_per_day AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY state_code, metric_type ORDER BY created_on DESC) AS recency_rank
          FROM ranked_job_ids_by_day
          WHERE day_recency_rank = 1
        ),
        # Get only the most recent week of job ids.
        most_recent_week_of_program_referrals AS (
          SELECT
              *
          FROM job_id_per_day
          WHERE recency_rank <= 7
        )
        # Get the number of supervisees with pending FTR status by district and PO, for each of the seven days.
        SELECT
            referral_metrics.state_code,
            most_recent_week_of_program_referrals.created_on,
            IFNULL(supervising_officer_external_id, 'UNKNOWN') as supervising_officer_external_id,
            IFNULL(referral_metrics.level_1_supervision_location_external_id, 'UNKNOWN') as level_1_supervision_location_external_id,
            COUNTIF(referral_metrics.participation_status = "PENDING") as pending_ftr_count,
        FROM
          `{project_id}.{metrics_dataset}.program_referral_metrics` referral_metrics
        INNER JOIN
          most_recent_week_of_program_referrals
        USING (state_code, job_id, metric_type)
        # TODO(#6614): Generalize to be state agnostic.
        WHERE state_code = "US_ND"
        GROUP BY state_code, created_on, supervising_officer_external_id, level_1_supervision_location_external_id
    )
    SELECT 
        pending_ftr.state_code,
        created_on as date_of_supervision,
        pending_ftr.supervising_officer_external_id,
        pending_ftr.level_1_supervision_location_external_id as district_id,
        locations.level_1_supervision_location_name as district_name,
        pending_ftr_count,
        sup_pop.people_under_supervision as total_under_supervision,
        SAFE_DIVIDE((sup_pop.people_under_supervision - pending_ftr_count), sup_pop.people_under_supervision) * 100 AS timely_ftr_enrollment,
    FROM pending_ftr 
    LEFT JOIN `{project_id}.{reference_views_dataset}.supervision_location_ids_to_names` locations
        ON pending_ftr.state_code = locations.state_code
        AND pending_ftr.level_1_supervision_location_external_id = locations.level_1_supervision_location_external_id
    LEFT JOIN `{project_id}.{vitals_views_dataset}.supervision_population_by_po_by_day_materialized` sup_pop
        ON sup_pop.state_code = pending_ftr.state_code
        AND sup_pop.date_of_supervision = pending_ftr.created_on
        AND sup_pop.supervising_officer_external_id = pending_ftr.supervising_officer_external_id
        AND sup_pop.supervising_district_external_id = pending_ftr.level_1_supervision_location_external_id
    """

PENDING_FTR_BY_PO_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=PENDING_FTR_BY_PO_BY_DAY_VIEW_NAME,
    view_query_template=PENDING_FTR_BY_PO_BY_DAY_QUERY_TEMPLATE,
    description=PENDING_FTR_BY_PO_BY_DAY_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    vitals_views_dataset=dataset_config.VITALS_REPORT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PENDING_FTR_BY_PO_BY_DAY_VIEW_BUILDER.build_and_print()
