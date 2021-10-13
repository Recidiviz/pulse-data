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
"""Revocations Matrix Filtered Caseload."""
from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_NAME = "revocations_matrix_filtered_caseload"

REVOCATIONS_MATRIX_FILTERED_CASELOAD_DESCRIPTION = """
 Person-level violation and caseload information for all of the people revoked to prison from supervision.
 """

REVOCATIONS_MATRIX_FILTERED_CASELOAD_QUERY_TEMPLATE = """
    /*{description}*/
  WITH inclusion_ranks_by_person AS (
    SELECT
      state_code,
      person_external_id,
      person_id AS internal_person_id,
      officer,
      officer_full_name,
      officer_recommendation,
      violation_record,
      CASE
        WHEN state_code = 'US_MO' THEN level_1_supervision_location
        WHEN state_code = 'US_PA' THEN level_2_supervision_location
        ELSE level_1_supervision_location
      END AS district,
      level_1_supervision_location,
      level_2_supervision_location,
      supervision_type,
      supervision_level,
      charge_category,
      risk_level,
      violation_type,
      reported_violations,
      metric_period_months,
      admission_type,
      admission_history_description,
      ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_external_id
                           -- We only want to include an ALL value if that person has no other set values
                           -- for the dimension
                            ORDER BY supervision_type != 'ALL' DESC,
                                     supervision_level != 'ALL' DESC,
                                     charge_category != 'ALL' DESC,
                                     admission_type != 'ALL' DESC,
                                     level_1_supervision_location != 'ALL' DESC,
                                     level_2_supervision_location != 'ALL' DESC) AS inclusion_order
      FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person_materialized`
      WHERE violation_type != 'ALL'
      AND reported_violations != 'ALL'
    ), caseload_by_person AS (
      SELECT
        state_code,
        -- We use UNKNOWN instead of EXTERNAL_UNKNOWN here because this value is seen on the FE --
        IFNULL(person_external_id, 'UNKNOWN') AS state_id,
        internal_person_id,
        officer,
        officer_full_name,
        officer_recommendation,
        violation_record,
        IF(district = 'ALL', 'EXTERNAL_UNKNOWN', district) AS district,
        IF(level_1_supervision_location = 'ALL', 'EXTERNAL_UNKNOWN', level_1_supervision_location) AS level_1_supervision_location,
        -- TODO(#3829): MO does not have level 2 values ingested, so level_2_supervision_location values are only
        -- 'ALL'. Once we do start ingesting MO region information and the front end supports the multi-district
        -- breakdown for US_MO we can remove this MO special case and replace with:
        -- IF(level_2_supervision_location = 'ALL', 'EXTERNAL_UNKNOWN', level_2_supervision_location) AS level_2_supervision_location,
        CASE WHEN state_code = 'US_MO' THEN 'ALL'
             WHEN level_2_supervision_location = 'ALL' THEN 'EXTERNAL_UNKNOWN'
             ELSE level_2_supervision_location
             END AS level_2_supervision_location,
        -- supervision_type is always ALL in US_PA -- 
        CASE WHEN state_code = 'US_PA' THEN 'ALL'
             WHEN supervision_type = 'ALL' THEN 'EXTERNAL_UNKNOWN'
             ELSE supervision_type
             END AS supervision_type,   
        -- supervision_level is always ALL in US_MO --
        CASE WHEN state_code = 'US_MO' THEN 'ALL'
             WHEN supervision_level = 'ALL' THEN 'EXTERNAL_UNKNOWN'
             ELSE supervision_level
             END AS supervision_level,
        -- charge_category is always ALL in US_PA --
        CASE WHEN state_code = 'US_PA' THEN 'ALL'
             WHEN charge_category = 'ALL' THEN 'EXTERNAL_UNKNOWN'
             ELSE charge_category
             END AS charge_category,
        -- admission_type is always ALL in US_MO --
        CASE WHEN state_code = 'US_MO' THEN 'ALL'
             WHEN admission_type = 'ALL' THEN 'EXTERNAL_UNKNOWN'
             ELSE admission_type
             END AS admission_type,
        admission_history_description,
        risk_level,
        violation_type,
        reported_violations,
        metric_period_months
      FROM inclusion_ranks_by_person
      WHERE inclusion_order = 1
    )
    
    
    SELECT
      *
    FROM
      caseload_by_person
    ORDER BY state_code, state_id, metric_period_months
    """

REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_FILTERED_CASELOAD_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "metric_period_months",
        "district",
        "level_1_supervision_location",
        "level_2_supervision_location",
        "admission_type",
        "supervision_type",
        "supervision_level",
        "charge_category",
        "risk_level",
        "violation_type",
        "reported_violations",
        "state_id",
        "officer",
    ),
    description=REVOCATIONS_MATRIX_FILTERED_CASELOAD_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_BUILDER.build_and_print()
