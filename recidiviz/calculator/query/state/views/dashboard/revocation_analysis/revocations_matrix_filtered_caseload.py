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
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_NAME = 'revocations_matrix_filtered_caseload'

REVOCATIONS_MATRIX_FILTERED_CASELOAD_DESCRIPTION = """
 Person-level violation and caseload information for all of the people revoked to prison from supervision.
 """

REVOCATIONS_MATRIX_FILTERED_CASELOAD_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH supervision_type_ranks_by_person AS (
      SELECT
        state_code,
        metric_period_months,
        person_external_id,
        supervision_type,
        -- We only want to include a supervision_type=ALL row if that person has no other set supervision types
        ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_external_id
                            ORDER BY supervision_type != 'ALL' DESC) as inclusion_order
      FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person_materialized` 
    ), people_without_supervision_types_to_include AS (
      SELECT 
        state_code,
        metric_period_months,
        person_external_id,
        supervision_type
      FROM supervision_type_ranks_by_person
      WHERE inclusion_order = 1
    )
    
    
    SELECT
      state_code,
      IFNULL(person_external_id, 'UNKNOWN') AS state_id,
      officer,
      officer_recommendation,
      violation_record,
      CASE
        WHEN state_code = 'US_MO' THEN level_1_supervision_location
        WHEN state_code = 'US_PA' THEN level_2_supervision_location
        ELSE level_1_supervision_location
      END AS district,
      level_1_supervision_location,
      level_2_supervision_location,
      sup_type.supervision_type,
      supervision_level,
      charge_category,
      risk_level,
      violation_type,
      reported_violations,
      metric_period_months
    FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person_materialized` 
    LEFT JOIN
        people_without_supervision_types_to_include sup_type
    USING (state_code, metric_period_months, person_external_id)
    WHERE CASE
        -- TODO(#4524): Once the front end supports the file size increase of multi-district breakdowns and we stop 
        -- filtering out hydrated level_1_supervision_location breakdown rows, we can remove this PA special case.
        WHEN state_code = 'US_PA' THEN true
        ELSE level_1_supervision_location != 'ALL'
    END
    AND CASE
        -- TODO(#3829): MO does not have level 2 values ingested, so level_2_supervision_location values are only
        -- 'ALL'. Once we do start ingesting MO region information, this query size will temporarily increase until
        -- we update this query to remove the MO special case.
        WHEN state_code = 'US_MO' THEN true
        ELSE level_2_supervision_location != 'ALL'
    END 
    -- State-specific filtering to allow ALL values for states where the dimension is disabled on the FE --
    AND (state_code = 'US_PA' OR charge_category != 'ALL')
    AND (state_code = 'US_MO' or (supervision_level != 'ALL'))
    AND violation_type != 'ALL'
    AND reported_violations != 'ALL'
    """

REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_FILTERED_CASELOAD_QUERY_TEMPLATE,
    dimensions=['state_code', 'metric_period_months', 'district', 'level_1_supervision_location',
                'level_2_supervision_location', 'supervision_type', 'supervision_level',
                'charge_category', 'risk_level', 'violation_type', 'reported_violations', 'state_id', 'officer'],
    description=REVOCATIONS_MATRIX_FILTERED_CASELOAD_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_specific_officer_recommendation=state_specific_query_strings.state_specific_officer_recommendation(),
    state_specific_supervision_level=state_specific_query_strings.state_specific_supervision_level(),
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_BUILDER.build_and_print()
