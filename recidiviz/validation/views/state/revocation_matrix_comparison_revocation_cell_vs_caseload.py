# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""A view which provides a comparison of total revocation counts in the initial, unfiltered view of the
Revocation Analysis Tool: the grid cells and the corresponding caseload chart across all filter combinations."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_NAME = (
    "revocation_matrix_comparison_revocation_cell_vs_caseload"
)

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_DESCRIPTION = """ 
Revocation matrix comparison of summed revocation counts between the grid cells and the month chart """

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_QUERY_TEMPLATE = """
    /*{description}*/
    WITH cell_counts AS (
      SELECT 
        state_code AS region_code, metric_period_months, level_1_supervision_location, level_2_supervision_location,
        charge_category, supervision_type, supervision_level,
        SUM(total_revocations) AS total_revocations
      FROM `{project_id}.{view_dataset}.revocations_matrix_cells`
      -- Only including dimensional combinations that can be compared between the matrix and the caseload --
      WHERE 
          (level_1_supervision_location != 'ALL'
            AND CASE
                -- TODO(#3829): MO does not have level 2 values ingested, so level_2_supervision_location values are all
                -- 'ALL'. Once we do start ingesting MO region information, this query size will temporarily increase
                -- until we update this query to remove the MO special case.
                WHEN state_code = 'US_MO' THEN true
                ELSE level_2_supervision_location != 'ALL'
            END 
          AND (state_code = 'US_PA' OR (supervision_type NOT IN ('ALL', 'EXTERNAL_UNKNOWN')
                                        AND charge_category NOT IN ('ALL', 'EXTERNAL_UNKNOWN')))
          AND (state_code = 'US_MO' or (supervision_level NOT IN ('ALL', 'EXTERNAL_UNKNOWN')))
          )
        OR 
          (level_1_supervision_location = 'ALL'
          AND level_2_supervision_location = 'ALL'
          AND charge_category = 'ALL'
          AND supervision_type = 'ALL'
          AND supervision_level = 'ALL')
      GROUP BY state_code, metric_period_months, level_1_supervision_location, level_2_supervision_location,
               charge_category, supervision_type, supervision_level
    ),
    caseload_counts AS (
      SELECT 
        state_code AS region_code, metric_period_months, level_1_supervision_location, level_2_supervision_location,
        charge_category, supervision_type, supervision_level,
        COUNT(DISTINCT state_id) AS total_revocations,
        -- This should always be equal to the total_revocations since a single state_id should never be included
        -- more than once in a given dimensional breakdown
        COUNT(*) AS total_rows
      FROM `{project_id}.{view_dataset}.revocations_matrix_filtered_caseload`
      -- Only including dimensional combinations that can be compared between the matrix and the caseload --
      WHERE reported_violations != 'ALL' AND violation_type NOT IN ('ALL', 'NO_VIOLATION_TYPE')
          AND 
          (level_1_supervision_location NOT IN ('ALL', 'EXTERNAL_UNKNOWN')
            AND CASE
                -- TODO(#3829): MO does not have level 2 values ingested, so level_2_supervision_location values are all
                -- 'ALL'. Once we do start ingesting MO region information, this query size will temporarily increase
                -- until we update this query to remove the MO special case.
                WHEN state_code = 'US_MO' THEN true
                ELSE level_2_supervision_location NOT IN ('ALL', 'EXTERNAL_UNKNOWN')
            END
          AND (state_code = 'US_PA' OR (supervision_type NOT IN ('ALL', 'EXTERNAL_UNKNOWN')
                                        AND charge_category NOT IN ('ALL', 'EXTERNAL_UNKNOWN')))
          AND (state_code = 'US_MO' or (supervision_level NOT IN ('ALL', 'EXTERNAL_UNKNOWN'))))
      GROUP BY state_code, metric_period_months, level_1_supervision_location, level_2_supervision_location,
               charge_category, supervision_type, supervision_level
      
      UNION ALL
      
      -- The caseload has only 1 row per region_code, metric_period_months, and state_id. We want to make sure
      -- the sum over all dimensions is the same as the sum where all fields are ALL in the matrix --
      SELECT
        state_code AS region_code,
        metric_period_months,
        'ALL' AS level_1_supervision_location,
        'ALL' AS level_2_supervision_location,
        'ALL' AS charge_category,
        'ALL' AS supervision_type,
        'ALL' AS supervision_level,
      COUNT(DISTINCT state_id) AS total_revocations,
        -- This should always be equal to the total_revocations since a single state_id should never be included
        -- more than once in a given dimensional breakdown
        COUNT(*) AS total_rows
      FROM `{project_id}.{view_dataset}.revocations_matrix_filtered_caseload`
      WHERE reported_violations != 'ALL' AND violation_type NOT IN ('ALL', 'NO_VIOLATION_TYPE')
      GROUP BY state_code, metric_period_months, level_1_supervision_location, level_2_supervision_location,
               charge_category, supervision_type, supervision_level
    )
    SELECT 
      region_code, metric_period_months, level_1_supervision_location, level_2_supervision_location, charge_category,
      supervision_type, supervision_level,
      IFNULL(c.total_revocations, 0) AS cell_sum,
      IFNULL(cl.total_revocations, 0) AS caseload_sum,
      IFNULL(cl.total_rows, 0) AS caseload_num_rows
    FROM cell_counts c 
    FULL OUTER JOIN caseload_counts cl
    USING (region_code, metric_period_months, level_1_supervision_location, level_2_supervision_location,
           charge_category, supervision_type, supervision_level)
    ORDER BY region_code, metric_period_months, level_1_supervision_location, level_2_supervision_location,
             charge_category, supervision_type, supervision_level      
"""

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_NAME,
    view_query_template=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_QUERY_TEMPLATE,
    description=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_DESCRIPTION,
    view_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_BUILDER.build_and_print()
