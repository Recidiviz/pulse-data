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

"""A view which ensures the admission_history_description column of the revocations_matrix_filtered_caseload matches
the expected number of admissions."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_VIEW_NAME = (
    "revocation_matrix_caseload_admission_history"
)

REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_DESCRIPTION = """
Validate admission history descriptions for the revocation matrix caseload dashboard view."""

REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_QUERY_TEMPLATE = """
    /*{description}*/
    WITH admission_counts AS (
        SELECT state_code, person_id, COUNT(*) AS total_admissions
        FROM `{project_id}.{reference_dataset}.event_based_commitments_from_supervision_for_matrix_materialized`
        WHERE admission_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 35 MONTH)
        GROUP BY state_code, person_id
    ),
    caseload_counts AS (
        SELECT state_code, internal_person_id AS person_id,
        CASE
            -- US_MO displays a total number of admissions instead of a list of admission types
            WHEN state_code = 'US_MO'
            THEN CAST(admission_history_description AS INT64)
        ELSE ARRAY_LENGTH(SPLIT(admission_history_description, ';'))
        END AS total_admissions
        FROM `{project_id}.{dashboard_dataset}.revocations_matrix_filtered_caseload`
        WHERE metric_period_months = 36
    )
    SELECT
        state_code AS region_code,
        a.total_admissions AS total_revocation_admissions, 
        c.total_admissions AS total_caseload_admissions
    FROM admission_counts a
    JOIN caseload_counts c
    USING(state_code, person_id)
    
"""

REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_VIEW_NAME,
    view_query_template=REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_QUERY_TEMPLATE,
    description=REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_DESCRIPTION,
    dashboard_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
    reference_dataset=state_dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_VIEW_BUILDER.build_and_print()
