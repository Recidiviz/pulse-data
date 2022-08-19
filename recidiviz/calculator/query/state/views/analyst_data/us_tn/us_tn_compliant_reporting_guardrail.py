# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Guardrail metric view for the Compliant Reporting Workflow"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_NAME = "us_tn_compliant_reporting_guardrail"

US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_DESCRIPTION = """View that supports guardrail metric
    for CR Workflow by pulling in daily drug screen information for people that are eligible, 
    ineligible, or almost eligible for compliant reporting"""

US_TN_COMPLIANT_REPORTING_GUARDRAIL_QUERY_TEMPLATE = """
    SELECT
        date_of_supervision,
        person_id,
        person_external_id,
        district,
        compliant_reporting_eligible,
        remaining_criteria_needed,
        officer_id,
        is_positive_result,
        substance_detected,
    FROM `{project_id}.{workflows_views_dataset}.client_record_archive_materialized`
    LEFT JOIN (
        -- view is unique on person-date-sample_type, but sample_type is not implemented for TN
        -- as of this writing so just deduping to future-proof
        SELECT 
            person_id,
            -- renaming this field for easier join
            drug_screen_date AS date_of_supervision,
            -- prioritize positive results, then deterministic order by sample type
            ARRAY_AGG(
                is_positive_result 
                ORDER BY is_positive_result DESC, substance_detected DESC, sample_type
            )[OFFSET(0)] AS is_positive_result,
            ARRAY_AGG(
                substance_detected 
                ORDER BY is_positive_result DESC, substance_detected DESC, sample_type
            )[OFFSET(0)] AS substance_detected,
        FROM `{project_id}.{sessions_dataset}.drug_screens_preprocessed_materialized`
        GROUP BY 1, 2
    ) USING (person_id, date_of_supervision)
    WHERE state_code = "US_TN"
"""

US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_GUARDRAIL_QUERY_TEMPLATE,
    should_materialize=True,
    sessions_dataset=SESSIONS_DATASET,
    workflows_views_dataset=WORKFLOWS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_BUILDER.build_and_print()
