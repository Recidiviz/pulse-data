# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
Queries information needed to surface eligible folks to be early released on a Transition Program Release AZ.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    join_current_task_eligibility_spans_with_external_id,
    opportunity_query_final_select_with_case_notes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_az_query_fragments import (
    home_plan_information_for_side_panel_notes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_NAME = (
    "us_az_approaching_acis_or_recidiviz_tpr_request_record"
)

US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_DESCRIPTION = """
    Queries information needed to surface eligible folks to be early released on a Transition Program Release AZ.
    """


US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_QUERY_TEMPLATE = f"""
# TODO(#33958) - Add all the relevant components of this opportunity record
WITH recidiviz_tpr_date_approaching AS (
{join_current_task_eligibility_spans_with_external_id(
    state_code="'US_AZ'",
    tes_task_query_view='overdue_for_recidiviz_tpr_request_materialized',
    id_type="'US_AZ_ADC_NUMBER'",
    eligible_and_almost_eligible_only=True)}
),

acis_tpr_date_approaching AS (
{join_current_task_eligibility_spans_with_external_id(
    state_code="'US_AZ'",
    tes_task_query_view='overdue_for_acis_tpr_request_materialized',
    id_type="'US_AZ_ADC_NUMBER'",
    almost_eligible_only=True)}
),

combine_acis_and_recidiviz_tpr_dates AS (
    -- Fast track: ACIS TPR date within 1 days and 30 days
    SELECT
        * EXCEPT(criteria_reason),
        CASE
            WHEN SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.acis_tpr_date') AS DATE)
                    < DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 30 DAY)
                THEN "FAST_TRACK"
            ELSE "APPROVED_BY_TIME_COMP"
        END AS metadata_tab_description,
    CASE
            WHEN SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.acis_tpr_date') AS DATE)
                    < DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 30 DAY)
                THEN "FAST_TRACK"
            ELSE "APPROVED_BY_TIME_COMP"
        END AS metadata_tab_name,
    FROM acis_tpr_date_approaching,
    UNNEST(JSON_QUERY_ARRAY(reasons)) AS criteria_reason
    WHERE "US_AZ_INCARCERATION_PAST_ACIS_TPR_DATE" IN UNNEST(ineligible_criteria)
        AND SAFE_CAST(JSON_VALUE(criteria_reason, '$.criteria_name') AS STRING) = "US_AZ_INCARCERATION_PAST_ACIS_TPR_DATE"
        AND SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.acis_tpr_date') AS DATE) BETWEEN 
            DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 1 DAY) AND DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 30 DAY)

    UNION ALL

    -- Almost eligible section 1: Projected TPR date within 7-180 days
    -- Almost eligible section 1: Projected TPR date within 7-180 days AND missing man lit
    -- Almost eligible section 2: Projected TPR date within 181-365 days AND missing at most one other criteria
    -- (functional literacy XOR no felony detainers)
    # TODO(#33958) - recidiviz_tpr_date_approaching needs to be split into section 1 and 2
    SELECT 
        * EXCEPT(criteria_reason),
        CASE
            WHEN is_eligible THEN "ALMOST_ELIGIBLE_BETWEEN_7_AND_180_DAYS"
            WHEN ARRAY_LENGTH(ineligible_criteria) = 1
                THEN CASE
                    WHEN "US_AZ_MEETS_FUNCTIONAL_LITERACY" IN UNNEST(ineligible_criteria)
                        THEN "ALMOST_ELIGIBLE_MISSING_MANLIT_BETWEEN_7_AND_180_DAYS"
                    WHEN "US_AZ_WITHIN_6_MONTHS_OF_RECIDIVIZ_TPR_DATE" IN UNNEST(ineligible_criteria)
                        THEN "ALMOST_ELIGIBLE_BETWEEN_181_AND_365_DAYS"
                    END
            ELSE "ALMOST_ELIGIBLE_MISSING_CRITERIA_AND_BETWEEN_181_AND_365_DAYS"
        END AS metadata_tab_description,
        CASE
            WHEN is_eligible THEN "ALMOST_ELIGIBLE_1"
            WHEN ARRAY_LENGTH(ineligible_criteria) = 1
                THEN CASE
                    WHEN "US_AZ_MEETS_FUNCTIONAL_LITERACY" IN UNNEST(ineligible_criteria)
                        THEN "ALMOST_ELIGIBLE_1"
                    WHEN "US_AZ_WITHIN_6_MONTHS_OF_RECIDIVIZ_TPR_DATE" IN UNNEST(ineligible_criteria)
                        THEN "ALMOST_ELIGIBLE_2"
                    END
            ELSE "ALMOST_ELIGIBLE_2"
        END AS metadata_tab_name,
    FROM recidiviz_tpr_date_approaching,
    UNNEST(JSON_QUERY_ARRAY(reasons)) AS criteria_reason
    WHERE
        SAFE_CAST(JSON_VALUE(criteria_reason, '$.criteria_name') AS STRING) = "US_AZ_WITHIN_6_MONTHS_OF_RECIDIVIZ_TPR_DATE"
        AND (
            is_almost_eligible
            OR (
                -- Only include residents with more than 7 days until the projected TPR date if all other criteria are met
                is_eligible
                AND DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 7 DAY)
                    < SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.recidiviz_tpr_date') AS DATE)
            )
        )
),

side_panel_notes AS (
    {home_plan_information_for_side_panel_notes()}
),


array_side_panel_notes_cte AS (
    {array_agg_case_notes_by_external_id(
        left_join_cte="side_panel_notes", 
        from_cte="combine_acis_and_recidiviz_tpr_dates")}
)

{opportunity_query_final_select_with_case_notes(
    from_cte = "combine_acis_and_recidiviz_tpr_dates",
    left_join_cte="array_side_panel_notes_cte",
    additional_columns="metadata_tab_name, metadata_tab_description",)}
"""

US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_NAME,
    view_query_template=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_QUERY_TEMPLATE,
    description=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_AZ
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
