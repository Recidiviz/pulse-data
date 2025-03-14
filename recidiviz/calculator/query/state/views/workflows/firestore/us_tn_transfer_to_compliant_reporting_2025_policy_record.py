# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query for relevant metadata needed to support compliant reporting opportunity in Tennessee, 2025 policy
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.shared_ctes import (
    keep_contact_codes,
    us_tn_compliant_reporting_shared_opp_record_fragment,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.collapsed_task_eligibility_spans import (
    build_collapsed_tes_spans_view_materialized_address,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.assessed_not_high_on_strong_r_domains import (
    STRONG_R_ASSESSMENT_METADATA_KEYS,
)
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.eligibility_spans.us_tn.transfer_low_medium_group_to_compliant_reporting_2025_policy import (
    VIEW_BUILDER as US_TN_LOW_MEDIUM_COMPLIANT_REPORTING_2025_TES_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_COLLAPSED_TES_SPANS_ADDRESS = build_collapsed_tes_spans_view_materialized_address(
    US_TN_LOW_MEDIUM_COMPLIANT_REPORTING_2025_TES_VIEW_BUILDER
)
US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_NAME = (
    "us_tn_transfer_to_compliant_reporting_2025_policy_record"
)

# TODO(#38984): Update with new form when available
# TODO(#38953): Include the Low-Intake group when TN rolls launches that part of new policy
# TODO(#39428): Move collapsed logic inner join to generalized function
US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_QUERY_TEMPLATE = f"""
    WITH base AS (
        SELECT
            "LOW-MODERATE" AS metadata_task_name,
            tes.external_id,
            tes.person_id,
            tes.state_code,
            tes.reasons,
            tes.ineligible_criteria,
            tes.is_eligible,
            tes.is_almost_eligible,
            tes_collapsed.start_date AS metadata_eligible_date,
        FROM (
        {join_current_task_eligibility_spans_with_external_id(
            state_code= "'US_TN'", 
            tes_task_query_view = 'transfer_low_medium_group_to_compliant_reporting_2025_policy_materialized',
            id_type = "'US_TN_DOC'",
            eligible_and_almost_eligible_only=True,
        )}) tes
        INNER JOIN `{{project_id}}.{_COLLAPSED_TES_SPANS_ADDRESS.to_str()}` tes_collapsed
            ON tes_collapsed.state_code = tes.state_code
            AND tes_collapsed.person_id = tes.person_id 
            AND CURRENT_DATE('US/Pacific') BETWEEN tes_collapsed.start_date AND {nonnull_end_date_exclusive_clause('tes_collapsed.end_date')}
    ), 
    relevant_contact_codes AS (
        SELECT
            contact.person_id,
            contact_type,
            contact_date,
        FROM `{{project_id}}.analyst_data.us_tn_relevant_contact_codes_materialized` contact
        LEFT JOIN `{{project_id}}.sessions.prioritized_supervision_sessions_materialized` pss
            ON contact.person_id = pss.person_id
            AND contact.contact_date >= pss.start_date
        WHERE CURRENT_DATE('US/Eastern') BETWEEN pss.start_date 
            AND {nonnull_end_date_exclusive_clause('pss.end_date_exclusive')}
    ),
    comments_clean AS (
        SELECT
            person_id,
            contact_date,
            contact_comment
        FROM `{{project_id}}.analyst_data.us_tn_contact_comments_preprocessed_materialized`
    ),
    latest_assessment AS (
    -- Metadata to show all domains of latest StrongR where someone has at least 1 domain that's High
        SELECT
            person_id,
            -- Formatting for front end
            INITCAP(REPLACE(need,'_',' ')) AS need,
            CAST(JSON_EXTRACT_SCALAR(single_reason.reason,'$.assessment_date') AS DATE) AS metadata_latest_strong_r_date,
            /* UNNEST returns one row per key in a column called "need"
              PARSE_JSON(assessment_metadata)[need] extracts the JSON value associated with the key
              from the need column, and JSON_VALUE formats this as a string
             */
            NULLIF(JSON_VALUE(PARSE_JSON(JSON_EXTRACT_SCALAR(single_reason.reason,'$.assessment_metadata'))[need]),'') AS need_level       
        FROM base,
        UNNEST(JSON_QUERY_ARRAY(reasons)) AS single_reason,
        UNNEST({STRONG_R_ASSESSMENT_METADATA_KEYS}) AS need    
        WHERE 'US_TN_ASSESSED_NOT_HIGH_ON_STRONG_R_DOMAINS' IN UNNEST(ineligible_criteria)
            AND STRING(single_reason.criteria_name) = 'US_TN_ASSESSED_NOT_HIGH_ON_STRONG_R_DOMAINS'
    ),
    compliant_reporting_form_query_fragment AS (
        SELECT *
        FROM ({us_tn_compliant_reporting_shared_opp_record_fragment()})
    ),
    case_notes_array AS (
        SELECT person_id,
                TO_JSON(
                    ARRAY_AGG(
                        IF(note_body IS NOT NULL,
                            STRUCT(note_title, note_body, event_date, criteria),
                            NULL
                        )
                    IGNORE NULLS
                    ORDER BY criteria, event_date
                    )
                ) AS case_notes    
        FROM (
            SELECT person_id,
                  NULL AS note_title,
                  NULL AS event_date,
                  offense AS note_body,
                  "CURRENT OFFENSES" AS criteria,
            FROM compliant_reporting_form_query_fragment,
            UNNEST(form_information_current_offenses) AS offense
            
            UNION ALL
            
            SELECT person_id,
                  need AS note_title,
                  metadata_latest_strong_r_date AS event_date,
                  need_level AS note_body,
                  "LATEST STRONG-R DOMAINS" AS criteria,
            FROM latest_assessment

            UNION ALL

            -- We don't have data on pending felony charges but recent VWAR (violation warrant) codes can be an indicator
            -- that there might be a pending charge
            SELECT person_id,
                  contact_type AS note_title,
                  contact_date AS event_date,
                  contact_comment AS note_body,
                  "RELEVANT CONTACT CODES - VIOLATION WARRANTS" AS criteria,
            FROM ({keep_contact_codes(
                    codes_cte="relevant_contact_codes",
                    comments_cte="comments_clean",
                    where_clause_codes_cte="WHERE contact_type IN ('VWAR')",
                    keep_last=False
                )})
        )
        GROUP BY 1
    )
    SELECT
        c.metadata_task_name,
        c.external_id,
        c.state_code,
        reasons,
        ineligible_criteria,
        -- used to create almost eligible categories
        CONCAT('MISSING_', CAST(ARRAY_LENGTH(ineligible_criteria) AS STRING), '_CRITERIA') AS metadata_tab_name,
        is_eligible,
        is_almost_eligible,
        -- case notes
        a.case_notes,
        -- metadata
        metadata_eligible_date,
        metadata_latest_negative_arrest_check,
        metadata_latest_spe_note,
        -- metadata also shared by form
        metadata_conviction_counties,
        metadata_all_offenses,
        metadata_ineligible_offenses_expired,
        -- form information
        form_information_drivers_license,
        form_information_drivers_license_suspended,
        form_information_drivers_license_revoked,
        form_information_restitution_amt,
        form_information_restitution_monthly_payment,
        form_information_restitution_monthly_payment_to,
        form_information_court_costs_paid, 
        form_information_supervision_fee_assessed,
        form_information_supervision_fee_arrearaged,
        form_information_supervision_fee_arrearaged_amount,
        form_information_current_exemptions_and_expiration,
        form_information_supervision_fee_waived,
        form_information_docket_numbers,
        form_information_current_offenses,        
        form_information_judicial_district,
        form_information_sentence_start_date,
        form_information_expiration_date,
        form_information_sentence_length_days,
    FROM compliant_reporting_form_query_fragment c
    LEFT JOIN case_notes_array a
        USING(person_id)
"""

US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_NAME,
    view_query_template=US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_QUERY_TEMPLATE,
    description=__doc__,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_TN
    ),
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.build_and_print()
