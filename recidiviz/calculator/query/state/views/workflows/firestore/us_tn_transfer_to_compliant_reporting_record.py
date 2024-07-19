# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Query for relevant metadata needed to support compliant reporting opportunity in Tennessee
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.shared_ctes import (
    us_tn_fines_fees_info,
    us_tn_get_offense_information,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_NAME = (
    "us_tn_transfer_to_compliant_reporting_record"
)

US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support compliant reporting opportunity in Tennessee
    """
US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_QUERY_TEMPLATE = f"""
    WITH eligible_with_discretion AS (
    -- Keep only the most current span per probation client
    {join_current_task_eligibility_spans_with_external_id(
        state_code= "'US_TN'", 
        tes_task_query_view = 'transfer_to_compliant_reporting_with_discretion_materialized',
        id_type = "'US_TN_DOC'",
        eligible_and_almost_eligible_only=True,
    )}
    ),
    eligible_discretion_and_almost AS (
        -- Eligible with discretion
        SELECT
            external_id,
            person_id,
            state_code,
            reasons,
            ineligible_criteria,
            is_eligible
        FROM
            eligible_with_discretion
        
        UNION ALL
        
        -- Eligible without discretion
        SELECT
            external_id,
            person_id,
            state_code,
            reasons,
            ineligible_criteria,
            is_eligible
        FROM
            ({join_current_task_eligibility_spans_with_external_id(
                state_code= "'US_TN'", 
                tes_task_query_view = 'transfer_to_compliant_reporting_no_discretion_materialized',
                id_type = "'US_TN_DOC'",
                eligible_only=True,
            )})
    ),
    /*
    We have two views, transfer_to_compliant_reporting_no_discretion and transfer_to_compliant_reporting_with_discretion to help identify people
    who might be eligible with discretion. This is done so that the people who are eligible with discretion AND require one other
    criteria (e.g. fines/fees) can be easily identified above as have 1 remaining criteria. The previous CTE therefore helps identify the 
    4 following groups:
    1. eligible without discretion or additional criteria
    2. eligible without discretion but requiring additional criteria
    3. eligible with discretion without additional criteria
    4. eligible with discretion and additional criteria
    
    Once we have those groups, we ultimately want a reasons blob that contains the relevant information for *all* required and
    discretionary criteria. To get this, we join back onto `transfer_to_compliant_reporting_no_discretion` which contains all this
    information. The reasons blob in transfer_to_compliant_reporting_with_discretion does not contain this since it is created
    without including discretionary criteria in order to identify almost-eligible folks.    
    */
    eligible_discretion_and_almost_reasons AS (
        SELECT
            a.external_id,
            a.person_id,
            a.state_code,
            a.ineligible_criteria,
            b.reasons,
        FROM eligible_discretion_and_almost a
        LEFT JOIN ({join_current_task_eligibility_spans_with_external_id(
                state_code= "'US_TN'", 
                tes_task_query_view = 'transfer_to_compliant_reporting_no_discretion_materialized',
                id_type = "'US_TN_DOC'"
            )}) b
        USING(person_id)   
    ),   
    contacts AS (
      SELECT
        external_id,
        contact_date,
        contact_type,
        STRUCT(contact_date,contact_type) AS contact,
        contact_type_category
      FROM (
          SELECT OffenderID AS external_id, 
                  CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date,
                  ContactNoteType AS contact_type,
                  CASE 
                      WHEN ContactNoteType IN ("ARRP","ARRN","XARR") THEN "arrest_check"
                      WHEN ContactNoteType IN ("SPET","SPEC","XSPE") THEN "spe_note"
                      WHEN ContactNoteType IN ("CCFT","CCFM") THEN "court_costs_note"
                  END AS contact_type_category
          FROM `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.ContactNoteType_latest`
          WHERE ContactNoteType IN ("ARRP", "ARRN", "XARR", "SPET","SPEC","XSPE","CCFT","CCFM")
      )
      QUALIFY ROW_NUMBER() OVER(PARTITION BY external_id,
                                              contact_type_category
                                ORDER BY contact_date DESC,
                                    CASE WHEN contact_type = 'SPET' THEN 0
                                         WHEN contact_type = 'SPEC' THEN 1
                                         ELSE 2
                                         END) = 1
    ),
    pivoted_contacts AS (
        SELECT
            external_id,
            contact_type_arrest_check,
            contact_type_spe_note,
            contact_type_court_costs_note,
        FROM (SELECT external_id, contact, contact_type_category FROM contacts)
        PIVOT(ANY_VALUE(contact) AS contact_type FOR contact_type_category IN ("arrest_check","spe_note","court_costs_note"))
    ),
    restitution AS (
        SELECT 
            person_id,
            SUM(COALESCE(CAST(vic.TotalRestitution AS FLOAT64),0)) AS restitution_amt,
            SUM(COALESCE(CAST(vic.MonthlyRestitution AS FLOAT64 ),0)) AS restitution_monthly_payment,
            ARRAY_AGG(DISTINCT vic.VictimName IGNORE NULLS ORDER BY vic.VictimName) AS restitution_monthly_payment_to,
        FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` pp
        LEFT JOIN
            `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.JOVictim_latest` vic
        ON pp.external_id = CONCAT(vic.OffenderID, '-', vic.ConvictionCounty,'-', vic.CaseYear, '-', vic.CaseNumber, '-', vic.CountNumber,  '-', 'SENTENCE')
        WHERE
            state_code = "US_TN"
            AND pp.status_raw_text NOT IN ('IN')
        GROUP BY 1
    ),
    {us_tn_fines_fees_info()}
    current_offense_info AS (
        SELECT 
            off.*,
            DATE_DIFF(expiration_date,sentence_start_date,DAY) AS sentence_length_days, 
        FROM ({us_tn_get_offense_information(in_projected_completion_array=True)}) off
    ),
    all_offenses_cte AS (
        SELECT 
            person_id,
            ARRAY_AGG(offense IGNORE NULLS ORDER BY offense) AS all_offenses,
        FROM (
            SELECT 
                person_id,
                description AS offense,
            FROM `{{project_id}}.{{analyst_dataset}}.us_tn_prior_record_preprocessed_materialized`
        
            UNION DISTINCT
            
            SELECT 
                person_id,
                description AS offense
            FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
            WHERE state_code = 'US_TN' 
        )
        GROUP BY 1
    ),
    permanent_exemptions AS (
        SELECT
            person_id,
            permanent_exemption_reasons
        FROM `{{project_id}}.{{analyst_dataset}}.permanent_exemptions_preprocessed_materialized`
        WHERE state_code = 'US_TN'
            AND CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
    ),
    all_exemptions AS (
        SELECT
            person_id,
            TO_JSON(
                ARRAY_AGG(
                STRUCT(
                    ReasonCode AS exemption_reason,
                    end_date AS exemption_end_date
                )
                ORDER BY start_date
            )) AS current_exemptions,
        FROM `{{project_id}}.{{analyst_dataset}}.us_tn_exemptions_preprocessed`
        WHERE state_code = 'US_TN'
            AND CURRENT_DATE('US/Pacific') BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date')}
        GROUP BY 1
    )
    SELECT
        base.external_id,
        base.state_code,
        base.reasons,
        base.ineligible_criteria,
        pc.contact_type_arrest_check AS metadata_most_recent_arrest_check,
        pc.contact_type_spe_note AS metadata_most_recent_spe_note,
        att.DriverLicenseNumber AS form_information_drivers_license,
        null AS form_information_drivers_license_suspended,
        null AS form_information_drivers_license_revoked,
        current_offense_info.conviction_counties AS metadata_conviction_counties,
        all_offenses_cte.all_offenses AS metadata_all_offenses,
        expired_offenses.ineligible_offenses_expired As metadata_ineligible_offenses_expired,
        restitution.restitution_amt AS form_information_restitution_amt,
        restitution.restitution_monthly_payment AS form_information_restitution_monthly_payment,
        restitution.restitution_monthly_payment_to AS form_information_restitution_monthly_payment_to,
        CASE WHEN pc.contact_type_court_costs_note.contact_type = "CCFT" THEN TRUE
             WHEN pc.contact_type_court_costs_note.contact_type = "CCFM" THEN FALSE
             END AS form_information_court_costs_paid, 
        latest_balance.current_balance AS form_information_supervision_fee_assessed,
        latest_balance.current_balance > 0 AS form_information_supervision_fee_arrearaged,
        latest_balance.current_balance AS form_information_supervision_fee_arrearaged_amount,
        all_exemptions.current_exemptions AS form_information_current_exemptions_and_expiration,
        permanent_exemptions.permanent_exemption_reasons IS NOT NULL AS form_information_supervision_fee_waived,
        current_offense_info.docket_numbers AS form_information_docket_numbers,
        current_offense_info.current_offenses AS form_information_current_offenses,        
        current_offense_info.judicial_district AS form_information_judicial_district,
        current_offense_info.sentence_start_date AS form_information_sentence_start_date,
        current_offense_info.expiration_date AS form_information_expiration_date,
        current_offense_info.sentence_length_days AS form_information_sentence_length_days,
    FROM eligible_discretion_and_almost_reasons base
    LEFT JOIN 
        `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.OffenderAttributes_latest` att
    ON
        base.external_id = att.OffenderID
    LEFT JOIN
        pivoted_contacts pc
    ON
        base.external_id = pc.external_id
    LEFT JOIN
        restitution
    ON
        base.person_id = restitution.person_id
    LEFT JOIN
        fines_fees_balance_info latest_balance
    ON
        base.person_id = latest_balance.person_id
    LEFT JOIN
        all_exemptions
    ON
        base.person_id = all_exemptions.person_id
    LEFT JOIN permanent_exemptions
    ON
        base.person_id = permanent_exemptions.person_id
    LEFT JOIN current_offense_info
    ON
        base.person_id = current_offense_info.person_id
    LEFT JOIN all_offenses_cte
    ON
        base.person_id = all_offenses_cte.person_id
    -- TODO(#22357): Remove this workaround when TES infra supports hydrating spans with previous spans' reasons 
    LEFT JOIN 
        ( 
            SELECT
                person_id,
                ARRAY_AGG(
                    DISTINCT JSON_EXTRACT_SCALAR(ineligible_offenses)
                    ORDER BY JSON_EXTRACT_SCALAR(ineligible_offenses)
                ) AS ineligible_offenses_expired
            FROM `{{project_id}}.task_eligibility_criteria_us_tn.ineligible_offenses_expired_materialized`,
            UNNEST(JSON_EXTRACT_ARRAY(reason,"$.ineligible_offenses")) AS ineligible_offenses
            GROUP BY 1
        ) expired_offenses
    ON
        base.person_id = expired_offenses.person_id

"""

US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_NAME,
    view_query_template=US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_QUERY_TEMPLATE,
    description=US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_TN
    ),
    should_materialize=True,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.build_and_print()
