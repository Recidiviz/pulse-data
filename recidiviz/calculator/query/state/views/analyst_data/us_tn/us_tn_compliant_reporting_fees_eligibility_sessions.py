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
"""Creates a view that sessionizes fines/fees eligibility for Compliant Reporting."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
    US_TN_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_FEES_ELIGIBILITY_SESSIONS_VIEW_NAME = (
    "us_tn_compliant_reporting_fees_eligibility_sessions"
)

US_TN_COMPLIANT_REPORTING_FEES_ELIGIBILITY_SESSIONS_VIEW_DESCRIPTION = (
    "Creates a view that surfaces sessions during which clients are ineligible for "
    "Compliant Reporting due to fines and fees."
)

US_TN_COMPLIANT_REPORTING_FEES_ELIGIBILITY_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    
    # associate person_id with AccountSAK
    WITH account_info AS (
        SELECT
            pei.person_id,
            acc.AccountSAK,
        FROM
            `{project_id}.{raw_dataset}.OffenderAccounts_latest` acc
        INNER JOIN
            `{project_id}.{state_base_dataset}.state_person_external_id` pei            
        ON
            pei.external_id = acc.OffenderID
            AND id_type = "US_TN_DOC"
    )
    
    , exemptions AS (
        SELECT
            AccountSAK,
            CAST(SPLIT(StartDate,' ')[OFFSET(0)] AS DATE) AS StartDate,
            CAST(SPLIT(EndDate,' ')[OFFSET(0)] AS DATE) AS EndDate,
            FeeItemID,
            ReasonCode,
            CAST(ExemptAmount AS FLOAT64) AS ExemptAmount,
        FROM
            `{project_id}.{raw_dataset}.OffenderExemptions_latest`
    )
    
    , permanent_exemption_sessions AS (
        SELECT 
            person_id,
            perm_exemption_session_id,
            MIN(StartDate) AS perm_exemption_start_date,
            IF(
                LOGICAL_AND(EndDate IS NOT NULL),
                MAX(EndDate),
                NULL
            ) AS perm_exemption_end_date,
        FROM (
            SELECT
                * EXCEPT(new_perm_exemption_session),
                SUM(new_perm_exemption_session) OVER (PARTITION BY person_id 
                    ORDER BY StartDate) AS perm_exemption_session_id,
            FROM (
                SELECT
                    *, 
                    IF(
                        LAG(IFNULL(EndDate, "9999-01-01")) OVER(PARTITION BY person_id 
                            ORDER BY StartDate) < DATE_SUB(StartDate, INTERVAL 1 DAY),
                        1, 0
                    ) AS new_perm_exemption_session,
                FROM (
                    # get latest exemption end date per start
                    SELECT
                        person_id,
                        StartDate,
                        IF(
                            LOGICAL_AND(EndDate IS NOT NULL),
                            MAX(EndDate),
                            NULL
                        ) AS EndDate,
                    FROM exemptions 
                    INNER JOIN account_info USING(AccountSAK)
                    WHERE ReasonCode IN ("SSDB", "JORD", "CORD", "SISS")
                    GROUP BY 1, 2
                )
            )
        )
        GROUP BY 1, 2
    )
    
    , inv AS (
        SELECT
            AccountSAK,
            FeeItemID,
            CAST(SPLIT(InvoiceDate,' ')[OFFSET(0)] AS DATE) as InvoiceDate,
            CAST(InvoiceAmount AS FLOAT64) AS InvoiceAmount
        FROM
            `{project_id}.{raw_dataset}.OffenderInvoices_latest`
    )
    
    , inv_with_exemptions AS (
        # sum over days in case multiple invoices on same day
        SELECT
            person_id,
            InvoiceDate AS activity_date,
            SUM(InvoiceAmount) AS amount,
        FROM (
            # remove exempt amount from invoices
            SELECT 
                account_info.person_id,
                inv.InvoiceDate,
                IF(
                    IFNULL(InvoiceAmount, 0) >= IFNULL(ExemptAmount, 0), 
                    IFNULL(InvoiceAmount, 0) - IFNULL(ExemptAmount, 0),
                    IFNULL(InvoiceAmount, 0)
                ) AS InvoiceAmount,
            FROM
                account_info
            LEFT JOIN
                inv
            USING
                (AccountSAK)
            LEFT JOIN
                exemptions
            ON
                inv.AccountSAK = exemptions.AccountSAK
                AND inv.FeeItemID = exemptions.FeeItemID
                AND inv.InvoiceDate BETWEEN exemptions.StartDate 
                    AND IFNULL(exemptions.EndDate, "9999-01-01")
        )
        GROUP BY 1, 2
    )
    , payments AS (
        # sum over days in case multiple payments on same day
        SELECT
            person_id,
            PaymentDate AS activity_date,
            SUM(PaidAmount) * -1 AS amount,
        FROM (
            SELECT
                person_id,
                CAST(SPLIT(PaymentDate,' ')[OFFSET(0)] AS DATE) AS PaymentDate,
                CAST(PaidAmount AS FLOAT64) - CAST(UnAppliedAmount AS FLOAT64) AS PaidAmount,
            FROM
                `{project_id}.{raw_dataset}.OffenderPayments_latest` p
            INNER JOIN
                account_info           
            USING
                (AccountSAK)
        )
        GROUP BY 1, 2
    )
    -- Calculate for each person and activity month whether a payment was made, which is used to see if a payment was made
    -- for 3 consecutive months
    , consecutive_monthly_payments AS (
        SELECT person_id,
                activity_month,
                SUM(CASE WHEN lag_month = DATE_SUB(activity_month, INTERVAL 1 MONTH) THEN 1 ELSE 0 END) OVER(PARTITION BY person_id ORDER BY activity_month ROWS BETWEEN 1 PRECEDING and CURRENT ROW  ) as consecutive_months
        FROM (
            SELECT *, LAG(activity_month) OVER(PARTITION BY person_id ORDER BY activity_month) as lag_month,
            FROM (
            SELECT person_id,  
                DATE_TRUNC(activity_date, MONTH) as activity_month,
                SUM(amount) as amount,
            
            FROM payments
            GROUP BY 1,2
            )            
        )   
    )
    , inv_and_payments AS (
        SELECT
            unioned.person_id, 
            activity_date, 
            ss.compartment_level_0_super_session_id, 
            perm_exemption_session_id, 
            SUM(amount) AS amount,
        FROM (
            SELECT * FROM inv_with_exemptions 
            UNION ALL
            SELECT * FROM payments
        ) unioned
        
        # add system session ID, because assuming fees drop across system sessions
        LEFT JOIN
            `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized` ss
        ON
            unioned.person_id = ss.person_id
            AND activity_date BETWEEN ss.start_date AND IFNULL(ss.end_date, "9999-01-01")
        
        # add perm exemption session ID - any non-null ID implies the person is exempt
        # from paying any fees (e.g. because disability)
        LEFT JOIN 
            permanent_exemption_sessions 
        ON
            unioned.person_id = permanent_exemption_sessions.person_id
            AND activity_date BETWEEN perm_exemption_start_date 
                AND IFNULL(perm_exemption_end_date, "9999-01-01")
        GROUP BY 1, 2, 3, 4
    )
        
    # use fee balance to classify eligible or not on activity date
    , eligible_on_date AS (
        SELECT
            inv_and_payments.person_id, 
            activity_date, 
            -- Edge case where people are eligible for making 3 consecutive payments, but become ineligible early in the next
            -- month when they haven't yet made a payment. We still want to consider these folks eligible
            CASE WHEN DATE_TRUNC(activity_date, MONTH) = DATE_TRUNC(CURRENT_DATE('US/Eastern'),MONTH) 
                AND SUM(amount) OVER(PARTITION BY inv_and_payments.person_id, compartment_level_0_super_session_id ORDER BY activity_date) <= 2000
                AND COALESCE(LAG(consecutive_months) OVER(PARTITION BY inv_and_payments.person_id ORDER BY activity_date)) = 2
                THEN true 
            ELSE    
                SUM(amount) OVER(PARTITION BY inv_and_payments.person_id, compartment_level_0_super_session_id ORDER BY activity_date) <= 500 
                OR perm_exemption_session_id IS NOT NULL 
                OR (SUM(amount) OVER(PARTITION BY inv_and_payments.person_id, compartment_level_0_super_session_id ORDER BY activity_date) <= 2000 
                    AND  COALESCE(consecutive_months,0) = 2
                )
            END
                AS eligible,
        FROM
            inv_and_payments 
        LEFT JOIN
            consecutive_monthly_payments
        ON 
            inv_and_payments.person_id = consecutive_monthly_payments.person_id
            AND DATE_TRUNC(inv_and_payments.activity_date, MONTH) = consecutive_monthly_payments.activity_month
    )
    
    # sessionize fee eligibility
    SELECT 
        person_id,
        "US_TN" AS state_code,
        eligibility_session_id,
        eligible AS fines_fees_eligible,
        MIN(activity_date) AS start_date,
        IF(
            LOGICAL_AND(end_date IS NOT NULL),
            MAX(end_date),
            NULL
        ) AS end_date,
    FROM (
        SELECT
            *,
            SUM(eligible_changed) OVER(
                PARTITION BY person_id ORDER BY activity_date
            ) AS eligibility_session_id,
        FROM (
            SELECT
                *, 
                IFNULL(IF(
                    LAG(eligible) OVER(PARTITION BY person_id ORDER BY activity_date) = eligible,
                    0, 1
                ), 1) AS eligible_changed,
                DATE_SUB(LEAD(activity_date) OVER(PARTITION BY person_id 
                    ORDER BY activity_date), INTERVAL 1 DAY) AS end_date,
            FROM
                eligible_on_date
        )
    )
    GROUP BY 1,2,3,4
"""

US_TN_COMPLIANT_REPORTING_FEES_ELIGIBILITY_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_FEES_ELIGIBILITY_SESSIONS_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_FEES_ELIGIBILITY_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_FEES_ELIGIBILITY_SESSIONS_QUERY_TEMPLATE,
    state_base_dataset=STATE_BASE_DATASET,
    raw_dataset=US_TN_RAW_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_FEES_ELIGIBILITY_SESSIONS_VIEW_BUILDER.build_and_print()
