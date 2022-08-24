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
"""Creates a view that encompasses time spent in supervision with a new session created every time any of someone's eligibility
criteria changes"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_ELIGIBILITY_SESSIONS_VIEW_NAME = (
    "us_tn_compliant_reporting_eligibility_sessions"
)

US_TN_COMPLIANT_REPORTING_ELIGIBILITY_SESSIONS_VIEW_DESCRIPTION = (
    "Creates a view that encompasses time spent in supervision with a new session "
    "created every time any of someone's compliant reporting eligibility changes. "
    "Sessions may end if the person loses eligibility, the reason for eligibility "
    "changes, the person is granted CR, or the her supervision session ends."
)

US_TN_COMPLIANT_REPORTING_ELIGIBILITY_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    -- TODO(#14425): Various updates to schema and logic of compliant reporting sessions
    WITH cte AS (
    SELECT 
        s.person_id,
        compartment_level_1_super_session_id,
        external_id AS offender_id,
        start_date AS supervision_start_date,
        end_date AS supervision_end_date, 
        supervision_date,
        last_day_of_data,
    FROM `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized` s
    JOIN `{project_id}.{base_dataset}.state_person_external_id` ex 
        ON s.person_id = ex.person_id
        AND ex.state_code = s.state_code
    ,
    UNNEST(GENERATE_DATE_ARRAY(start_date, COALESCE(end_date,last_day_of_data), INTERVAL 1 DAY)) AS supervision_date
    WHERE compartment_level_1 = 'SUPERVISION'
        AND s.state_code = 'US_TN' 
        AND supervision_date>='2018-01-01'
    ORDER BY supervision_date
    )
    ,
    cte2 AS 
    (
    SELECT 
        cte.*, 
        COALESCE(sanction.sanction_eligible, TRUE) AS sanction_eligible,
        COALESCE(sl.supervision_level_eligible, FALSE) AS supervision_level_eligible,
        COALESCE(jo.judicial_order_eligible, TRUE) AS judicial_order_eligible,
        COALESCE(fines_fees_eligible, TRUE) AS fines_fees_eligible,
        
        CASE WHEN offense.c1_eligible = 1 THEN TRUE
            WHEN offense.c1_eligible = 0 THEN FALSE END AS offense_c1_eligible,
        
        CASE WHEN offense.c2_eligible = 1 THEN TRUE
            WHEN offense.c2_eligible = 0 THEN FALSE END AS offense_c2_eligible,
            
        CASE WHEN offense.c3_eligible = 1 THEN TRUE
            WHEN offense.c3_eligible = 0 THEN FALSE END AS offense_c3_eligible,
        
        c4.c4_isc_eligibility_flag AS eligible_c4_isc,
        
        COALESCE(cr_rejection.cr_rejection_eligible, TRUE) AS cr_rejection_eligible,
        
        COALESCE(cr.on_compliant_reporting, FALSE) AS on_compliant_reporting,
        
        CASE WHEN drug_screen.drug_screen_eligible = 1 THEN TRUE
            ELSE FALSE END AS drug_screen_eligible,
    FROM cte
    LEFT JOIN `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_sanction_ineligible_materialized` sanction
        ON sanction.person_id = cte.person_id
        AND cte.supervision_date BETWEEN sanction.start_date AND COALESCE(sanction.end_date, '9999-01-01')
    LEFT JOIN `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_supervision_level_eligible_materialized` sl
        ON sl.person_id = cte.person_id
        AND cte.supervision_date BETWEEN sl.start_date AND COALESCE(sl.end_date, '9999-01-01') 
    LEFT JOIN `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_judicial_order_ineligible_materialized` jo
        ON jo.person_id = cte.person_id
        AND cte.supervision_date BETWEEN jo.start_date AND COALESCE(jo.end_date, '9999-01-01')
    LEFT JOIN `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_offense_eligible_materialized` offense
        ON offense.person_id = cte.person_id
        AND cte.supervision_date BETWEEN offense.start_date AND COALESCE(offense.end_date, '9999-01-01')
    LEFT JOIN `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_fees_eligibility_sessions_materialized` fees
        ON fees.person_id = cte.person_id
        AND cte.supervision_date BETWEEN COALESCE(fees.start_date, '1901-01-01')
            AND COALESCE(fees.end_date, '9999-01-01')
    LEFT JOIN `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_c4_isc_eligibility_sessions_materialized` c4
        ON c4.person_id = cte.person_id
        AND cte.supervision_date BETWEEN CAST(c4.start_date AS DATE) AND COALESCE(CAST(c4.end_date AS DATE), '9999-01-01')
    LEFT JOIN `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_cr_rejection_ineligible_materialized` cr_rejection
        ON cr_rejection.person_id = cte.person_id
        AND cte.supervision_date BETWEEN cr_rejection.start_date AND COALESCE(cr_rejection.end_date, '9999-01-01')
    LEFT JOIN  `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_drug_screen_eligible_materialized` drug_screen
        ON drug_screen.person_id = cte.person_id
        AND cte.supervision_date BETWEEN drug_screen.start_date AND COALESCE(drug_screen.end_date, '9999-01-01')
    LEFT JOIN 
    (
    SELECT 
        person_id,
        start_date,
        end_date,
        TRUE AS on_compliant_reporting,
    FROM `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized`
    WHERE supervision_level = 'LIMITED' 
        AND state_code = 'US_TN'
    ) cr
        ON cr.person_id = cte.person_id
        AND cte.supervision_date BETWEEN cr.start_date AND COALESCE(cr.end_date, '9999-01-01')
    )
    SELECT
        person_id,
        offender_id,
        compartment_level_1_super_session_id,
        eligibility_session_id,
        sanction_eligible,
        supervision_level_eligible,
        judicial_order_eligible,
        fines_fees_eligible,
        offense_c1_eligible,
        offense_c2_eligible,
        offense_c3_eligible,
        drug_screen_eligible,
        cr_rejection_eligible,

        sanction_eligible 
            AND supervision_level_eligible 
            AND judicial_order_eligible 
            AND fines_fees_eligible 
            AND cr_rejection_eligible
            AND drug_screen_eligible
            AND offense_c1_eligible AS eligible_c1,
        sanction_eligible 
            AND supervision_level_eligible 
            AND judicial_order_eligible 
            AND fines_fees_eligible 
            AND cr_rejection_eligible
            AND drug_screen_eligible
            AND offense_c2_eligible AS eligible_c2,
        sanction_eligible 
            AND supervision_level_eligible 
            AND judicial_order_eligible 
            AND fines_fees_eligible 
            AND cr_rejection_eligible
            AND drug_screen_eligible
            AND offense_c3_eligible AS eligible_c3,        
        
        eligible_c4_isc,
        
        on_compliant_reporting,
        MIN(supervision_date) AS start_date,
        NULLIF(MAX(supervision_date), ANY_VALUE(last_day_of_data)) AS end_date
    FROM (
        SELECT 
            *,
            SUM(IF(new_session OR date_gap,1,0)) OVER(PARTITION BY person_id ORDER BY supervision_date) AS eligibility_session_id
        FROM (
            SELECT
                *,
                COALESCE(sanction_eligible != LAG(sanction_eligible) OVER w
                    OR supervision_level_eligible != LAG(supervision_level_eligible) OVER w
                    OR judicial_order_eligible != LAG(judicial_order_eligible) OVER w
                    OR fines_fees_eligible != LAG(fines_fees_eligible) OVER w
                    OR on_compliant_reporting != LAG(on_compliant_reporting) OVER w
                    OR offense_c1_eligible != LAG(offense_c1_eligible) OVER w
                    OR offense_c2_eligible != LAG(offense_c2_eligible) OVER w
                    OR offense_c3_eligible != LAG(offense_c3_eligible) OVER w
                    OR cr_rejection_eligible != LAG(cr_rejection_eligible) OVER w
                    OR drug_screen_eligible != LAG(drug_screen_eligible) OVER w

                    OR IFNULL(CAST(eligible_c4_isc AS STRING), "NONE") != 
                        IFNULL(CAST(LAG(eligible_c4_isc) OVER (w) AS STRING), "NONE")

                        , TRUE) AS new_session,
                LAG(supervision_date) OVER w != DATE_SUB(supervision_date, INTERVAL 1 DAY) AS date_gap,
            FROM cte2
            WINDOW w AS (
                PARTITION BY person_id ORDER BY supervision_date
            )
        )
    )
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
"""

US_TN_COMPLIANT_REPORTING_ELIGIBILITY_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_ELIGIBILITY_SESSIONS_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_ELIGIBILITY_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_ELIGIBILITY_SESSIONS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_ELIGIBILITY_SESSIONS_VIEW_BUILDER.build_and_print()
