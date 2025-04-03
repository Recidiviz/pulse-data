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
"""Sessions view for compliance with special conditions in Nebraska"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_NE_SPECIAL_CONDITION_COMPLIANCE_SESSIONS_VIEW_NAME = (
    "us_ne_special_condition_compliance_sessions"
)

US_NE_SPECIAL_CONDITION_COMPLIANCE_SESSIONS_QUERY_TEMPLATE = f"""
    -- Deduplicate case plans to ensure we only have one case plan per day
    WITH case_plans_deduped AS (
        SELECT
            pkPIMSCasePlanId AS case_plan_id,
        FROM `{{project_id}}.{{raw_dataset}}.PIMSCasePlan_latest`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY casePlanDate, inmateNumber
            -- Take the most recent case plan entry for each day
            ORDER BY pkPIMSCasePlanId DESC
        ) = 1
    )
    ,
    -- Gather all case plan entries for special conditions
    -- A new case plan is entered each time a PO checks compliance on a client's special 
    -- conditions
    special_condition_updates AS (
        SELECT
            pei.state_code,
            pei.person_id,
            pei.external_id,
            cpsc.fkPIMSCasePlanId AS case_plan_id,
            DATE(cp.casePlanDate) AS case_plan_date,
            cpsc.fkSpecialConditionId AS special_condition_id,
            c1.codeValue AS intervention_type,
            c2.codeValue AS compliance,
            c3.codeValue AS special_condition_type,
            c4.codeValue AS reason_for_special_condition,
            DATE(sc.beginDate) AS condition_start_date,
            DATE(sc.endDate) AS condition_end_date,
            DATE(sc.suspendedDate) AS condition_suspended_date,
        FROM `{{project_id}}.{{raw_dataset}}.PIMSCasePlanSpecialCondition_latest` cpsc
        LEFT JOIN `{{project_id}}.{{raw_dataset}}.PIMSCasePlan_latest` cp
            ON cpsc.fkPIMSCasePlanId = cp.pkPIMSCasePlanId
        LEFT JOIN `{{project_id}}.{{raw_dataset}}.PIMSSpecialCondition_latest` sc
            ON cpsc.fkSpecialConditionId = sc.specialConditionId
        LEFT JOIN `{{project_id}}.{{raw_dataset}}.CodeValue_latest` c1
            ON cp.interventionTypeCode = c1.codeId
        LEFT JOIN `{{project_id}}.{{raw_dataset}}.CodeValue_latest` c2
            ON cpsc.complianceCode = c2.codeId
        LEFT JOIN `{{project_id}}.{{raw_dataset}}.CodeValue_latest` c3
            ON specialConditionType = c3.codeId
        LEFT JOIN `{{project_id}}.{{raw_dataset}}.CodeValue_latest` c4
            ON reasonForSpecialCondition = c4.codeId 
        LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
            ON
                cp.inmateNumber = pei.external_id
                AND pei.state_code = 'US_NE'
        -- Filter out any case plans that are not in the deduplicated list
        WHERE cpsc.fkPIMSCasePlanId IN (
            SELECT case_plan_id FROM case_plans_deduped
        )
    )
    SELECT
        state_code,
        person_id,
        special_condition_id AS special_condition_external_id,
        case_plan_date AS start_date,
        -- Define each span of time as the period during which some case plan was the most
        -- recent entry for the special condition. The period also ends if the condition
        -- is suspended or expires.
        LEAST(
            {nonnull_end_date_clause("LEAD(case_plan_date) OVER condition_window")},
            {nonnull_end_date_clause("condition_end_date")},
            {nonnull_end_date_clause("condition_suspended_date")}
        ) AS end_date_exclusive,
        compliance,
        intervention_type,
        special_condition_type,
        reason_for_special_condition,
        condition_start_date,
        condition_end_date,
        condition_suspended_date,
    FROM special_condition_updates
    -- Ensure the case plan was entered while the special condition was active
    WHERE 
        case_plan_date < LEAST(
            {nonnull_end_date_clause("condition_end_date")},
            {nonnull_end_date_clause("condition_suspended_date")}
        )
        AND
        case_plan_date > condition_start_date
    WINDOW condition_window AS (
        PARTITION BY state_code, person_id, special_condition_id
        ORDER BY case_plan_date
    )
    ORDER BY special_condition_id, case_plan_date DESC
"""

US_NE_SPECIAL_CONDITION_COMPLIANCE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_NE_SPECIAL_CONDITION_COMPLIANCE_SESSIONS_VIEW_NAME,
    view_query_template=US_NE_SPECIAL_CONDITION_COMPLIANCE_SESSIONS_QUERY_TEMPLATE,
    description=__doc__,
    raw_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_NE, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_NE_SPECIAL_CONDITION_COMPLIANCE_SESSIONS_VIEW_BUILDER.build_and_print()
