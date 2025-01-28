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
"""
Defines a criteria span view that shows spans of time during which a resident in Idaho does NOT have an active
discretionary override.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_NO_ACTIVE_DISCRETIONARY_OVERRIDE"

_DESCRIPTION = """Defines a criteria span view for residents in Idaho, identifying time periods when they do NOT
have an active discretionary override.

An active discretionary override is defined as:
1. An override applied within the last year, OR
2. An override applied after the most recent scheduled classification review.
"""

_QUERY_TEMPLATE = f"""
WITH classification_reviews AS(
    SELECT 
        state_code,
        person_id, 
        assessment_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_assessment`
    WHERE state_code = "US_IX"
        AND assessment_type = "RSLS"
        AND JSON_EXTRACT_SCALAR(assessment_metadata, "$.CLASSIFICATIONTYPE") != "RIDER"
),
discretionary_overrides AS (
    SELECT DISTINCT
        pei.person_id,
        DATE(ApprovedDate) AS override_date,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ind_OffenderSecurityLevel_latest` a
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ind_SecurityLevelReviewType_latest` b
      USING (SecurityLevelReviewTypeId) 
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.clsf_OffenderSecurityLevelAct_latest` c 
      USING(OffenderSecurityLevelId) 
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ind_SecurityLevelOverride_latest` d
        ON c.SecurityLevelOverrideId = d.SecurityLevelOverrideId 
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON pei.external_id = OffenderId
        AND pei.state_code = "US_IX"
    WHERE SecurityLevelOverrideDesc = 'Needs to be managed at higher custody level'
),
date_spans AS (
/* This cte associates each discretionary override to an INCARCERATION-GENERAL session, and selects the most recent
assessment date that happens AFTER that override so that we can set the critical date in the next cte
as the least of one year past the discretionary override, or the next assessment. */
    SELECT 
        c.state_code,
        c.person_id, 
        d.override_date AS start_date,
        c.end_date_exclusive,
        d.override_date,
        MIN(r.assessment_date) AS most_recent_assessment_date
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` c
    INNER JOIN discretionary_overrides d
        ON c.person_id = d.person_id 
        AND d.override_date BETWEEN c.start_date and {nonnull_end_date_clause('c.end_date')}
    LEFT JOIN classification_reviews r
        ON c.person_id = r.person_id 
        AND r.assessment_date BETWEEN c.start_date and {nonnull_end_date_clause('c.end_date')}
        AND r.assessment_date > d.override_date
    WHERE 
        c.compartment_level_1 = 'INCARCERATION'
        AND c.compartment_level_2 = 'GENERAL'
        AND c.state_code = 'US_IX'
    GROUP BY 1,2,3,4,5
),
critical_date_spans AS (
    SELECT
        state_code,
        person_id,
        start_date AS start_datetime,
        end_date_exclusive AS end_datetime,
        LEAST(DATE_ADD(override_date, INTERVAL 1 YEAR), {nonnull_end_date_clause('most_recent_assessment_date')}) AS critical_date
    FROM date_spans
),
{critical_date_has_passed_spans_cte()},
{create_sub_sessions_with_attributes('critical_date_has_passed_spans')}
SELECT 
    ssa.state_code,
    ssa.person_id,
    ssa.start_date,
    ssa.end_date,
    --if any span is FALSE, meets_criteria is FALSE
    LOGICAL_AND(critical_date_has_passed) AS meets_criteria,
    TO_JSON(STRUCT(MAX(critical_date) AS override_inactive_date)) AS reason,
    MAX(critical_date) AS override_inactive_date
FROM sub_sessions_with_attributes ssa
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX,
        instance=DirectIngestInstance.PRIMARY,
    ),
    reasons_fields=[
        ReasonsField(
            name="override_inactive_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The date on which the discretionary override becomes inactive",
        ),
    ],
    meets_criteria_default=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
