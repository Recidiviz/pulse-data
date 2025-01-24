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
"""Defines a criteria span view that shows spans of time during which someone is
not serving on a sex offense protocol"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_NOT_ON_SEX_OFFENSE_PROTOCOL"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone is
not serving on a sex offense protocol"""

_QUERY_TEMPLATE = f"""
WITH sex_offense_condition_spans AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        termination_date AS end_date,
        False AS meets_criteria,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`,
    UNNEST(SPLIT(conditions, '##')) condition
    WHERE state_code = 'US_PA'
        AND ((condition LIKE '%SEX%' AND condition LIKE '%OFFEN%') 
            OR condition LIKE '%MEGANS%' 
            OR condition LIKE '%MEGAN\\'s%')
),
sex_offense_treatment_spans AS (
    SELECT 
        state_code,
        person_id,
        COALESCE(start_date, referral_date, '1900-01-01') AS start_date,
        discharge_date AS end_date,
        False AS meets_criteria,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_program_assignment` 
    WHERE state_code = 'US_PA'
        AND JSON_EXTRACT_SCALAR(referral_metadata, "$.PROGRAM_CODE") IN ('MEGS', 'MEGP', 'SSO', 'SEXO') 
        -- MEGS = megan's law, MEGP = megan's law, sexually violent predator, SSO = sex offender specialized caseload, SEXO = sex offender treatments/evaluations 
),
sex_offense_spans AS (
    SELECT * 
    FROM sex_offense_condition_spans
    WHERE start_date IS DISTINCT FROM end_date -- exclude zero-day sessions
    
    UNION ALL
    
    SELECT * 
    FROM sex_offense_treatment_spans
    WHERE start_date IS DISTINCT FROM end_date -- exclude zero-day sessions
),
{create_sub_sessions_with_attributes('sex_offense_spans')}
, deduped_sex_offense_spans AS (
    -- addresses overlapping spans 
    SELECT DISTINCT
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
     FROM sub_sessions_with_attributes
)
SELECT *,
    start_date AS sex_offense_protocol_start,
    TO_JSON(STRUCT(start_date AS sex_offense_protocol_start)) AS reason,
FROM({aggregate_adjacent_spans(table_name='deduped_sex_offense_spans', attribute = 'meets_criteria')})
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_PA,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="sex_offense_protocol_start",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date that someone began serving on sex offense protocol",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
