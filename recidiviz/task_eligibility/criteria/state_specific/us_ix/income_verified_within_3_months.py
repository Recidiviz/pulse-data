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
"""Defines a criteria span view that shows spans of time for
which employment or nonemployment lawful income is verified within 3 months
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time for
which employment or nonemployment lawful income is verified within 3 months"""

_QUERY_TEMPLATE = f"""
WITH verified_employment_spans AS (
--select contact notes with verified employment
    SELECT
        state_code,
        person_id,
        contact_date AS start_date,
        DATE_ADD(contact_date, INTERVAL 3 MONTH) AS end_date,
        TRUE AS verified_income,
        contact_date AS income_verified_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_contact`
    WHERE verified_employment
    AND state_code = 'US_IX' 
    UNION ALL
--select case notes with verified ssdi/ssi non employment income
    SELECT 
        a.state_code,
        a.person_id,
        a.NoteDate AS start_date,
        DATE_ADD(a.NoteDate, INTERVAL 3 MONTH) AS end_date,
        TRUE as verified_income,
        a.NoteDate AS income_verified_date,
    FROM `{{project_id}}.{{supplemental_dataset}}.us_ix_case_note_matched_entities` a
    WHERE (SSDI_SSI AND NOT PENDING)
    UNION ALL
--select open employment periods with UNABLE_TO_WORK
    SELECT DISTINCT
        ep.state_code,
        ep.person_id,
        start_date,
        end_date,
        TRUE as verified_income,
        start_date AS income_verified_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_employment_period` ep
    WHERE state_code = 'US_IX'
        AND employment_status = 'UNABLE_TO_WORK'
    ),
{create_sub_sessions_with_attributes('verified_employment_spans')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    verified_income AS meets_criteria,
    TO_JSON(STRUCT(IF(verified_income, MAX(income_verified_date), NULL) AS income_verified_date)) AS reason,
    IF(verified_income, MAX(income_verified_date), NULL) AS income_verified_date,
    
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
"""
VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_IX,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    reasons_fields=[
        ReasonsField(
            name="income_verified_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="The date on which employment or nonemployment lawful income is verified within 3 months",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
