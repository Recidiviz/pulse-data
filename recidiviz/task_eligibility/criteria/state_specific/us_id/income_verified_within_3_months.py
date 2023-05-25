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
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ID_INCOME_VERIFIED_WITHIN_3_MONTHS"

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
    UNION ALL
--select case notes with verified ssdi/ssi non employment income
    SELECT 
        a.state_code,
        a.person_id,
        a.create_dt AS start_date,
        DATE_ADD(a.create_dt, INTERVAL 3 MONTH) AS end_date,
        TRUE as verified_income,
        a.create_dt AS income_verified_date,
    FROM `{{project_id}}.{{supplemental_dataset}}.us_id_case_note_matched_entities` a
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.agnt_case_updt_latest` cn
      USING(agnt_case_updt_id)
    WHERE (SSDI_SSI AND NOT PENDING)
    ),
{create_sub_sessions_with_attributes('verified_employment_spans')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    verified_income AS meets_criteria,
    TO_JSON(STRUCT(start_date AS income_verified_date)) AS reason,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
"""
VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ID,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ID,
            instance=DirectIngestInstance.PRIMARY,
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
