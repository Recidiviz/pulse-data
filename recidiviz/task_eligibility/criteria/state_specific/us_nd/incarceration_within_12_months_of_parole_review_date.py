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
Defines a criteria view that shows spans of time for which residents
are within 12 or less months of their parole review date.
"""
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_INCARCERATION_WITHIN_12_MONTHS_OF_PAROLE_REVIEW_DATE"

_DESCRIPTION = """
Defines a criteria view that shows spans of time for which residents
are within 12 or less months of their parole review date.
"""

_QUERY_TEMPLATE = f"""
WITH medical_screening AS (
  SELECT 
    peid.state_code,
    peid.person_id,
    SAFE_CAST(SAFE.PARSE_DATETIME('%m/%d/%Y  %H:%M:%S%p', ms.MEDICAL_DATE) AS DATE) AS parole_review_date,
  FROM `{{project_id}}.{{raw_data_dataset}}.elite_offender_medical_screenings_6i` ms
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON peid.external_id = REPLACE(REPLACE(ms.OFFENDER_BOOK_ID,',',''), '.00', '')
      AND peid.id_type = 'US_ND_ELITE_BOOKING'
      AND peid.state_code = 'US_ND'
),

critical_date_spans AS (
  SELECT 
    iss.state_code,
    iss.person_id,
    iss.start_date AS start_datetime,
    iss.end_date AS end_datetime,
    DATE_SUB(MAX(ms.parole_review_date), INTERVAL 1 YEAR) AS critical_date,
    MAX(ms.parole_review_date) AS parole_review_date
  FROM `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
  LEFT JOIN medical_screening ms
    ON iss.state_code = ms.state_code
      AND iss.person_id = ms.person_id
      AND ms.parole_review_date BETWEEN iss.start_date AND IFNULL(iss.end_date, '9999-12-31')
  WHERE iss.state_code = 'US_ND'
  GROUP BY 1,2,3,4
),
{critical_date_has_passed_spans_cte(attributes=['parole_review_date'])}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(parole_review_date AS parole_review_date)) AS reason
FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ND,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_data_dataset=raw_tables_dataset_for_region(
            state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
