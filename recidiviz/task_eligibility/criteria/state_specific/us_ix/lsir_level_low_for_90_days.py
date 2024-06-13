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
"""
Defines a criteria span view that shows spans of time during which
someone in ID has a valid LSIR level for the required number of days:
-LSI-R score at or below the low potential level to reoffend with no increase in risk
 for the 90 days of active supervision
-At or below moderate potential to reoffend can have no increase in risk level 360 days prior
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import lsir_spans
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_LSIR_LEVEL_LOW_FOR_90_DAYS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone in ID has a LSI-R score at or below the low potential level to reoffend with no increase in risk 
 for the 90 days of active supervision
"""

_QUERY_TEMPLATE = f"""
WITH {lsir_spans()},
critical_date_spans AS (
    SELECT
        state_code,
        person_id,
        score_span_start_date AS start_datetime,
        score_span_end_date AS end_datetime,
        DATE_ADD(score_span_start_date, INTERVAL 90 DAY) AS critical_date
    FROM lsir_spans
    WHERE lsir_level = "LOW"

),
{critical_date_has_passed_spans_cte()},
{create_sub_sessions_with_attributes('critical_date_has_passed_spans')}
SELECT 
    ssa.state_code,
    ssa.person_id,
    ssa.start_date,
    ssa.end_date,
    --choose TRUE spans over FALSE 
    LOGICAL_OR(critical_date_has_passed) AS meets_criteria,
    TO_JSON(STRUCT(MIN(critical_date) AS eligible_date, MAX(lsir_level) AS risk_level)) AS reason,
    MIN(critical_date) AS eligible_date,
    MAX(lsir_level) AS risk_level,
FROM sub_sessions_with_attributes ssa
LEFT JOIN LSIR_level_gender ls
  ON ls.state_code = ssa.state_code
  AND ls.person_id = ssa.person_id
  AND ssa.start_date >= ls.score_start_date AND ssa.start_date < ls.score_end_date
GROUP BY 1,2,3,4
ORDER BY person_id, start_date
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        reasons_fields=[
            ReasonsField(
                name="eligible_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="risk_level",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
