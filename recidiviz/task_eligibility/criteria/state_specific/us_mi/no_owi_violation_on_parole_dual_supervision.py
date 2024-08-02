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
# ============================================================================
"""Defines a criteria span view that shows spans of time during which someone has no new
operating while intoxicated violation on parole/dual supervision
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_NO_OWI_VIOLATION_ON_PAROLE_DUAL_SUPERVISION"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone has no involvement in a violation of MCL 257.625 (Operating while intoxicated) during parole term
"""
_QUERY_TEMPLATE = f"""
WITH parole_starts AS (
/* This CTE creates spans of parole or dual supervision sessions */
  SELECT
    state_code,
    person_id,
    start_date,
    end_date,
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_{{sessions_dataset}}_materialized`
  WHERE state_code = "US_MI"
    AND compartment_level_2 IN ("PAROLE", "DUAL")
    --ignore transitions from PAROLE to DUAL as parole starts
    AND inflow_from_level_2 NOT IN ("PAROLE", "DUAL")
),
sentences AS (
/* This CTE groups by person and date imposed to identify dates where OWI charges were imposed */
  SELECT
      span.state_code,
      span.person_id,
      sent.date_imposed,
      --exclude all subsections of 257.625 except for (2) and (10), and exclude all codes like 257.625M or 257.625B
      --which are separate mcl codes and not subsections of 257.625
      LOGICAL_OR(REGEXP_CONTAINS(REGEXP_REPLACE(statute, r'257.625', ''), r'^[13456789][a-zA-Z]*([^0a-zA-Z]|$)|^$')) AS is_owi,
  FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
  UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    USING (state_code, person_id, sentences_preprocessed_id)
  WHERE state_code = "US_MI"
  AND sent.statute LIKE '257.625%'
  GROUP BY 1, 2, 3
),
critical_date_spans AS (
  SELECT
    p.state_code,
    p.person_id,
    p.start_date AS start_datetime,
    p.end_date AS end_datetime,
    s.date_imposed AS critical_date,
  FROM parole_starts p
  INNER JOIN sentences s
    ON p.state_code = s.state_code
    AND p.person_id = s.person_id
    --only join parole spans that have a OWI date imposed during a parole session
    AND s.date_imposed > p.start_date 
    AND s.date_imposed <= {nonnull_end_date_clause('p.end_date')}
    AND s.is_owi
),
{critical_date_has_passed_spans_cte()},
{create_sub_sessions_with_attributes('critical_date_has_passed_spans')}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    --group by spans and if any OWI date has passed, set meets_criteria to FALSE
    NOT LOGICAL_OR(cd.critical_date_has_passed) AS meets_criteria,
    --only included sentence dates where meets_criteria is FALSE
    TO_JSON(STRUCT(
      IF(LOGICAL_OR(cd.critical_date_has_passed), ARRAY_AGG(cd.critical_date), NULL)
        AS latest_ineligible_convictions
    )) AS reason,
    IF(LOGICAL_OR(cd.critical_date_has_passed), ARRAY_AGG(cd.critical_date), NULL)
        AS latest_ineligible_convictions,
FROM sub_sessions_with_attributes cd
GROUP BY 1,2,3,4

"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="latest_ineligible_convictions",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="List of OWI convictions while on supervision",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
