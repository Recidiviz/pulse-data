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
"""Defines a criteria span view that shows spans of time during which someone is past
their parole/dual supervision early discharge date, computed using US_ID specific logic.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
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

_CRITERIA_NAME = "US_IX_PAROLE_DUAL_SUPERVISION_PAST_EARLY_DISCHARGE_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is past their parole/dual supervision early discharge date, computed using
US_ID specific logic.
- If convicted of nonviolent crime, served at least one (1) year on parole
- If convicted of sex/violent offense, served at least one-third (1/3) of their remaining sentence on parole.
- If sentenced to a life term must serve at least five (5) years on parole
"""

_QUERY_TEMPLATE = f"""
WITH parole_starts AS (
  SELECT
    state_code,
    person_id,
    start_date AS parole_start_date,
    compartment_level_2 AS supervision_type,
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_{{sessions_dataset}}_materialized`
  WHERE state_code = "US_IX"
    AND compartment_level_2 IN ("PAROLE", "DUAL")
    AND inflow_from_level_2 NOT IN ("PAROLE", "DUAL")
),
sentences AS (
  SELECT
      span.state_code,
      span.person_id,
      span.start_date,
      span.end_date,
      LOGICAL_OR(sent.is_violent OR sent.is_sex_offense) AS any_violent_or_sex_offense,
      LOGICAL_OR(sent.life_sentence) AS any_life_sentence,
      MAX(sent.projected_completion_date_max) AS projected_completion_date,
  FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
  UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    USING (state_code, person_id, sentences_preprocessed_id)
  WHERE state_code = "US_IX"
  GROUP BY 1, 2, 3, 4
),
parole_starts_with_sentences AS (
  SELECT
    s.*,
    parole_start_date,
  FROM sentences s
  INNER JOIN parole_starts p
    ON p.state_code = s.state_code
    AND p.person_id = s.person_id
    AND p.parole_start_date < {nonnull_end_date_clause('s.end_date')}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.state_code, s.person_id, s.start_date
    -- Pick the most recent parole start date
    ORDER BY parole_start_date DESC
  ) = 1
),
critical_date_spans AS (
  SELECT
    state_code,
    person_id,
    start_date AS start_datetime,
    end_date AS end_datetime,
    CASE
      WHEN any_life_sentence THEN DATE_ADD(parole_start_date, INTERVAL 5 YEAR)
      WHEN any_violent_or_sex_offense
        THEN DATE_ADD(
          parole_start_date,
          INTERVAL CAST(
            CEILING(DATE_DIFF(projected_completion_date, parole_start_date, DAY) / 3)
          AS INT64) DAY
        )
      ELSE DATE_ADD(parole_start_date, INTERVAL 1 YEAR)
    END AS critical_date
  FROM parole_starts_with_sentences
),
{critical_date_has_passed_spans_cte()}
SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(
        sup_type.supervision_type AS sentence_type,
        cd.critical_date AS eligible_date
    )) AS reason,
    sup_type.supervision_type AS sentence_type,
    cd.critical_date AS eligible_date,
FROM critical_date_has_passed_spans cd
LEFT JOIN parole_starts sup_type
    ON sup_type.state_code = cd.state_code
    AND sup_type.person_id = cd.person_id
    AND sup_type.parole_start_date < {nonnull_end_date_clause('cd.end_date')}
-- Prioritize the latest supervision session
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id, cd.start_date
    ORDER BY parole_start_date DESC
) = 1
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_IX,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="sentence_type",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="eligible_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
