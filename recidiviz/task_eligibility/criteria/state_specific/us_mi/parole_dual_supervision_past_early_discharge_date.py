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
"""Defines a criteria span view that shows spans of time during which someone is past
their parole/dual supervision early discharge date, computed using US_MI specific logic.
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_PAROLE_DUAL_SUPERVISION_PAST_EARLY_DISCHARGE_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is past their parole/dual supervision early discharge date, computed using
US_MI specific logic.
        - If serving life, has served at least four years
        - If serving under MCL 750.317, 750.520b, 750.520c, 750.520d, 750.520f, 750.529, 750.349, 750.350, 750.213, 
             750.110 (with max term of 15+ years), 750.110a (with max term of 15+ years), 
            or habitual offender with underlying offense of one of those charges, has served at least two years
"""
_QUERY_TEMPLATE = f"""
WITH parole_starts AS (
  SELECT
    state_code,
    person_id,
    start_date AS parole_start_date,
    compartment_level_2 AS supervision_type,
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
  WHERE state_code = "US_MI"
    AND compartment_level_2 IN ("PAROLE", "DUAL")
    AND inflow_from_level_2 NOT IN ("PAROLE", "DUAL")
),
sentences AS (
  SELECT
      span.state_code,
      span.person_id,
      span.start_date,
      span.end_date,
      ARRAY_AGG(DISTINCT statute) AS statutes_being_served,
      LOGICAL_OR(sent.life_sentence) AS any_life_sentence,
      --checks for whether 750.110 and 750.110a are accompanied by a 15 year term 
      LOGICAL_OR(IF((statute IN ("750.110", "750.110A") AND CAST(DATE_DIFF(projected_completion_date_max,effective_date,DAY) AS INT64) >= 5475),
                  true, false)) AS any_qualifying_statute_term
  FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` span,
  UNNEST (sentences_preprocessed_id_array) AS sentences_preprocessed_id
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    USING (state_code, person_id, sentences_preprocessed_id)
  WHERE state_code = "US_MI"
  GROUP BY 1, 2, 3, 4
),
sentence_statutes_preprocessed AS (
 SELECT 
    * EXCEPT (statutes_being_served),
    EXISTS(SELECT * FROM UNNEST(statutes_being_served) AS x WHERE x IN 
            ("750.317", "750.520B", "750.520C", "750.520D", "750.520F", "750.529", "750.349", "750.350", "750.213")) AS any_qualifying_statute,
 FROM sentences
 --only select parole sessions where life sentences or qualifying statutes are being served
 WHERE EXISTS(SELECT * FROM UNNEST(statutes_being_served) AS x WHERE x IN 
            ("750.317", "750.520B", "750.520C", "750.520D", "750.520F", "750.529", "750.349", "750.350", "750.213"))
    OR any_life_sentence
    OR any_qualifying_statute_term
 ),
parole_starts_with_sentences AS (
  SELECT
    s.*,
    parole_start_date,
  FROM sentence_statutes_preprocessed s
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
      WHEN any_life_sentence THEN DATE_ADD(parole_start_date, INTERVAL 4 YEAR)
      WHEN (any_qualifying_statute OR any_qualifying_statute_term) THEN DATE_ADD(parole_start_date, INTERVAL 2 YEAR)
      ELSE NULL
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
        state_code=StateCode.US_MI,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
