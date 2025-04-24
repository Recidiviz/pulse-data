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
# ============================================================================
"""Shows spans of time during which a client is not serving an ineligible offense for early discharge.
See CBC-FS-01 Supervision Early Discharge Appendix A for list of ineligible offenses
and https://www.legis.iowa.gov/law/iowaCode/chapters?title=XVI&year=2025 for Iowa criminal policy"""

from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SENTENCE_SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_ia_query_fragments import (
    is_fleeing,
    is_other_ed_ineligible_offense,
    is_veh_hom,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IA_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_EARLY_DISCHARGE"

_DESCRIPTION = __doc__

_REASON_QUERY = f"""
WITH clean_offenses AS (
  SELECT *, 
    SPLIT(REGEXP_REPLACE(statute, '[,|/(]', '#'), '#')[SAFE_OFFSET(0)] AS clean_statute, -- remove anything after parentheses/commas - for example, transform 707.3(A) -> 707.3
    CONCAT(COALESCE(statute, ''), ' ', COALESCE(description, '')) AS offense,
  FROM `{{project_id}}.{{sentence_sessions_dataset}}.sentences_and_charges_materialized`
  WHERE state_code = 'US_IA'
), preprocessed_spans as (
  SELECT
    span.state_code,
    span.person_id,
    span.start_date,
    span.end_date_exclusive AS end_date,
    LOGICAL_OR({is_veh_hom()}) as veh_homicide_indicator, 
    LOGICAL_OR({is_fleeing()}) as fleeing_indicator, 
    LOGICAL_OR({is_other_ed_ineligible_offense()})  AS other_ed_ineligible_offense_indicator,
    ARRAY_AGG(DISTINCT offense ORDER BY offense) AS ineligible_offenses,
  FROM `{{project_id}}.{{sentence_sessions_dataset}}.overlapping_sentence_serving_periods_materialized` span,
  UNNEST (sentence_id_array) AS sentence_id
  INNER JOIN clean_offenses
    USING (state_code, person_id, sentence_id)
  WHERE span.state_code = "US_IA"
    AND ({is_veh_hom()} OR {is_fleeing()} OR {is_other_ed_ineligible_offense()})
  GROUP BY 1,2,3,4
)
SELECT state_code,
  person_id, 
  start_date, 
  end_date,
  False AS meets_criteria,
  ineligible_offenses,
  TO_JSON(STRUCT(ineligible_offenses)) AS reason,
FROM preprocessed_spans
WHERE other_ed_ineligible_offense_indicator 
    OR (veh_homicide_indicator AND fleeing_indicator) -- per appendix A, vehicular homicide is only ineligible if they also fled the accident
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_IA,
    criteria_spans_query_template=_REASON_QUERY,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="ineligible_offenses",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="List of offenses that a client is serving for which make them ineligible for early discharge",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
