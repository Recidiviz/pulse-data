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
Defines a criteria view that shows spans of time when clients are not serving any Admin Supervision-ineligible sentences.
All offense info for below can be found at https://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm by selecting
the correct title (typically 18)
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_pa_query_fragments import (
    offense_is_admin_ineligible,
    sentences_and_charges_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION"

_DESCRIPTION = """Defines a criteria view that shows spans of time when clients are not serving any Admin Supervision-ineligible sentences.
All offense info for below can be found at https://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm by selecting
the correct title (typically 18)
"""

_REASON_QUERY = f"""
    WITH sentences_and_charges AS (
    /* combine sentences preprocessed (to get correct date info) and state charge (to get status of individual charges) */
        {sentences_and_charges_cte()}
    ), ineligible_spans AS (
          SELECT
            state_code,
            person_id,
            CASE WHEN date_imposed IS NULL OR date_imposed > CURRENT_DATE('US/Eastern') 
                THEN '1900-01-01' ELSE date_imposed END AS start_date, -- placeholder for missing or incorrect imposed dates
            CAST(NULL AS DATE) AS end_date,
            CONCAT(COALESCE(statute, ''), ' ', COALESCE(description, '')) AS offense,
            FALSE AS meets_criteria,
          FROM sentences_and_charges
          WHERE
            state_code = 'US_PA'
            AND {offense_is_admin_ineligible()}
            -- Removes a few cases where someone has multiple spans for the same offense so they aren't later
            -- aggregated unnecessarily
            QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, description ORDER BY date_imposed ASC, projected_completion_date_max DESC) = 1 ),
    {create_sub_sessions_with_attributes('ineligible_spans')},
    dedup_cte AS (
        SELECT * except (offense),
            TO_JSON(STRUCT(ARRAY_AGG(DISTINCT offense ORDER BY offense) AS ineligible_offenses)) AS reason,
            ARRAY_AGG(DISTINCT offense ORDER BY offense) AS ineligible_offenses,
        FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4,5
     )
    SELECT
      *
    FROM
      dedup_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_REASON_QUERY,
    state_code=StateCode.US_PA,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="ineligible_offenses",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="List of offenses that a client has committed which make them ineligible for admin supervision",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
