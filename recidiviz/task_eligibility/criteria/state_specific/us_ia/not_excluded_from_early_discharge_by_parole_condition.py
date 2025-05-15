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
"""Shows spans of time during which a client is not excluded from early discharge
per board of parole condition"""
# TODO(#42139) to ingest into conditions on state_supervision_period

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IA_NOT_EXCLUDED_FROM_EARLY_DISCHARGE_BY_PAROLE_CONDITION"

_DESCRIPTION = __doc__

_REASON_QUERY = f"""
WITH conditions AS (
/* take all conditions that are part of the most recent set of conditions imposed (determined by effective date) for clients currently on parole/dual supervision */
/* note that this is a quick fix that only pulls current eligibility - once we get these ingested into state_supervision_period this query will be a lot cleaner and we can look at historical eligibility more easily */
    SELECT pei.state_code,
      pei.person_id,
      DATE(D.EffectiveDt) AS start_date,
      DATE(NULL) AS end_date,
      False AS meets_criteria,
      CONCAT(DC.DecisionCode, '-', DC.DecisionCodeDesc) AS condition,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.IA_DOC_BOPDecisionConditions_latest` DC
    INNER JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.IA_DOC_BOPDecisions_latest` D 
        USING(BOPDecisionId, OffenderCd)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei 
      ON CAST(DC.OffenderCd AS STRING) = pei.external_id
      AND pei.state_code = 'US_IA'
      AND pei.id_type = 'US_IA_OFFENDERCD'
    INNER JOIN `{{project_id}}.{{sessions_dataset}}.prioritized_supervision_sessions_materialized` ps
      ON pei.person_id = ps.person_id
      AND ps.end_date_exclusive IS NULL -- only pull current supervision period
      AND ps.compartment_level_2 IN ('PAROLE', 'DUAL') -- only applies to clients on parole/dual supervision 
    QUALIFY RANK() OVER(PARTITION BY OffenderCd ORDER BY D.EffectiveDt DESC) = 1 -- if multiple sets of conditions imposed, only take most recently effective
    # TODO(#42402) validate this logic to make sure we are only capturing currently effective conditions 
), ed_conditions AS (
/* pull conditions that exclude the client from early discharge */
    SELECT * 
    FROM conditions
    WHERE UPPER(condition) LIKE '%NO EARLY DISCHARGE%'
), /* sub-sessionize and aggregate for cases where a client has multiple open conditions at once */
{create_sub_sessions_with_attributes('ed_conditions')}
SELECT * EXCEPT(condition),
    TO_JSON(STRUCT(ARRAY_AGG(DISTINCT condition ORDER BY condition) AS conditions)) AS reason,
    ARRAY_AGG(DISTINCT condition ORDER BY condition) AS conditions,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_IA,
    criteria_spans_query_template=_REASON_QUERY,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IA,
        instance=DirectIngestInstance.PRIMARY,
    ),
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="conditions",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="List of open conditions (with codes and descriptions) that exclude the client from early discharge",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
