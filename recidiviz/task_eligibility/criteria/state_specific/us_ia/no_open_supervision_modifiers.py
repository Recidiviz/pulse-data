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
"""Shows spans of time during which a client has no open supervision modifiers"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
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

_CRITERIA_NAME = "US_IA_NO_OPEN_SUPERVISION_MODIFIERS"

_DESCRIPTION = __doc__

_REASON_QUERY = f"""
WITH modifiers AS (
/* pull spans of time where a client has an open supervision modifier */
    SELECT state_code,
      person_id, 
      DATE(SupervisionModifierStartDt) AS start_date,
      DATE(SupervisionModifierEndDt) AS end_date,
      False AS meets_criteria,
      SupervisionModifier AS modifier,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.IA_DOC_Supervision_Modifiers_latest` mod
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei 
      ON mod.OffenderCd = pei.external_id
      AND pei.state_code = 'US_IA'
      AND pei.id_type = 'US_IA_OFFENDERCD'
    WHERE DATE(SupervisionModifierStartDt) IS DISTINCT FROM DATE(SupervisionModifierEndDt) -- remove 0-day spans 
), 
/* sub-sessionize and aggregate for cases where a client has multiple open supervision modifiers at once */
{create_sub_sessions_with_attributes('modifiers')}
SELECT * EXCEPT(modifier),
    TO_JSON(STRUCT(ARRAY_AGG(DISTINCT modifier ORDER BY modifier) AS open_supervision_modifiers)) AS reason,
    ARRAY_AGG(DISTINCT modifier ORDER BY modifier) AS open_supervision_modifiers,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_IA,
        criteria_spans_query_template=_REASON_QUERY,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_IA,
            instance=DirectIngestInstance.PRIMARY,
        ),
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="open_supervision_modifiers",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="List of open supervision modifiers",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
