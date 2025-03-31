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
"""Shows spans of time during which a client has no specialty of sex offender"""
# TODO(#40262) - replace raw data reference with supervision case type once ingested

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

_CRITERIA_NAME = "US_IA_NO_SEX_OFFENDER_SPECIALTY"

_DESCRIPTION = __doc__

_REASON_QUERY = f"""
WITH specialties AS (
/* pull spans of time where a client has an open sex offender specialty */
    SELECT state_code,
      person_id, 
      DATE(SpecialtyStartDt) AS start_date,
      DATE(SpecialtyEndDt) AS end_date,
      False AS meets_criteria,
      specialty,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.IA_DOC_Specialties_latest` spe
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei 
      ON spe.OffenderCd = pei.external_id
      AND pei.state_code = 'US_IA'
      AND pei.id_type = 'US_IA_OFFENDERCD'
    WHERE DATE(SpecialtyStartDt) IS DISTINCT FROM DATE(SpecialtyEndDt) -- remove 0-day spans 
      AND specialty = 'Sex Offender'
), 
/* sub-sessionize and aggregate for cases where a client has multiple SO specialties at once */
{create_sub_sessions_with_attributes('specialties')}
SELECT *,
    TO_JSON(STRUCT(specialty)) AS reason,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5,6
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
                name="specialty",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Specialty description (will always be Sex Offender)",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
