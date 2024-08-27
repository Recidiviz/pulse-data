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
# ============================================================================
"""Describes spans of time during which a candidate does not have an active
    felony detainer"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_NO_ACTIVE_FELONY_DETAINERS"

_DESCRIPTION = """Describes spans of time during which a candidate does not have an active
    felony detainer"""

_QUERY_TEMPLATE = f"""
    WITH detainer_status AS ( 
        SELECT
          pei.state_code,
          pei.person_id, 
          PARSE_DATE('%m/%d/%Y', SPLIT(eval.CREATE_DTM, ' ')[OFFSET(0)] ) AS critical_date,
          ACTIVE_FELONY_DETAINER,
        FROM
          `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_TRANSITION_PRG_EVAL_latest` eval
        INNER JOIN 
        `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_TRANSITION_PRG_ELIG_latest` map_to_docid
        USING (TRANSITION_PRG_ELIGIBILITY_ID)
        LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.DOC_EPISODE_latest` doc_ep
        USING(DOC_ID)
        LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.PERSON_latest` person
        USING(PERSON_ID)
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON ADC_NUMBER = external_id 
        AND pei.state_code = 'US_AZ'
        AND pei.id_type = 'US_AZ_ADC_NUMBER'
    ),
    critical_date_spans AS (
        SELECT 
          detainer_status.state_code,
          detainer_status.person_id,
          detainer_status.critical_date,
          sesh.start_date AS start_datetime,
          sesh.end_date AS end_datetime, 
        FROM detainer_status
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sesh
        ON detainer_status.person_id = sesh.person_id
        AND critical_date BETWEEN sesh.start_date and IFNULL(sesh.end_date_exclusive, '9999-12-31')
        WHERE ACTIVE_FELONY_DETAINER = 'Y'
        -- Get the most recent felony detainer for a given session
        QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id, sesh.session_id ORDER BY critical_date DESC) = 1
    ),
    {critical_date_has_passed_spans_cte()}
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        NOT critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            critical_date AS latest_detainer_date
        )) AS reason,
        critical_date AS latest_detainer_date,
    FROM critical_date_has_passed_spans
    WHERE start_date != end_date
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="latest_detainer_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Date of the most recent felony detainer.",
    ),
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_AZ,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_AZ,
            instance=DirectIngestInstance.PRIMARY,
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
