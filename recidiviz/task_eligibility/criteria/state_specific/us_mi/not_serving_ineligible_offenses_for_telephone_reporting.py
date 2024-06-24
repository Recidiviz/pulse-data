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
# =============================================================================
"""Defines a criteria span view that shows spans of time during which
someone is not serving ineligible offenses on supervision for downgrade to minimum telephone reporting
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_spans_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_NOT_SERVING_INELIGIBLE_OFFENSES_FOR_TELEPHONE_REPORTING"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is not serving ineligible offenses (below) for downgrade to minimum telephone reporting
        - not serving an offense excluded in the TRS exclusion list
        - not serving a life or commuted sentence
        - not serving probation with delay of sentence
        - not serving for a SORA Offense 
        - not currently serving on a felony offense involving possession or use of a firearm.
"""

_QUERY_TEMPLATE = f"""
 SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date,
        FALSE as meets_criteria,
        TO_JSON(STRUCT(ARRAY_AGG(DISTINCT statute IGNORE NULLS ORDER BY statute) AS ineligible_offenses,
                        ARRAY_AGG(DISTINCT sent.status IGNORE NULLS ORDER BY sent.status) AS sentence_status,
                         LOGICAL_OR(sent.life_sentence) AS is_life_sentence,
                         ARRAY_AGG(DISTINCT ref.description IGNORE NULLS ORDER BY ref.description) AS sentence_status_raw_text)) AS reason,
        ARRAY_AGG(DISTINCT statute IGNORE NULLS ORDER BY statute) AS ineligible_offenses,
        ARRAY_AGG(DISTINCT sent.status IGNORE NULLS ORDER BY sent.status) AS sentence_status,
        LOGICAL_OR(sent.life_sentence) AS is_life_sentence,
        ARRAY_AGG(DISTINCT ref.description IGNORE NULLS ORDER BY ref.description) AS sentence_status_raw_text,
    {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap="SUPERVISION")}
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ADH_REFERENCE_CODE_latest` ref 
        ON sent.status_raw_text = ref.reference_code_id
    WHERE span.state_code = "US_MI"
    --offenses that are excluded for TR and offenses that requires SO registration 
    AND (sent.statute IN (SELECT statute_code 
                            FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.RECIDIVIZ_REFERENCE_offense_exclusion_list_latest`
                            WHERE (CAST(is_excluded_from_trs AS BOOL) OR CAST(requires_so_registration AS BOOL))
                            )
        OR sent.status = 'COMMUTED'
        OR sent.life_sentence
        --serving probation with delay of sentence
        OR LOWER(ref.description) like '%delay%'
        )
    GROUP BY 1, 2, 3, 4, 5
    """

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MI,
            instance=DirectIngestInstance.PRIMARY,
        ),
        reasons_fields=[
            ReasonsField(
                name="ineligible_offenses",
                type=bigquery.enums.SqlTypeNames.RECORD,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="sentence_status",
                type=bigquery.enums.SqlTypeNames.RECORD,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="is_life_sentence",
                type=bigquery.enums.SqlTypeNames.BOOLEAN,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="sentence_status_raw_text",
                type=bigquery.enums.SqlTypeNames.RECORD,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
