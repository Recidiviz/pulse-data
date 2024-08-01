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
"""Defines a criteria span view that shows spans of time during which someone is not serving
ineligible offenses on parole/dual supervision
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

_CRITERIA_NAME = "US_MI_NOT_SERVING_INELIGIBLE_OFFENSES_FOR_EARLY_DISCHARGE_FROM_PAROLE_DUAL_SUPERVISION"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is not serving on parole for any offense requiring registration under the Sex Offenders Registration Act
or an offense resulting in death, serious bodily injury, or an offense involving the discharge of a firearm
"""
_QUERY_TEMPLATE = f"""
 SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date,
        FALSE as meets_criteria,
        TO_JSON(STRUCT(ARRAY_AGG(DISTINCT statute) AS ineligible_offenses)) AS reason,
        ARRAY_AGG(DISTINCT statute) AS ineligible_offenses,
    {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap = ['SUPERVISION'],
                                                 compartment_level_2_to_overlap=['PAROLE', 'DUAL'])}
    WHERE span.state_code = "US_MI"
    --exclude probation sentences for DUAL clients
    AND sent.sentence_type = "INCARCERATION" 
    --only include sentence spans with offenses that require registration
    AND sent.statute IN (SELECT statute_code FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.RECIDIVIZ_REFERENCE_offense_exclusion_list_latest`
            WHERE (CAST(requires_so_registration AS BOOL) OR CAST(is_excluded_from_parole_ed AS BOOL))
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
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
