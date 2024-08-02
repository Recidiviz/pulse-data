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
"""Defines a criteria span view that shows spans of time during which someone was not
required to register under SORA (Sex Offender Registration Act)
"""
from google.cloud import bigquery

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

_CRITERIA_NAME = "US_MI_NOT_REQUIRED_TO_REGISTER_UNDER_SORA"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone was not
required to register under SORA"""

_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        --find the earliest date imposed for a sex offense requiring registration
        MIN(date_imposed) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(MIN(date_imposed) AS ineligible_date)) AS reason,
        MIN(date_imposed) AS sentence_date_imposed,
    FROM `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized` sent
    INNER JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.RECIDIVIZ_REFERENCE_offense_exclusion_list_latest` l
        ON sent.statute = l.statute_code
        AND CAST(requires_so_registration AS BOOL)
    WHERE state_code = "US_MI" 
    GROUP BY 1,2
"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_MI,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI,
        instance=DirectIngestInstance.PRIMARY,
    ),
    meets_criteria_default=True,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="sentence_date_imposed",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Earliest date imposed for a sentence that requires registration under SORA",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
