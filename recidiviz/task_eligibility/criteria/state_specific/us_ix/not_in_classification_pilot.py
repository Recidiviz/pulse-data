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
"""Defines a criteria span view that shows spans of time for
which a resident is not part of the classification pilot.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
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

_CRITERIA_NAME = "US_IX_NOT_IN_CLASSIFICATION_PILOT"

_DESCRIPTION = """Defines a criteria span view that shows spans of time for
which a resident is not part of the classification pilot."""

_QUERY_TEMPLATE = f"""
WITH active_alerts AS (
    SELECT
        pei.state_code,
        pei.person_id,
        CAST(CAST(StartDate AS DATETIME) AS DATE) AS start_date,
        {nonnull_end_date_clause('''
                CAST(CAST(EndDate AS DATETIME) AS DATE)''')} AS end_date,
        TRUE AS active_alert
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ind_Offender_Alert_latest` o
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON o.OffenderId = pei.external_id
        AND pei.state_code = 'US_IX'
        --caution code that indicates classification pilot
    WHERE AlertId = "1030"
        AND CAST(CAST(StartDate AS DATETIME) AS DATE) != {nonnull_end_date_clause('''
            CAST(CAST(EndDate AS DATETIME) AS DATE)''')}
),
{create_sub_sessions_with_attributes('active_alerts')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT active_alert AS meets_criteria,
    TO_JSON(STRUCT(LOGICAL_OR(active_alert) AS active_alert)) AS reason,
    LOGICAL_OR(active_alert) AS active_alert,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4,5
"""
VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_IX,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX,
        instance=DirectIngestInstance.PRIMARY,
    ),
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="active_alert",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Whether there is an active alert for the classification pilot",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
