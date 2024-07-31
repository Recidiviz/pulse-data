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
# =============================================================================

"""Defines a criteria view that shows spans of time for
which residents do not have a sex offender alert"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_NO_SEX_OFFENDER_ALERT"

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which residents do not have a sex offender alert"""

_SEX_OFFENDER_CODES = [
    "61",  # Supervise as Sex offender
    "62",  # Sex Offender Registration Required
]

_QUERY_TEMPLATE = f"""
WITH sex_offender_alert AS (
    SELECT
        peid.state_code,
        peid.person_id,
        SAFE_CAST(LEFT(off.StartDate, 10) AS DATE) AS start_date,
        SAFE_CAST(LEFT(off.EndDate, 10) AS DATE) AS end_date,
        off.AlertId AS alert_id,
        ia.AlertDesc AS alert_description,
    FROM `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_Offender_Alert_latest` off
    LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.ind_Alert_latest` ia
    USING(AlertId)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON peid.external_id = off.OffenderId
        AND peid.state_code = 'US_IX'
        AND peid.id_type = 'US_IX_DOC'
    -- Alerts related to sex offenses
    WHERE off.AlertId IN {tuple(_SEX_OFFENDER_CODES)}
),
{create_sub_sessions_with_attributes(table_name='sex_offender_alert')}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    False AS meets_criteria,
    TO_JSON(STRUCT(STRING_AGG(DISTINCT alert_description ORDER BY alert_description) AS latest_alert_descriptions,
                   STRING_AGG(DISTINCT alert_id ORDER BY alert_id) AS latest_alert_ids)) AS reason,
    STRING_AGG(DISTINCT alert_description ORDER BY alert_description) AS latest_alert_descriptions,
    STRING_AGG(DISTINCT alert_id ORDER BY alert_id) AS latest_alert_ids
FROM sub_sessions_with_attributes
WHERE start_date < {nonnull_end_date_clause('end_date')}
GROUP BY 1,2,3,4"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_IX,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="latest_alert_descriptions",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="List of distinct alert descriptions for the latest alert",
            ),
            ReasonsField(
                name="latest_alert_ids",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="List of distinct alert IDs for the latest alert",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
