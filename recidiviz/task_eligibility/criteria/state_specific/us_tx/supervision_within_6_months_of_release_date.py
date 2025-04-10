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
Defines a criteria span view that shows spans of time during which
someone is on supervision within 6 months of their full term completion date.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.workflows.us_tx.shared_ctes import (
    US_TX_MAX_TERMINATION_DATES,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_SUPERVISION_WITHIN_6_MONTHS_OF_RELEASE_DATE"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone is on supervision within 6 months of their full term completion date.
"""


_REASON_QUERY = f"""
WITH us_tx_max_termination_dates AS (
    {US_TX_MAX_TERMINATION_DATES}
),
critical_date_spans AS (
    SELECT 
        comp_sessions.state_code,
        comp_sessions.person_id,
        comp_sessions.start_date as start_datetime,
        comp_sessions.end_date_exclusive as end_datetime,
        term_dates.tx_max_termination_date as critical_date
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` as comp_sessions
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` as ext_ids
      ON (comp_sessions.person_id = ext_ids.person_id)
      AND ext_ids.id_type = 'US_TX_SID'
    LEFT JOIN us_tx_max_termination_dates as term_dates
      ON term_dates.sid_number = ext_ids.external_id
      AND term_dates.tx_max_termination_date > comp_sessions.start_date
    WHERE comp_sessions.compartment_level_1 = 'SUPERVISION'
        AND comp_sessions.state_code = 'US_TX'       
),
{critical_date_has_passed_spans_cte(meets_criteria_leading_window_time = 6, date_part = 'MONTH')}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed as meets_criteria,
    TO_JSON(STRUCT(critical_date AS full_term_completion_date)) AS reason,
    critical_date AS full_term_completion_date
FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_REASON_QUERY,
    state_code=StateCode.US_TX,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    meets_criteria_default=False,
    us_tx_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TX,
        instance=DirectIngestInstance.PRIMARY,
    ),
    reasons_fields=[
        ReasonsField(
            name="full_term_completion_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Full term completion date, which is the same as the release date in TX since there is no early release.",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
