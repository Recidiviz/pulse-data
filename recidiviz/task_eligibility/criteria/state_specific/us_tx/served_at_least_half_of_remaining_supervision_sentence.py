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
someone has been under supervision for at least one half of the time that remained on their sentence
when released to supervision.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.workflows.us_tx.shared_ctes import (
    US_TX_MAX_TERMINATION_DATES,
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

_CRITERIA_NAME = "US_TX_SERVED_AT_LEAST_HALF_OF_REMAINING_SUPERVISION_SENTENCE"


_REASON_QUERY = f"""
WITH 
us_tx_max_termination_dates AS (
    {US_TX_MAX_TERMINATION_DATES}
),
critical_date_spans AS (
    SELECT 
        comp_sessions.state_code,
        comp_sessions.person_id,
        comp_sessions.start_date as start_datetime,
        comp_sessions.end_date_exclusive as end_datetime,
        DATE_ADD(comp_sessions.start_date,
            INTERVAL DIV(DATE_DIFF(term_dates.tx_max_termination_date, comp_sessions.start_date, DAY), 2) DAY
            ) AS critical_date
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` as comp_sessions
    LEFT JOIN us_tx_max_termination_dates as term_dates
      ON term_dates.person_id = comp_sessions.person_id
      AND term_dates.tx_max_termination_date > comp_sessions.start_date
    WHERE comp_sessions.compartment_level_1 = 'SUPERVISION'
        AND comp_sessions.state_code = 'US_TX'   
),
{critical_date_has_passed_spans_cte()}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed as meets_criteria,
    TO_JSON(STRUCT(critical_date AS served_at_least_half_of_remaining_supervision_sentence_date)) AS reason,
    critical_date AS served_at_least_half_of_remaining_supervision_sentence_date
FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_REASON_QUERY,
    sessions_dataset=SESSIONS_DATASET,
    state_code=StateCode.US_TX,
    us_tx_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TX,
        instance=DirectIngestInstance.PRIMARY,
    ),
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="served_at_least_half_of_remaining_supervision_sentence_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date at which someone has been under supervision for at least one half of the time that remained on their sentence when released to supervision.",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
