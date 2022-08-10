# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Describes the spans of time during which someone in ND is not
currently in the process of being revoked.
"""
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_exists_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_NOT_IN_ACTIVE_REVOCATION_STATUS"

_DESCRIPTION = """Describes the spans of time during which someone in ND is not
currently in the process of being revoked."""

_QUERY_TEMPLATE = f"""
/*{{description}}*/
WITH critical_date_update_datetimes AS (
    SELECT
        state_code,
        person_id,
        SAFE.PARSE_DATE('%m/%d/%Y', SPLIT(REV_DATE, ' ')[OFFSET(0)]) AS critical_date,
        update_datetime,
        SAFE.PARSE_DATETIME('%m/%d/%Y %l:%M:%S%p', RECORDCRDATE) AS created_datetime
    FROM `{{project_id}}.{{us_nd_raw_data}}.docstars_offendercasestable` raw
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON raw.SID = pei.external_id
        AND pei.id_type = "US_ND_SID"
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_code, person_id, update_datetime
        -- Prioritize the case record that was most recently created to handle when
        -- two cases are updated on the same date but one is closed (due to revocation)
        -- and a new case is opened
        ORDER BY created_datetime DESC, CASE_NUMBER DESC
    ) = 1
),
{critical_date_exists_spans_cte()}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    -- Mark this span as meeting the criteria if the revocation date is not set
    NOT critical_date_exists AS meets_criteria,
    TO_JSON(STRUCT(critical_date AS revocation_date)) AS reason,
FROM critical_date_exists_spans
/*
Create an artificial span leading up to the first real span to fill in the time
when the `docstars_offendercasestable` does not have a record for a client yet
*/
UNION ALL
SELECT
    state_code,
    person_id,
    DATE("1900-01-01") AS start_date,
    MIN(start_date) AS end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(NULL AS revocation_date)) AS reason,
FROM critical_date_exists_spans
GROUP BY state_code, person_id
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_ND,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        us_nd_raw_data=raw_tables_dataset_for_region("us_nd"),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
