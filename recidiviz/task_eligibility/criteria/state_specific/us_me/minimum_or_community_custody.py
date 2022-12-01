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
# =============================================================================

"""Defines a criteria view that shows spans of time for
which residents have a custody level of "Minimum" or "Community".
"""
from recidiviz.calculator.query.bq_utils import revert_nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_MINIMUM_OR_COMMUNITY_CUSTODY"

_DESCRIPTION = """Defines a criteria view that shows spans of time where
clients have a custody level of "Minimum" or "Community".
"""

_QUERY_TEMPLATE = f"""
WITH custody_levels AS (
    -- Grab custody levels, transform datetime to date, 
    -- merge to recidiviz ids, only keep distinct values
    SELECT
        state_code,
        person_id,
        CLIENT_SYS_DESC AS custody_level,
        SAFE_CAST(LEFT(CUSTODY_DATE, 10) AS DATE) AS start_date,
    #TODO(#16722): pull custody level from ingested data once it is hydrated in our schema
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_112_CUSTODY_LEVEL_latest`
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id`
        ON CIS_100_CLIENT_ID = external_id
        AND id_type = 'US_ME_DOC'
    INNER JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_1017_CLIENT_SYS_latest` cl_sys
        ON CIS_1017_CLIENT_SYS_CD = CLIENT_SYS_CD
),
custody_level_spans AS (
    SELECT
        state_code,
        person_id,
        custody_level,
        -- Use 999 for any other custody level to make them the highest priority
        COALESCE(custody_level_priority, 999) AS custody_level_priority,
        start_date,
        LEAD(start_date) OVER (PARTITION BY state_code, person_id ORDER BY start_date) AS end_date
    FROM custody_levels
    LEFT JOIN (
        -- Set the custody level priority from lowest to highest, where "Unclassified" is
        -- lowest so that it is dropped if there is any other overlapping value. All other
        -- levels not listed here will be inferred as the highest priority.
        SELECT *
        FROM UNNEST(
            ["Unclassified", "Community", "Minimum"]
        ) AS custody_level
        WITH OFFSET custody_level_priority
    ) priority
        USING (custody_level)
),
{create_sub_sessions_with_attributes(table_name='custody_level_spans', use_magic_date_end_dates=True)},
highest_priority_custody_level AS (
    SELECT
        state_code,
        person_id,
        custody_level,
        start_date,
        end_date
    FROM sub_sessions_with_attributes
    -- Pick the custody level with the highest priority for each span
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_code, person_id, start_date, end_date
        ORDER BY custody_level_priority DESC, custody_level
    ) = 1
)
SELECT
    state_code,
    person_id,
    start_date,
    {revert_nonnull_end_date_clause('end_date')} AS end_date,
    custody_level IN ("Community", "Minimum") AS meets_criteria,
    TO_JSON(STRUCT(custody_level AS custody_level)) AS reason
FROM highest_priority_custody_level
WHERE start_date != end_date
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
