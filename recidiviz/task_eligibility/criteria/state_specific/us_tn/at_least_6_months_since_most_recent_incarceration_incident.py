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
"""Describes the spans of time when a resident has had at least 6 months since the most recent incarceration incident."""
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    INCARCERATION_INCIDENTS_FOUND_WHERE_CLAUSE,
    has_at_least_x_incarceration_incidents_in_time_interval,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_AT_LEAST_6_MONTHS_SINCE_MOST_RECENT_INCARCERATION_INCIDENT"

_DESCRIPTION = """Describes the spans of time when a resident has had at least 6 months since the most
recent incarceration incident."""


_QUERY_TEMPLATE = f"""
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        -- if someone has had at least 1 incident in the past 6 months, they don't meet the criteria. otherwise, they do
        NOT meets_criteria AS meets_criteria, 
        reason
    FROM (
        {has_at_least_x_incarceration_incidents_in_time_interval(
            number_of_incidents=1,
            date_interval = 6, 
            date_part = "MONTH",
            where_clause = INCARCERATION_INCIDENTS_FOUND_WHERE_CLAUSE)}
    )
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        state_code=StateCode.US_TN,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
        ),
    )
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
