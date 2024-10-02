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
"""Sessionized view of `task_eligibility.all_tasks` that aggregates ineligible criteria
accross any overlapping span collapses spans based on the `task_name` and `is_eligible`
fields for each client"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ALL_TASK_INELIGIBLE_CRITERIA_SUBSESSION_VIEW_NAME = (
    "all_task_type_ineligible_criteria_sessions"
)

ALL_TASK_INELIGIBLE_CRITERIA_SUBSESSIONS_VIEW_DESCRIPTION = """Sessionized view of `task_eligibility.all_tasks`
that aggregates ineligible criteria accross any overlapping span, and collapses spans based
on `completion_event_type`,  `state_code` and `person_id` fields for each client. """

ALL_TASK_INELIGIBLE_CRITERIA_SUBSESSIONS_QUERY_TEMPLATE = f"""
    WITH joined_tasks AS (
        SELECT 
            atm.person_id, 
            atm.state_code, 
            atm.start_date, 
            atm.end_date,
            atm.is_eligible,
            t_e.completion_event_type,
            atm.ineligible_criteria
        FROM
            `{{project_id}}.task_eligibility.all_tasks_materialized` atm
        LEFT JOIN
            `{{project_id}}.reference_views.task_to_completion_event` t_e
        USING
            (state_code, task_name)
    ),
    {create_sub_sessions_with_attributes(table_name='joined_tasks',
                       index_columns=['completion_event_type', 'person_id', 'state_code'],
                       use_magic_date_end_dates=False,
                       end_date_field_name='end_date')}
    SELECT
        person_id,
        state_code,
        completion_event_type,
        start_date,
        end_date,
        -- Client is marked as eligible only if all the associated opportunities in that span are also is_eligilble=True
        LOGICAL_AND(COALESCE(is_eligible, FALSE)) AS is_eligible,
        STRING_AGG(DISTINCT criterion, ', ' ORDER BY criterion) AS ineligible_criteria
    FROM sub_sessions_with_attributes
    CROSS JOIN UNNEST(ineligible_criteria) AS criterion
    GROUP BY
        person_id,
        state_code,
        completion_event_type,
        start_date,
        end_date
"""


ALL_TASK_TYPE_INELIGIBLE_CRITERIA_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=ALL_TASK_INELIGIBLE_CRITERIA_SUBSESSION_VIEW_NAME,
    view_query_template=ALL_TASK_INELIGIBLE_CRITERIA_SUBSESSIONS_QUERY_TEMPLATE,
    description=ALL_TASK_INELIGIBLE_CRITERIA_SUBSESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_TASK_TYPE_INELIGIBLE_CRITERIA_SESSIONS_VIEW_BUILDER.build_and_print()
