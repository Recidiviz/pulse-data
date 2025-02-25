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
"""View with custody level changes"""
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Custody level changes"

_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT 
    cl.state_code,
    cl.person_id,
    cl.custody_level_num_change,
    months_between_assessment_due_and_downgrade,
    IF(custody_downgrade > 0, "DOWNGRADE", "UPGRADE") AS change_type,
    cl.previous_custody_level,
    cl.custody_level AS new_custody_level,
    cl.start_date
FROM
    `{project_id}.sessions.custody_level_sessions_materialized` cl
LEFT JOIN `{project_id}.analyst_data.number_months_between_custody_downgrade_and_assessment_due_materialized` n
    ON cl.person_id = n.person_id
    AND cl.state_code = n.state_code
    AND cl.custody_level_session_id = n.custody_level_session_id
WHERE
    custody_downgrade > 0 OR custody_upgrade > 0
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.CUSTODY_LEVEL_CHANGE,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "change_type",
        "previous_custody_level",
        "new_custody_level",
        "custody_level_num_change",
        "months_between_assessment_due_and_downgrade",
    ],
    event_date_col="start_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
