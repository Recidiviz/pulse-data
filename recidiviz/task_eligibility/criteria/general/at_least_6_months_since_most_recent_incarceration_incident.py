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
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.placeholder_criteria_builders import (
    state_agnostic_placeholder_criteria_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "AT_LEAST_6_MONTHS_SINCE_MOST_RECENT_INCARCERATION_INCIDENT"

_DESCRIPTION = """Describes the spans of time when a resident has had at least 6 months since the most
recent incarceration incident."""

_REASON_QUERY = (
    """TO_JSON(STRUCT('9999-99-99' AS latest_incarceration_incident_date))"""
)

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    state_agnostic_placeholder_criteria_view_builder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        reason_query=_REASON_QUERY,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
