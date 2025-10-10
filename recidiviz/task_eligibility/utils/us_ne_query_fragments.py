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
Helper SQL queries for Nebraska
"""


def supervision_oras_overrides_completion_event_query_template(
    overridden_to_level: str,
) -> str:
    """Returns a query identifying supervision level overrides to a certain level.

    Args:
        overridden_to_level (str): The supervision level to which someone is overridden
    """
    return f"""
        SELECT
            state_code,
            person_id,
            CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S', JSON_EXTRACT_SCALAR(assessment_metadata, '$.DATE_OF_OVERRIDE')) AS DATE) AS completion_event_date,
        FROM `{{project_id}}.normalized_state.state_assessment`
        WHERE
            state_code = 'US_NE'
            -- Gather only ORAS assessments
            AND assessment_type = 'ORAS_COMMUNITY_SUPERVISION_SCREENING'
            -- Filter out "overrides" that aren't really overrides
            AND JSON_EXTRACT_SCALAR(assessment_metadata, '$.SUPERVISION_LEVEL_OVERRIDE') != assessment_level
            -- Identify overrides to the specified level ("{overridden_to_level}"")
            AND JSON_EXTRACT_SCALAR(assessment_metadata, '$.SUPERVISION_LEVEL_OVERRIDE') = '{overridden_to_level}'
    """
