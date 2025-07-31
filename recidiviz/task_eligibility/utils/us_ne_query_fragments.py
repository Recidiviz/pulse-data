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


def us_ne_state_specific_contact_types_query_fragment() -> str:
    """
    Compiles data on supervision contacts completed in NE
    """
    return """
    -- initial CTE: pull out LE indicator, expand "PERSONAL/COLLATERAL" contacts into two rows: PERSONAL and COLLATERAL
    WITH events_expanded AS (
     SELECT
        state_code,
        person_id,
        external_id AS contact_external_id,
        contact_date,
        JSON_EXTRACT_SCALAR(supervision_contact_metadata, '$.lawEnforcement') AS is_law_enforcement,
        contact_type_raw_text_split
     FROM
        `{project_id}.{normalized_state_dataset}.state_supervision_contact`,
        UNNEST(CASE
                WHEN contact_type_raw_text LIKE '%/%' AND contact_type_raw_text != 'LE/NCJIS CHECK' THEN SPLIT(contact_type_raw_text, '/')
                ELSE [contact_type_raw_text]
            END) AS contact_type_raw_text_split
     WHERE
        state_code = "US_NE"
        AND status = "COMPLETED")
    -- final CTE: recode contact_type_raw_text now that "PERSONAL/COLLATERAL" is separated out
    SELECT
     state_code,
     person_id,
     contact_external_id,
     contact_date,
     CASE
        WHEN contact_type_raw_text_split = "PERSONAL" AND is_law_enforcement != '1' THEN "PERSONAL"
        WHEN contact_type_raw_text_split = "COLLATERAL" AND is_law_enforcement != '1' THEN "COLLATERAL"
        WHEN contact_type_raw_text_split = "LE/NCJIS CHECK" THEN "NCJIS"
        ELSE NULL
        END AS contact_type
    FROM events_expanded
    """
