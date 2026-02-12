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
"""Helper SQL fragments that import raw tables for NC
"""


def prs_spans_cte() -> str:
    """Returns a CTE for Post-Release Supervision (PRS) spans in NC.

    PRS is represented as COMMUNITY_CONFINEMENT within SUPERVISION in compartment sessions.
    """
    return """
    prs_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
        FROM `{project_id}.sessions.compartment_sessions_materialized`
        WHERE compartment_level_1 IN ('SUPERVISION')
            AND compartment_level_2 IN ('COMMUNITY_CONFINEMENT')
            AND state_code = 'US_NC'
    )"""


FACILITY_PROGRAM_ID_VALUES = [
    # Substance abuse
    "SUBSTANCE ABUSE-COMPLETE RECOMMENDED SUBSTANCE ABUSE TREATMENT.",
    "SUBSTANCE ABUSE-PARTICIPATE AND COMPLETE BLACK MOUNTAIN.",  # residential treatment program
    "SUBSTANCE ABUSE-PARTICIPATE IN TROSA PROGRAM.",  # Triangle Residential Options for Substance Abusers
    "SUBSTANCE ABUSE-PARTICIPATE IN DART PROGRAM.",  # Drug Abuse Residential Treatment
    "SUBSTANCE ABUSE-COMPLETE DRUG EDUCATION CLASSES.",
    "SUBSTANCE ABUSE-PARTICIPATE AND COMPLETE TWO SESSIONS PER WEEK DWI.",
    # Mental health
    "MENTAL HEALTH-COMPLETE RECOMMENDED MENTAL HEALTH TREATMENT.",
    "MENTAL HEALTH-PARTICIPATE IN PSYCHOLOGICAL COUNSELING.",
    "MENTAL HEALTH-PARTICIPATE IN PSYCHIATRIC COUNSELING.",
    "MENTAL HEALTH-PARTICIPATE IN AND COMPLETE MENTAL HEALTH TREATMENT COURT.",
    "MENTAL HEALTH-PARTICIPATE IN MEDICAL/PSYCHOLOGICAL TREATMENT.",
]
