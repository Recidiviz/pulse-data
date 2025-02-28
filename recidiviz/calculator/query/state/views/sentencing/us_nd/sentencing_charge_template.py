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
"""View logic to prepare US_ND Sentencing charge data for PSI tools

This view is identical to that used in IX."""

US_ND_SENTENCING_CHARGE_TEMPLATE = """
    SELECT
    description AS charge,
    "US_ND" AS state_code,
    -- Only count the number of times a charge appears in the past two years to determine frequency
    SUM(
    IF
        (DATE_DIFF(CURRENT_DATE("US/Eastern"), offense_date, YEAR) <= 2, 1, 0)) AS frequency,
    IF
    -- Determine if a charge is violent based what percentage of the time it is tagged as such, rounded down or up
    (ROUND(SUM(
        IF
            (is_violent, 1, 0)) / COUNT(*)) = 1,
        TRUE,
        FALSE) AS is_violent,
    IF
    -- Determine if a charge is a sex offense based what percentage of the time it is tagged as such, rounded down or up
    (ROUND(SUM(
        IF
            (is_sex_offense, 1, 0)) / COUNT(*)) = 1,
        TRUE,
        FALSE) AS is_sex_offense
    FROM
      `{project_id}.normalized_state.state_charge_v2`
    WHERE
    state_code = "US_ND" AND description is NOT NULL
    GROUP BY
    description
"""
