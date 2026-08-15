# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared roster-collection SQL for the US_ND Elite (facilities) staff views.

Both the activity ingest view and the identity view collect the same set of
facilities staff: everyone in the base Elite staff table, plus any program
referral source who is not already in that table. The two views differ only in
how they transform the name columns afterward: the activity view passes them
through as-is, while the identity view applies its own name cleaning. This
template exposes the shared collection CTEs with the raw, untransformed name
columns so each view can apply its own transformations on top of the all_staff
CTE.

TODO(OBT-44439): Once the activity pipeline consumes the identity pipeline's
output and the activity staff view is deleted, fold this template into the
identity view.
"""

# Name of the CTE this template ends on, holding the deduplicated union of Elite
# staff and program-referral staff. Each consuming view selects from it and
# applies its own name transformations.
ALL_STAFF_CTE_NAME = "all_staff"

ELITE_STAFF_ROSTER_TEMPLATE = f"""
WITH
-- Collect all staff information from the base Elite staff table.
elite_staff AS (
SELECT DISTINCT
    REPLACE(REPLACE(STAFF_ID, '.00', ''), ',', '') AS STAFF_ID,
    FIRST_NAME,
    MIDDLE_NAME,
    LAST_NAME
FROM {{recidiviz_elite_staff_members}}
WHERE PERSONNEL_TYPE IN ('STAFF', 'CON')
    -- A NULL STAFF_ID would emit a staff row with no id, and the anti-join in
    -- program_referral_staff counts on every roster id being non-null.
    AND STAFF_ID IS NOT NULL
),
-- Collect any STAFF_IDs that are not included in elite_staff, but are included as a referral
-- source in programming data. An anti-join against elite_staff excludes the
-- ones already collected in a single scan; elite_staff.STAFF_ID is guaranteed
-- non-null above, so the join matches exactly the ids the roster already holds.
program_referral_staff AS (
SELECT DISTINCT
    referral.STAFF_ID,
    CAST(NULL AS STRING) AS FIRST_NAME,
    CAST(NULL AS STRING) AS MIDDLE_NAME,
    CAST(NULL AS STRING) AS LAST_NAME
FROM (
    SELECT REPLACE(REPLACE(REFERRAL_STAFF_ID, '.00', ''), ',', '') AS STAFF_ID
    FROM {{elite_OffenderProgramProfiles}}
    WHERE REFERRAL_STAFF_ID IS NOT NULL
) referral
LEFT JOIN elite_staff ON referral.STAFF_ID = elite_staff.STAFF_ID
WHERE elite_staff.STAFF_ID IS NULL
),
-- Combine the base Elite staff and the program-referral staff into a single roster.
{ALL_STAFF_CTE_NAME} AS (
    SELECT * FROM elite_staff
    UNION ALL
    SELECT * FROM program_referral_staff
)
"""
