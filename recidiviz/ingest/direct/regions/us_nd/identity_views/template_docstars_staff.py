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
"""Shared roster-collection SQL for the US_ND Docstars (supervision) staff views.

Both the activity ingest view and the identity view collect the same set of
officers from the monthly P&P directory reference file and the nightly Docstars
officers table, then choose the most recent name and email for each officer. The
two views differ only in how they transform those name and email columns
afterward: the activity view uppercases them, while the identity view applies
its own name cleaning. This template exposes the shared collection CTEs with the
raw, untransformed name and email columns so each view can apply its own
transformations on top of the latest_names CTE.

TODO(OBT-44439): Once the activity pipeline consumes the identity pipeline's
output and the activity staff view is deleted, fold this template into the
identity view.
"""

# Name of the CTE this template ends on. Each consuming view selects from it and
# applies its own name and email transformations.
LATEST_NAMES_CTE_NAME = "latest_names"

DOCSTARS_STAFF_ROSTER_TEMPLATE = f"""
WITH
-- This CTE pulls officer information from the P&P directory we receive monthly via email
-- and upload as a reference file.
staff_from_directory AS (
SELECT DISTINCT
    OFFICER,
    LastName,
    FirstName,
    email,
    CAST(update_datetime AS DATETIME) AS RecDate
FROM (
    SELECT
        CAST(CAST(OFFICER AS INT64) AS STRING) AS OFFICER,
        LastName,
        FirstName,
        email,
        update_datetime,
        -- Partition on the cast id so that '007' and '7' rank together, and
        -- order descending so the most recent directory row wins.
        ROW_NUMBER() OVER (
            PARTITION BY CAST(OFFICER AS INT64)
            ORDER BY update_datetime DESC) AS recency_rank
    FROM {{RECIDIVIZ_REFERENCE_PP_directory@ALL}}
    -- Do not include the three erroneous non-numeric staff IDs in these results.
    WHERE SAFE_CAST(OFFICER AS INT64) IS NOT NULL
    )
WHERE recency_rank = 1
),
-- This CTE pulls the most recently available information for each officer in Docstars.
staff_from_docstars AS (
SELECT DISTINCT
    OFFICER,
    LNAME AS LastName,
    FNAME AS FirstName,
    EMAIL,
    CAST(RecDate AS DATETIME) AS RecDate
FROM (
    SELECT
        -- Officer external IDs are either all letters or all numbers. If they are
        -- numbers, trim leading zeroes by casting to INT64 then back to string.
        CASE
          WHEN REGEXP_CONTAINS(OFFICER , '[0-9]') THEN CAST(CAST(OFFICER AS INT64) AS STRING)
          ELSE OFFICER
        END AS OFFICER,
        LNAME,
        FNAME,
        -- Null out emails for entries like "Bismarck CS", which sometimes appear
        -- as terminating officers in supervision periods, but are not real people.
        IF(LOGINNAME LIKE '%% %%', NULL, CONCAT(LOWER(LOGINNAME), '@nd.gov')) AS EMAIL,
        RecDate,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(OFFICER AS INT64)
            ORDER BY CAST(RecDate AS DATETIME) DESC) AS recency_rank
    FROM {{docstars_officers}}
    ) sub
WHERE recency_rank = 1
),
-- Choose the most recent available version of the officer's name, in case it has been
-- changed in the nightly raw data but not yet in the manual roster.
{LATEST_NAMES_CTE_NAME} AS (
SELECT DISTINCT
    OFFICER,
    -- If staff_from_directory.RecDate is NULL, the result of the comparison is
    -- unknown and the logic chooses staff_from_docstars.LastName. If
    -- staff_from_docstars.RecDate is NULL, the IFNULL() picks
    -- staff_from_directory.LastName.
    CASE WHEN staff_from_directory.RecDate > IFNULL(staff_from_docstars.RecDate, '1000-01-01')
        THEN staff_from_directory.LastName
        ELSE staff_from_docstars.LastName
    END AS LastName,
    CASE WHEN staff_from_directory.RecDate > IFNULL(staff_from_docstars.RecDate, '1000-01-01')
        THEN staff_from_directory.FirstName
        ELSE staff_from_docstars.FirstName
    END AS FirstName,
    CASE WHEN staff_from_directory.RecDate > IFNULL(staff_from_docstars.RecDate, '1000-01-01')
        THEN staff_from_directory.EMAIL
        ELSE staff_from_docstars.EMAIL
    END AS EMAIL,
FROM staff_from_directory
FULL OUTER JOIN staff_from_docstars
USING(OFFICER)
)
"""
