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
"""Query containing state person information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Returns the court or legal name from the most recently updated record for each person
-- TODO(#35794): Confirm which name types we should select here
names AS (
    SELECT
        ofndr_num,
        TRIM(fname) AS fname,
        TRIM(mname) AS mname,
        TRIM(lname) AS lname,
    FROM {ofndr_name}
    WHERE name_typ_cd IN ('C', 'L') -- C: Court, L: Legal
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ofndr_num
        -- prioritize most recent record, then sort by primary key to handle null update dates and make this deterministic
        ORDER BY
            CAST(updt_dt AS DATETIME) DESC NULLS LAST,
            ofndr_name_id DESC
    ) = 1
),
-- Returns the DOB from the most recently updated record for each person
date_of_birth AS (
    SELECT
        ofndr_num,
        CAST(dob AS DATE) AS dob,
    FROM {ofndr_dob}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ofndr_num
        -- prioritize most recent record, then sort by primary key to handle null update dates and make this deterministic
        ORDER BY
            CAST(updt_dt AS DATETIME) DESC NULLS LAST,
            ofndr_dob_id DESC
    ) = 1
),
-- Returns the email address from the most recently updated record for each person,
-- prioritizing home email addresses over other emails
-- To get current emails, we limit to records with a valid start date and NULL end date
emails AS (
    SELECT
        ofndr_num,
        TRIM(email_addr) AS email_addr,
    FROM {ofndr_email}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ofndr_num
        -- prioritize home emails, then most recent record, then sort by primary key
        -- to handle null update dates and make this deterministic
        ORDER BY
            CASE WHEN email_typ_cd = 'H' THEN 0 ELSE 1 END ASC,
            CAST(updt_dt AS DATETIME) DESC NULLS LAST,
            id DESC
    ) = 1
),
-- Returns the phone number from the most recently updated record for each person
-- To get current numbers, we limit to records with a non-null start date and null end date
-- TODO(#35794): Confirm which phone types we should select here
phones AS (
    SELECT
        ofndr_num,
        TRIM(area_cd) || REPLACE(TRIM(phone_num),'-','') AS phone_num,
    FROM {ofndr_phone}
    WHERE phone_typ_cd IN ('C', 'H') -- C: Cell, H: Home
        -- basic validity checks
        AND REGEXP_CONTAINS(TRIM(area_cd), r'^\\d\\d\\d$')
        AND TRIM(area_cd) != '000'

        AND REGEXP_CONTAINS(TRIM(phone_num), r'^\\d\\d\\d-?\\d\\d\\d\\d$')
        AND LEFT(TRIM(phone_num), 3) != '000'

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ofndr_num
        -- prioritize cell phones, then most recent record, then sort by primary key
        -- to handle null update dates and make this deterministic
        ORDER BY
            CASE WHEN phone_typ_cd = 'C' THEN 0 ELSE 1 END ASC,
            CAST(updt_dt AS DATETIME) DESC NULLS LAST,
            id DESC
    ) = 1
)

-- Join all previous CTEs onto primary person table
SELECT
    ofndr_num,
    TRIM(race_cd) AS race_cd,
    TRIM(ethnic_cd_ncic) AS ethnic_cd_ncic,
    TRIM(sex) AS sex,
    dob,
    email_addr,
    fname,
    mname,
    lname,
    phone_num,
FROM {ofndr}
LEFT JOIN date_of_birth
    USING (ofndr_num)
LEFT JOIN emails
    USING (ofndr_num)
LEFT JOIN names
    USING (ofndr_num)
LEFT JOIN phones
    USING (ofndr_num)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="state_person",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
