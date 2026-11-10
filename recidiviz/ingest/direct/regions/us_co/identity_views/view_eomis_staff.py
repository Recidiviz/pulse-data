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
"""Identity ingest view for US_CO eomis staff identity information.

One row per DOC-badge-holding party record: the party's name, birthdate, and sex
from eomis_personprofile and email from eomis_partyprofile. The party tables hold
every person and organization eomis tracks, not just staff, so holding a DOC
badge is what defines the staff roster here, the same roster rule the activity
StateStaff view uses; parties without a badge are not staff and emit no
fragment.

Each fragment carries only the party's own US_CO_PARTYID, satisfying the
one-row-per-person requirement on identity views (see
FilterSentinelExternalIds) even though a staff member who re-enters the system
gets a new PARTYID. The companion eomis_staff_badges view links each badge to
all of its PARTYIDs, so clustering merges a re-entering staff member's party
fragments through the shared badge.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = r"""
WITH
-- Parties holding a DOC badge (PTYIDTYPE '35', agency CO): the staff roster.
badge_holders AS (
    SELECT DISTINCT PARTYID
    FROM {eomis_ptyotherid}
    WHERE PTYIDTYPE = '35'
        AND PTYIDAGENCY = 'CO'
),
-- Party email addresses with placeholder addresses and non-state addresses
-- NULLed out, matching the activity StateStaff view's filters; the mapping
-- validates the format of what survives. The comparisons lowercase inline so
-- the emitted address keeps its source casing: case normalization belongs at
-- comparison time, not in ingested values.
party_emails AS (
    SELECT
        PARTYID,
        CASE
            WHEN REGEXP_CONTAINS(LOWER(email), r'^(x+@[\w-]+\.[\w.-]+)$') THEN NULL
            WHEN REGEXP_CONTAINS(LOWER(email), r'^((unknown|none|noname|nobody)@[\w-]+\.[\w.-]+)$') THEN NULL
            WHEN LOWER(email) LIKE '%state.co.us' THEN email
            ELSE NULL
        END AS EMAIL
    FROM (
        SELECT PARTYID, TRIM(PARTYEMAILADDR) AS email
        FROM {eomis_partyprofile}
    )
)
SELECT
    person.PARTYID,
    person.PERSONFIRSTNAME AS FIRST_NAME,
    person.PERSONMIDDLENAME AS MIDDLE_NAME,
    person.PERSONLASTNAME AS LAST_NAME,
    person.PERSONSUFFIX AS NAME_SUFFIX,
    -- The raw datetime column surfaces as a datetime string; only the date part
    -- is a birthdate. The mapping omits out-of-bounds birthdates via
    -- reasonable_birthdate_or_null. Carrying the staff birthdate matters beyond
    -- display: a matching birthdate is what lets the conflict checker excuse a
    -- surname change (marriage/rename) across a staff member's party records
    -- instead of rejecting the cluster.
    CAST(CAST(person.PERSONDATEOFBIRTH AS DATETIME) AS DATE) AS BIRTH_DATE,
    person.PERSONSEX AS SEX_CODE,
    party_emails.EMAIL
FROM badge_holders
JOIN {eomis_personprofile} person
    USING (PARTYID)
JOIN party_emails
    USING (PARTYID)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_co",
    ingest_view_name="eomis_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
