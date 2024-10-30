# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Null out placeholder staff emails before deduping in the next step.
staff_cleaned_emails as (
  SELECT DISTINCT 
    PARTYID,
    PERSONLASTNAME,
    PERSONFIRSTNAME,
    PERSONMIDDLENAME,
    PERSONSUFFIX,
    -- The following patterns show up in placeholder emails, which are often shared among
    -- several staff members and therefore get nulled out in this CTE.
    CASE 
      WHEN 
        PARTYEMAILADDR IS NULL OR
        UPPER(PARTYEMAILADDR) LIKE "NOEMAIL@%" OR
        UPPER(PARTYEMAILADDR) LIKE "NO.EMAIL@%" OR
        UPPER(PARTYEMAILADDR) LIKE "NOMAIL@%" OR
        UPPER(PARTYEMAILADDR) LIKE "NO.MAIL@%" OR
        UPPER(PARTYEMAILADDR) LIKE "UNKNOWN@%" OR
        UPPER(PARTYEMAILADDR) LIKE "NONE@%" OR
        UPPER(PARTYEMAILADDR) LIKE "X@%" OR
        UPPER(PARTYEMAILADDR) LIKE "NO@%" OR
        PARTYEMAILADDR LIKE "@%" OR
        PARTYEMAILADDR LIKE "%?%" OR 
        PARTYEMAILADDR NOT LIKE "%@%" 
      -- Placeholder emails and missing emails are set to "no_email", which is replaced
      -- with NULL in the final step. They can't be set to null here because the email
      -- itself is part of a join condition in this view.
      THEN "no_email" 
      ELSE REGEXP_REPLACE(
          PARTYEMAILADDR,
          r"[\\s(),:;<>[\\]\\\\]",
          ""
      )
    END AS PARTYEMAILADDR,

    -- These columns are pulled through so they can be used for deduplication in the next step. 
    person.DATELASTUPDATE,
    person.TIMELASTUPDATE
  FROM {PERSONPROFILE} person
  LEFT JOIN {RELATEDPARTY} rel_party
  USING(PARTYID)
  LEFT JOIN {PARTYPROFILE} party
  USING(PARTYID)
  -- This logic is used in most AR ingest views that include person/staff IDs, since some
  -- older data has a handful of unusable rows with XML tags in the ID columns.
  WHERE REGEXP_CONTAINS(PARTYID, r'^[[:alnum:]]+$') 
  AND
  /*
  Only ingest data from PERSONPROFILE if the person has had a role within ADC/ACC, since 
  PERSONPROFILE includes many people in external positions who don't need to be ingested. 
  We use SELECT DISTINCT in this view because people could have multiple rows of staff-type 
  relationships in RELATEDPARTY, resulting in data getting duplicated in the join.
  This set of allowed relationships may need to be expanded over time if we end up wanting
  data for people in other roles (such as court figures or external programming providers).
  */
  (
    PARTYRELTYPE IN (
      '1AA', -- ADC Employee
      '1AB', -- ADC Employee/Extra Help
      '1AC', -- ACSD CTE
      '1AE', -- DOC Employee
      '1AI', -- IFI
      '1AP', -- PIE
      '1BB', -- Board of Corrections
      '1CA', -- Private Prison Staff
      '2AA', -- ACC Employee
      '2BB', -- Parole Board
      '3BO', -- City Jail Employee
      '3BP', -- Police Department Employee
      '3BS', -- Sheriff Department Employee
      '3BT' -- County Jail Employee
    ) OR 
    -- Some staff IDs are specified in other tables, and even if people with these IDs don't
    -- have valid staff-type roles in RELATEDPARTY, we want to make sure they still get ingested
    -- as staff.
    PARTYID IN (
      SELECT DISTINCT PPOFFICERID
      FROM {SUPERVISIONEVENT}
      UNION DISTINCT
      SELECT WORKASSIGNMENTAUTHBY
      FROM {JOBPROGRAMASGMT}
      UNION DISTINCT
      SELECT STAFFIDPERFASSESS
      FROM {RISKNEEDCLASS}
    )
  )
),
-- For each set of identifying information (name + email), get an array of all the staff IDs
-- associated with those values. The next step will deduplicate so that each email is unique
-- to a single staff ID, but we need to preserve the other IDs that were associated with it.
identifying_info_to_id_array AS (
    SELECT 
        PARTYEMAILADDR,
        PERSONFIRSTNAME,
        PERSONLASTNAME,
        STRING_AGG(PARTYID, ',') AS staff_id_array,
    FROM (
        SELECT DISTINCT 
            PARTYEMAILADDR,
            PERSONFIRSTNAME,
            PERSONLASTNAME,
            PARTYID
        FROM staff_cleaned_emails
    ) GROUP BY 1,2,3
)
-- If multiple staff entries share a first name, last name, and email address, then keep
-- whichever record (including the staff ID) was updated the most recently. After this deduping,
-- and the handling of placeholder emails in the previous step, around 65 duplicated emails 
-- remain. These simply get nulled out, so that we're ultimately left with no emails shared
-- across multiple staff members.

SELECT 
    *
    EXCEPT(PARTYEMAILADDR),
    NULLIF(PARTYEMAILADDR,"no_email") AS PARTYEMAILADDR 
FROM (
    SELECT 
    id_array.staff_id_array,
    deduped_staff.PERSONLASTNAME,
    deduped_staff.PERSONFIRSTNAME,
    deduped_staff.PERSONMIDDLENAME,
    deduped_staff.PERSONSUFFIX,
    CASE 
        WHEN COUNT(DISTINCT deduped_staff.PARTYID) OVER (PARTITION BY deduped_staff.PARTYEMAILADDR) > 1
        THEN "no_email"
        ELSE deduped_staff.PARTYEMAILADDR 
    END AS PARTYEMAILADDR
    FROM (
        SELECT 
            * 
        FROM (
            SELECT * 
            FROM staff_cleaned_emails
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY PARTYEMAILADDR,PERSONFIRSTNAME,PERSONLASTNAME 
                ORDER BY DATELASTUPDATE DESC,TIMELASTUPDATE DESC
            ) = 1
        )
    ) deduped_staff
    LEFT JOIN identifying_info_to_id_array id_array
    USING(
        PERSONFIRSTNAME,
        PERSONLASTNAME,
        PARTYEMAILADDR
    ) 
)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar", ingest_view_name="staff", view_query_template=VIEW_QUERY_TEMPLATE
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
