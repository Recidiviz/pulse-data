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
# =============================================================================
"""Query containing InferredViolations information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This CTE is essentially the ingest view for documented violations direct from the Violations table which 
-- will be used later on to validate/potentially disregard inferred violations when we know a violation was already 
-- recorded in the official documentation process 
documented_violations as 
(SELECT 
  OffenderID,
  TriggerNumber,
  CAST(ContactNoteDate AS DATETIME) as ContactNoteDate,
  ContactNoteType
FROM {Violations}
),
-- Selects all contact notes with type ABSV, ABSR, or ABSW to not count inferred violations that occur up to 60 days after an ABS* ContactNoteType. 
abs_contact_notes AS (
  SELECT 
    OffenderID, 
    ContactNoteType,
    CAST(ContactNoteDateTime AS DATETIME) as ContactNoteDateTime 
  FROM {ContactNoteType} 
  WHERE ContactNoteType LIKE 'ABS%'
),
-- Selects all VWAR and VRPT contact notes and groups the contact notes that occur on the same day, prioritizing VWARs over VRPTs. 
cleaned_contact_note_type AS (
    SELECT 
        OffenderID,
        CAST(ContactNoteDateTime AS DATETIME) as ContactNoteDateTime,
        ContactNoteType
    FROM 
      (SELECT 
        OffenderID,
        ContactNoteDateTime,
        ContactNoteType,
        ROW_NUMBER() OVER (PARTITION BY OffenderID, DATE_TRUNC(CAST(ContactNoteDateTime AS DATETIME), DAY) ORDER BY ContactNoteType DESC, ContactNoteDateTime ASC ) as type_ranking
      FROM {ContactNoteType} c
      WHERE ContactNoteType IN ('VWAR','VRPT')) c
    WHERE type_ranking = 1 
),
-- Selects all SORV and CSLR contact notes and groups each of the contact notes types that occur on the same day
contact_note_type_SORV_CSLR AS (
    SELECT 
        OffenderID,
        CAST(ContactNoteDateTime AS DATETIME) as ContactNoteDateTime,
        ContactNoteType,
    FROM ContactNoteType_generated_view c
    WHERE ContactNoteType IN ('SORV','CSLR') 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY OffenderID, ContactNoteType, DATE_TRUNC(CAST(ContactNoteDateTime AS DATETIME), DAY) ORDER BY ContactNoteDateTime ASC ) = 1

),
-- Assuming that if a VWAR and VRPT (or multiple of either) are filed within 30 days of each other, they are for a 
-- related violation not for separate violations, so we assign them this flag to help with grouping in subsequent CTEs 
ordered_vwars_vrpts AS (
SELECT 
  *, 
  ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY ContactNoteDateTime) as sequence,
  CASE 
    WHEN ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY ContactNoteDateTime) = 1 THEN 'first_event'
    WHEN DATE_DIFF(ContactNoteDateTime, LAG(ContactNoteDateTime) OVER (PARTITION BY OffenderID ORDER BY ContactNoteDateTime ASC), DAY) < 31 THEN 'less_than_30_days'
    ELSE 'likely_unrelated' END AS time_between_this_and_previous_event
FROM cleaned_contact_note_type
),
-- Nulls out sequence number for events that should be grouped with the previous event
identifying_likely_related_events AS (
    SELECT 
        OffenderID,
        ContactNoteDateTime,
        ContactNoteType,
        sequence,
        time_between_this_and_previous_event,
        CASE 
            WHEN time_between_this_and_previous_event = 'less_than_30_days' THEN NULL
            ELSE sequence END AS sequence_with_groupings_for_related_events
    FROM ordered_vwars_vrpts
),
-- Here, we are looking for any sequence_with_groupings_for_related_events that are null from the previous 
-- CTE which means they should have the sequence value of the first event in the grouped event and then assigning that sequenvce value 
grouping_likely_related_events AS (
  SELECT 
    OffenderID,
        ContactNoteDateTime,
        ContactNoteType,
        sequence,
        time_between_this_and_previous_event,
        CASE    
            WHEN sequence_with_groupings_for_related_events is NULL THEN LAST_VALUE(sequence_with_groupings_for_related_events IGNORE NULLS) OVER (PARTITION BY OffenderID ORDER BY ContactNoteDateTime ASC) 
            ELSE sequence_with_groupings_for_related_events END AS sequence_with_groupings_for_related_events
  FROM identifying_likely_related_events 
),
-- Having min and max contact date allows us to join later on within 31 days before or after either the min or max date 
-- of an inferred violation. (So for example, if an inferred violation has a VWAR on DAY 1, a VRPT on DAY 28, we are 
-- checking if there are any documented violations within 31 days of day 1 OR day 28)
all_inferred_violations AS (
SELECT 
  OffenderID,
  'INFERRED' AS TriggerNumber,
  CAST(MIN(ContactNoteDateTime) AS DATETIME) as ContactNoteDate_min, 
  CAST(MAX(ContactNoteDateTime) AS DATETIME) as 
  ContactNoteDate_max,
  TO_JSON_STRING(
                  ARRAY_AGG(STRUCT<OffenderID string,
                                  ContactNoteDateTime datetime,
                                  ContactNoteType string,
                                  sequence string,
                                  time_between_this_and_previous_event string,
                                  sequence_with_groupings_for_related_events string>
                          (OffenderID,
                          ContactNoteDateTime,
                          ContactNoteType,
                          CAST(sequence AS STRING),
                          time_between_this_and_previous_event,
                          CAST(sequence_with_groupings_for_related_events AS STRING)) ORDER BY OffenderID, ContactNoteDateTime)
              ) as response_info,
   'INFERRED' AS ContactNoteType
FROM grouping_likely_related_events
GROUP BY OffenderID, sequence_with_groupings_for_related_events
),
-- Making null placeholders so we can join SORV and CSLR to the other violations format
-- We have to make sequence the contactnotetype since we aren't applying the sequence logic
-- to SORV/CSLR and one of each could happen on the same day and then they would have the 
-- same external id for StateSupervisionViolationResponse
 SORV_and_CSLR_info AS (
  SELECT *, 
    CASE WHEN ContactNoteType = "SORV" THEN "SORV"
        WHEN ContactNoteType = "CSLR" THEN "CSLR"
     ELSE null
    END AS sequence, 
    CASE 
      WHEN ContactNoteType = "CSLR" THEN "CSLR"
      WHEN ContactNoteType = "SORV" THEN "SORV"
      ELSE "INFERRED"
    END AS TriggerNumber,
    null AS time_between_this_and_previous_event,
    null AS sequence_with_groupings_for_related_events
  FROM  contact_note_type_SORV_CSLR
)
-- NOTE: If TN begins adding CSLR and SORV to Violations we will want to add them earlier
-- in the logic to avoid double counting, but they are currently not added to Violations 
-- table by design and we do not arbitrarily want to apply the same logic to SORV/CSLR 
-- as we do for the VWARS/VRPTS
SELECT 
    aiv.OffenderID, 
    aiv.TriggerNumber,
    ContactNoteDate_min as ContactNoteDate, # We keep the oldest contactnotedate to understand when responses for violation first began,
    response_info,
    aiv.ContactNoteType
FROM all_inferred_violations aiv
LEFT JOIN documented_violations dv ON aiv.OffenderID=dv.OffenderID 
  AND (DATE_DIFF(aiv.ContactNoteDate_min, dv.ContactNoteDate, DAY) BETWEEN -31 AND 31 OR DATE_DIFF(aiv.ContactNoteDate_max, dv.ContactNoteDate, DAY) BETWEEN -31 AND 31)
LEFT JOIN abs_contact_notes acn ON aiv.OffenderID=acn.OffenderID 
  AND (DATE_DIFF(aiv.ContactNoteDate_min, acn.ContactNoteDateTime, DAY) BETWEEN 0 and 90 OR DATE_DIFF(aiv.ContactNoteDate_max, acn.ContactNoteDateTime, DAY) BETWEEN 0 and 90)
WHERE (dv.TriggerNumber is null AND acn.OffenderID is null) 
    # This ensures we only bring in violations that do not have 
    # any Violations + Sanctions from documented_violations tables in TOMIS or do not have an ABS* code 90 days before the inferred violation 

UNION ALL

SELECT DISTINCT
  OffenderID,
  -- we have to make the triggerNumber the ContactNoteType for these since with the current 
  -- methodology we see some CSLR and SORV codes being entered with the same datetime
  TriggerNumber,
  ContactNoteDateTime AS ContactNoteDate,
  TO_JSON_STRING(
                  ARRAY_AGG(STRUCT<OffenderID string,
                                  ContactNoteDateTime datetime,
                                  ContactNoteType string,
                                  sequence string,
                                  time_between_this_and_previous_event string,
                                  sequence_with_groupings_for_related_events string>
                          (OffenderID,
                          ContactNoteDateTime,
                          ContactNoteType,
                          CAST(sequence AS STRING),
                          CAST(time_between_this_and_previous_event AS STRING),
                          CAST(sequence_with_groupings_for_related_events AS STRING)) ORDER BY OffenderID, ContactNoteDateTime)
              ) as response_info,

  ContactNoteType
  FROM SORV_and_CSLR_info
  GROUP BY OffenderID, TriggerNumber, ContactNoteDate,ContactNoteType
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="InferredViolations",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
