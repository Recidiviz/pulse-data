# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Ingest view query for US_IX parole early discharges"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# In US_IX, there's a two part process for parole early discharge requests.
# First there's the "Review" step where the parole board decides whether to allow a hearing for the case.
# If the parole board grants the hearing request, then the second step is the actual "Hearing" where early discharge is decided.
#
# In Atlas, info about parole early discharge is stored in the parole board tables, where
# we have information on each parole board case (prb_PBCase), each parole board meeting (bop_BOPHearing) which is either "Review" or "Hearing",
# and each parole board meeting vote decision (bop_BOPVoteResult).  A single parole board case can
# have multiple meetings, and each meeting can have multiple vote decisions.  In addition, a single petition
# for early discharge from parole seems to be able to span multiple parole board cases.  For example, the "Review"
# step is often one parole board case, and if that is granted, then another parole board case is recorded for the "Hearing" step.
# Furthermore, the "Review" and "Hearing" steps can also have multiple vote decisions when the parole board decides to delay their decision
# (so you'll see a first vote decision of "Continue" and then a second vote decision of either "Grant" or "Deny").
# For our purposes, we're going to count the "Review" step and the "Hearing" step together as a single early discharge request.
# So the possible trajectories of a single early discharge request could be any of the following:
#  - Review -> Deny
#  - Review -> Grant, Hearing -> Deny
#  - Review -> Grant, Hearing -> Grant
#  - Review -> Continue, Review -> Deny
#  - Review -> Continue, Review -> Grant, Hearing -> Grant
#  - etc.
# There are some other additional vote values but are infrequent and not as relevant for our purposes

VIEW_QUERY_TEMPLATEp = """{prb_PBCase}"""

VIEW_QUERY_TEMPLATE = """
WITH 
-- compile all relevant info so that each row represents either a single parole board meeting with a single vote decision
-- (or no vote decision if the meeting was recorded but not vote decision was recorded)
all_relevant_info as (
  SELECT 
    cases.OffenderId,
    cases.TermId,
    cases.PBCaseId,
    cases.InsertDate as Case_InsertDate,
    hearing_type.BOPHearingTypeName,
    hearing.BOPDocketId,
    vote_result.BOPVoteResultId,
    vote_result.UpdateDate as Vote_UpdateDate,
    vote_value.BOPVoteValueName
  FROM {prb_PBCase} cases
  LEFT JOIN {bop_BOPHearing} hearing
    USING(PBCaseId)
  LEFT JOIN {bop_BOPHearingType} hearing_type 
    USING(BOPHearingTypeId)
  LEFT JOIN {prb_PBCaseSubType} casesub 
    USING(PBCaseSubTypeId)
  LEFT JOIN {bop_BOPVoteResult} vote_result 
    USING(PBCaseId,BOPDocketId)
  LEFT JOIN {bop_BOPVoteValue} vote_value 
    USING(BOPVoteValueId)
  WHERE 
    PBCaseSubTypeDesc = 'Commutation'
    and BOPHearingTypeName in ('Hearing', 'Review')
    and TermId is not null
),
all_relevant_info_w_request_groupings as (
  -- For each person, parole board meetings by order of occurrence, and then delineate each set of meetings for an individual early discharge request by identifying the first parole board case id and the final vote decision for each request.
  -- We'll define the final vote decision recorded for a single early discharge request as a "Grant" vote from a Hearing OR a "Deny"/"Reject Request" vote from either a Hearing or Review.
  -- We'll define the first parole board case recorded for a single early discharge request as the earliest entered parole board case for a person OR the first new parole board case entered following a final decision.
  -- Then, we'll label all parole board meetings with vote decisions within the same early discharge request with the same first parole board case id and the same final vote decision id (which could be null if no final decision has been made)

  SELECT *,
    FIRST_VALUE(PBCaseId IGNORE NULLS)
      OVER (PARTITION BY final_BOPVoteResultId
            ORDER BY CAST(Case_InsertDate as DATETIME), CAST(Vote_UpdateDate as DATETIME) NULLS FIRST
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as first_PBCaseId
  FROM (
    SELECT *,
        FIRST_VALUE(final_decision_vote_id IGNORE NULLS)
        OVER (PARTITION BY OffenderId, TermId 
                ORDER BY CAST(Case_InsertDate as DATETIME), CAST(Vote_UpdateDate as DATETIME) NULLS FIRST
                RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as final_BOPVoteResultId
        FROM (
        SELECT *,
            CASE WHEN ((BOPHearingTypeName = 'Hearing' and BOPVoteValueName = 'Grant') or BOPVoteValueName in ('Deny', 'Reject Request')) 
                THEN BOPVoteResultId
                ELSE NULL
            END as final_decision_vote_id
        FROM all_relevant_info
        ) sub1 
  ) sub2
),
all_requests as (
  -- For each set of rows associated with each early discharge request, collapse the rows together and
  -- take the earliest parole board case record insert date as the request date and the latest vote decision date as the decision date
  SELECT
    OffenderId,
    first_PBCaseId,
    final_BOPVoteResultId,
    MIN(Case_InsertDate) as request_date,
    MAX(Vote_UpdateDate) as decision_date,
  FROM all_relevant_info_w_request_groupings
  GROUP BY 1,2,3
)
-- finally, if there was no final decision for this request (aka final_BOPVoteResultId is null), null out the decision date
SELECT 
  OffenderId,
  first_PBCaseId,
  (DATE(request_date)) as request_date,
  case when final_BOPVoteResultId is null then null else (DATE(decision_date)) end as decision_date,
  BOPVoteValueName
FROM all_requests
LEFT JOIN {bop_BOPVoteResult} vote_result 
  on vote_result.BOPVoteResultId = final_BOPVoteResultId
LEFT JOIN {bop_BOPVoteValue} vote_value 
  USING(BOPVoteValueId)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="early_discharge_parole",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
