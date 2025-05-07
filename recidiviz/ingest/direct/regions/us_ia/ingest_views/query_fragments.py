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
"""Shared helper fragments for the US_IA ingest view queries."""

# Query fragments that rely only on raw data tables

# List of charge end reasons that indicate a charge was invalid.  We exclude charges with
# these end reasons when ingest StateChargeV2.
INVALID_CHARGE_END_REASONS = """
    "Pre Trial Interview - No Supervision Imposed",
    "Duplicate Charge",
    "Merged with another charge",
    "Acquitted",
    "Found Not Guilty",
    "Dismissed"
"""

PENALTIES_TO_KEEP = f"""
-- This CTE returns the penalty is only for the penalties we're interested in
-- by filtering out penalties associated with charges that were dismissed
-- and sentences without a date imposed (currently there is only one record that gets
-- excluded because of this)
    penalties_to_keep AS (
      SELECT PenaltyId, ChargeId
      FROM {{IA_DOC_Penalities}} penalty
      INNER JOIN {{IA_DOC_Sentences}} sentence USING(OffenderCd, SentenceId)
      -- TODO(#38328) I think there are only null charge ids right now because we don't 
      -- have daily transfers yet, but to check again when we do get daily transfers
      INNER JOIN {{IA_DOC_Charges}} charge USING(OffenderCd, ChargeId)
      WHERE (
            charge.EndReason IS NULL OR 
            charge.EndReason NOT IN (
                {INVALID_CHARGE_END_REASONS}
            )
        )
        -- We can't have sentences without date imposed and with sentence dates before 1900-1-2,
        -- and there are only four sentences that fall in this category so let's just drop
        AND sentence.SentenceDt IS NOT NULL AND DATE(sentence.SentenceDt) >= DATE(1900,1,2)
)"""


# Supervision Violations Parole/Probation CTE

FIELD_RULE_VIOLATIONS_CTE = """
-- Part One: Field Rule Violations and Instances, including parole and probation.
FRV_I as
 (
    select
        FRVI.active as FRVI_active


        , OffenderCd
        , (case when cast(FRVI.IncidentDt as datetime)<CAST('1900-01-02' as datetime) or CURRENT_DATE < cast(FRVI.IncidentDt as datetime) then null else cast(IncidentDt as datetime) end) as IncidentDt

        -- ID's
        , FRVI.FieldRuleViolationIncidentId

        -- To use for response date
        , CAST(FRVI.EnteredDt AS DATETIME) as EnteredDt
    from
        {IA_DOC_FieldRuleViolationIncidents} as FRVI
    -- We want to keep all incidents regardless of active status because active just connotes if a decision has been finalized.
 )
"""
