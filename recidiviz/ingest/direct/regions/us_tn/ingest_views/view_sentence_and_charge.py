#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""
This ingest view hydrates StateChargeV2 and StateSentence, which have a 1-1 relationship in TN and
are identified uniquely by OffenderID, ConvictionCounty, CaseYear, CaseNumber, and CountNumber.

We use the earliest known 'SentencedTo' and 'SentenceStatus' values along with the 'SuspendedToProbation'
value (which does not change over time) to determine the sentence type.

All other data is taken from the latest known values within sentencing data.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This CTE selects the earliest known SentencedTo value for each sentence.
earliest_known_charge_information AS (
    SELECT
      OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber,
      SentencedTo AS earliest_known_sentenced_to
    FROM 
        {JOCharge@ALL}
    QUALIFY 
        ROW_NUMBER() OVER (
            PARTITION BY OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber 
            ORDER BY update_datetime ASC
        ) = 1
),
-- This CTE selects the earliest known SentenceStatus for each sentence.
earliest_known_sentence_information AS (
    SELECT
      OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber,
      SentenceStatus
    FROM 
        {Sentence@ALL}
    QUALIFY 
        ROW_NUMBER() OVER (
            PARTITION BY OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber 
            ORDER BY update_datetime ASC
        ) = 1
),
-- This CTE aggregates all pages and lines of conditions for a single case/count into a single row.
conditions_cte AS (
    SELECT 
        OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber, 
        STRING_AGG(
            SpecialConditions, ' ' 
            ORDER BY CAST(PageNumber AS INT64), CAST(LineNumber AS INT64)
        ) AS ConditionsText
    FROM 
        {JOSpecialConditions}
    GROUP BY OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber
)
SELECT 
    -- These form a unique ID for charges and their sentences.
    latest_charge_info.OffenderID,
    latest_charge_info.ConvictionCounty,
    latest_charge_info.CaseYear,
    latest_charge_info.CaseNumber,
    latest_charge_info.CountNumber,

    -- StateSentence information
    DATE(latest_charge_info.SentenceImposedDate) AS SentenceImposedDate,
    latest_charge_info.SuspendedToProbation,
    jo_sentence.LifeDeathHabitual,
    conditions_cte.ConditionsText,
    earliest_known_charge_information.earliest_known_sentenced_to,
    earliest_known_sentence_information.SentenceStatus AS earliest_known_sentence_status,
    -- These are concatenated to form a unique ID for the parent sentence.
    latest_known_sentence_information.ConsecutiveConvictionCounty,
    latest_known_sentence_information.ConsecutiveCaseYear,
    latest_known_sentence_information.ConsecutiveCaseNumber,
    latest_known_sentence_information.ConsecutiveCountNumber,

    -- StateChargeV2 information
    DATE(latest_charge_info.OffenseDate) AS OffenseDate,
    latest_charge_info.ConvictionOffense,
    latest_charge_info.CrimeType,              -- charge_classification_type
    latest_charge_info.ConvictionClass,        -- classification_subtype
    statute.AssaultiveOffenseFlag,    -- derives is_violent
    statute.SexOffenderFlag,
    statute.OffenseDescription
FROM 
    {JOCharge} AS latest_charge_info
JOIN 
    {OffenderStatute} AS statute
ON
    latest_charge_info.ConvictionOffense = statute.Offense
JOIN
    {JOSentence} AS jo_sentence
USING
    (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
JOIN
    {Sentence} AS latest_known_sentence_information
USING
    (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
JOIN
    earliest_known_sentence_information
USING
    (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
JOIN
    earliest_known_charge_information
USING
    (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
LEFT JOIN
    conditions_cte 
USING
    (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
WHERE 
    -- Only a few cases in the early 90s
    latest_charge_info.SentenceImposedDate IS NOT NULL
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="sentence_and_charge",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
