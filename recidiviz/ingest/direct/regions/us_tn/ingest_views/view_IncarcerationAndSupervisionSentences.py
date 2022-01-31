#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
"""This query pulls in from a number of different sentencing tables in TN to produce one row per sentence. It
includes both supervision and incarceration sentences, as well as pulls in external identifiers other sentences the
sentence may be consecutive to.
"""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH order_sentence_actions_by_date_per_sentence AS (
    SELECT 
        OffenderID, 
        ConvictionCounty, 
        CaseYear, 
        CaseNumber, 
        CountNumber,
        SentenceAction as MostRecentSentenceAction
    FROM {SentenceAction} 
    -- Only pull in the most recent sentence action associated with a sentence.
    WHERE TRUE
    QUALIFY ROW_NUMBER() OVER ( PARTITION BY OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber ORDER BY ActionDate DESC ) = 1
)
SELECT
    OffenderID,
    Sentence.ConvictionCounty,
    Sentence.CaseYear,
    Sentence.CaseNumber,
    Sentence.CountNumber,
    MostRecentSentenceAction,
    Sentence.SentenceStatus,
    JOCharge.SuspendedToProbation,
    Sentence.SentenceEffectiveDate,
    Sentence.EarliestPossibleReleaseDate,
    Sentence.FullExpirationDate,
    Sentence.ExpirationDate,
    DATE_DIFF(DATE(Sentence.EarliestPossibleReleaseDate), DATE(Sentence.SentenceEffectiveDate), DAY) as CalculatedMinimumSentenceDays,
    DATE_DIFF(DATE(Sentence.FullExpirationDate), DATE(Sentence.SentenceEffectiveDate), DAY) as CalculatedMaximumSentenceDays, 
    JOSentence.LifeDeathHabitual,
    -- The most accurate consecutive sentence information can be found in the `Sentence` table.
    Sentence.ConsecutiveConvictionCounty,
    Sentence.ConsecutiveCaseYear,
    Sentence.ConsecutiveCaseNumber,
    Sentence.ConsecutiveCountNumber,
FROM {Sentence} Sentence
LEFT JOIN order_sentence_actions_by_date_per_sentence
USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
LEFT JOIN {JOSentence} JOSentence
USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
LEFT JOIN {JOCharge} JOCharge
USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
LEFT JOIN  {SentenceMiscellaneous} SentenceMisc
USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_tn",
    ingest_view_name="IncarcerationAndSupervisionSentences",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderID ASC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
