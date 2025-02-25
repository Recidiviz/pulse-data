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
"""This query pulls in from a number of different sentencing, statute, and charge tables in TN to produce one row per
sentence, charge, and court case. It includes both supervision and incarceration sentences, and pulls in external
identifiers other sentences the sentence may be consecutive to. It also creates a single charge object for each
sentence row, as well as a single court case with a single agent (judge).

"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    UPDATE_DATETIME_PARAM_NAME,
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH order_sentence_actions_by_date_per_sentence AS (
    SELECT 
        OrderedSentenceAction.OffenderID, 
        OrderedSentenceAction.ConvictionCounty, 
        OrderedSentenceAction.CaseYear, 
        OrderedSentenceAction.CaseNumber, 
        OrderedSentenceAction.CountNumber,
        OrderedSentenceAction.SentenceAction as MostRecentSentenceAction
    FROM (
        SELECT 
            OrderedSentenceAction.*,
            ROW_NUMBER() OVER (
                PARTITION BY OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber
                ORDER BY ActionDate DESC, CAST(SequenceNumber AS INT64) DESC
            ) as SentenceActionNumber
        FROM {{SentenceAction}} OrderedSentenceAction
    ) OrderedSentenceAction
    WHERE SentenceActionNumber = 1
),
special_conditions_date_grouping AS (
    SELECT 
        OffenderID,
        ConvictionCounty,
        CaseYear,
        CaseNumber,
        CountNumber,
        # Escape quotes for correct parsing for string aggregation and array creation
        STRING_AGG( REPLACE(SpecialConditions, '"', '\"'), ' ' ORDER BY CAST(PageNumber AS INT64) ASC, CAST(LineNumber AS INT64) ASC) as ConditionsOnDate,
        DATE_TRUNC(CAST(LastUpdateDate as DATETIME), DAY) as NoteUpdateDate
    FROM {{JOSpecialConditions}}
    GROUP BY OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber, NoteUpdateDate
),
special_conditions_aggregation AS (
    SELECT
        OffenderID,
        ConvictionCounty,
        CaseYear,
        CaseNumber,
        CountNumber,
        # Include underscores for column names when converting to JSON for better readability 
        TO_JSON_STRING(
            ARRAY_AGG(STRUCT<note_update_date DATETIME, conditions_on_date string>(NoteUpdateDate,ConditionsOnDate) ORDER BY NoteUpdateDate)
        ) AS Conditions
    FROM special_conditions_date_grouping 
    WHERE ConditionsOnDate is not NULL
    GROUP BY OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber
),
cleaned_Sentence_view AS (
    SELECT 
        Sentence.OffenderID AS OffenderID,
        Sentence.ConvictionCounty AS ConvictionCounty,
        Sentence.CaseYear AS CaseYear,
        # There was the same case number with both one and two spaces being treated as seperate cases when they are the same
        REGEXP_REPLACE(Sentence.CaseNumber, '  ', ' ') AS CaseNumber, 
        Sentence.CountNumber AS CountNumber,
        JOCharge.OffenseDate AS OffenseDate,
        JOCharge.ConvictionOffense AS ConvictionOffense,
        OffenderStatute.OffenseDescription as OffenseDescription,
        Sentence.SentenceStatus AS SentenceStatus,
        Sentence.SentenceEffectiveDate AS SentenceEffectiveDate,
        IF(SentenceMisc.AlternateSentenceImposeDate is not null, SentenceMisc.AlternateSentenceImposeDate, JOCharge.SentenceImposedDate) as SentenceImposeDate,
        Sentence.EarliestPossibleReleaseDate AS EarliestPossibleReleaseDate,
        Sentence.FullExpirationDate AS FullExpirationDate,
        Sentence.ExpirationDate AS ExpirationDate,
        Sentence.LastUpdateDate,
        OffenderStatute.AssaultiveOffenseFlag as AssaultiveOffenseFlag,
        OffenderStatute.SexOffenderFlag as SexOffenderFlag,
        Sentence.ConsecutiveConvictionCounty AS ConsecutiveConvictionCounty,
        Sentence.ConsecutiveCaseYear AS ConsecutiveCaseYear,
        Sentence.ConsecutiveCaseNumber AS ConsecutiveCaseNumber,
        Sentence.ConsecutiveCountNumber AS ConsecutiveCountNumber,
        CAST(NULL as STRING) as ISCSentencyType,
        CASE 
            WHEN JOSentence.LifeDeathHabitual IS NOT NULL THEN JOSentence.LifeDeathHabitual 
            WHEN JOSentence.LifetimeSupervision IS NOT NULL THEN JOSentence.LifetimeSupervision
            ELSE NULL END AS lifetime_flag,
        Sentence.ReleaseEligibilityDate as ReleaseEligibilityDate,
        'SENTENCE' AS sentence_source,
    FROM {{Sentence@ALL}} Sentence
    LEFT JOIN {{JOCharge}} JOCharge
    USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
    LEFT JOIN {{JOSentence}} JOSentence
    USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
    LEFT JOIN {{OffenderStatute}} OffenderStatute
    ON JOCharge.ConvictionOffense = OffenderStatute.Offense
    LEFT JOIN  {{SentenceMiscellaneous}} SentenceMisc
    USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
),
cleaned_Diversion_view AS (
    SELECT 
        Diversion.OffenderID AS OffenderID,
        Diversion.ConvictionCounty AS ConvictionCounty,
        Diversion.CaseYear AS CaseYear,
        # There was the same case number with both one and two spaces being treated as seperate cases when they are the same
        REGEXP_REPLACE(Diversion.CaseNumber, '  ', ' ') AS CaseNumber,
        Diversion.CountNumber AS CountNumber,
        DiversionGrantedDate AS OffenseDate,
        Offense AS ConvictionOffense, 
        OffenseDescription as OffenseDescription,
        CASE WHEN DiversionStatus = 'C' THEN 'IN' ELSE 'AC' END AS SentenceStatus,
        DiversionGrantedDate as SentenceEffectiveDate,
        DiversionGrantedDate as SentenceImposeDate,
        ExpirationDate as EarliestPossibleReleaseDate,
        ExpirationDate as FullExpirationDate,
        ExpirationDate as ExpirationDate,
        Diversion.LastUpdateDate,
        OffenderStatute.AssaultiveOffenseFlag as AssaultiveOffenseFlag,
        OffenderStatute.SexOffenderFlag as SexOffenderFlag,
        CAST(NULL as STRING) AS ConsecutiveConvictionCounty,
        CAST(NULL as STRING) AS ConsecutiveCaseYear,
        CAST(NULL as STRING) AS ConsecutiveCaseNumber,
        CAST(NULL as STRING) AS ConsecutiveCountNumber,
        CAST(NULL as STRING) as ISCSentencyType,
        CAST(NULL as STRING) as lifetime_flag,
        CAST(NULL as STRING) as ReleaseEligibilityDate,
        'DIVERSION' AS sentence_source
    FROM {{Diversion@ALL}} Diversion
    LEFT JOIN {{OffenderStatute}} OffenderStatute USING (Offense)
),
# TODO(#17454) - Note: Both this CTE and the following were created as temporary selection fixes for some strange behavior we note in the ISCRelatedSentence table. 
# In these cases, there were a small number of ISC cases that were consecutive to/related to multiple count numbers of the same case number in the ISCRelatedSentence table. 
# For now, we apply a row number selection and keep the lowest count number for a given related sentence. 
ISCRelated_sentence_count_selection AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY OffenderID,RelatedJurisidicationCounty,RelatedCaseYear, RelatedCaseNumber ORDER BY RelatedCountNumber ASC) AS count_rank
    FROM {{ISCRelatedSentence}}
    WHERE RelatedSentenceType = 'X'
),
# TODO(#17454) - Similar to the above note, there were 2 instances of ISC sentences that were related to multiple distinct sentences in the ISCRelatedSentence table
# Unlike the above, there is not a clear lowest count or most recent sentence that we should select. So for now, we pick the sentence with the oldest case year 
# and lowest count number. However, we anticipate that we will change this behavior once we get feedback from TN about how to handle these situations properly. 
ISCRelated_sentence_selection AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY OffenderID, Jurisdication, CaseYear, CaseNumber,CountNumber ORDER BY RelatedCaseYear,RelatedCountNumber ASC) as sentence_rank
    FROM ISCRelated_sentence_count_selection
    WHERE count_rank = 1
),
consecutive_ISCRelated_sentences AS (
    SELECT *
    FROM ISCRelated_sentence_selection
    WHERE  sentence_rank = 1
),
cleaned_ISCSentence_view AS (
    SELECT 
        ISC.OffenderID,
        ISC.Jurisdiction as ConvictionCounty,
        ISC.CaseYear,
        # There was the same case number with both one and two spaces being treated as seperate cases when they are the same
        REGEXP_REPLACE(ISC.CaseNumber, '  ', ' ') AS CaseNumber,
        ISC.CountNumber,
        ISC.OffenseDate,
        'SEE OffenseDescription' as ConvictedOffense,
        ISC.ConvictedOffense as OffenseDescription, 
        CASE WHEN CAST(ISC.ExpirationDate AS DATETIME) < @{UPDATE_DATETIME_PARAM_NAME} THEN 'IN' ELSE 'AC' END AS SentenceStatus,
        CAST(NULL as STRING) as SentenceEffectiveDate,
        ISC.SentenceImposedDate as SentenceImposeDate,
        ISC.ExpirationDate as EarliestPossibleReleaseDate,
        ISC.ExpirationDate as FullExpirationDate,
        ISC.ExpirationDate as ExpirationDate,
        ISC.LastUpdateDate as LastUpdateDate,
        CASE WHEN ISC.ConvictedOffense LIKE '%MURDER%' 
                        OR ConvictedOffense LIKE '%HOMICIDE%'
                        OR ConvictedOffense LIKE '%MANSLAUGHTER%'
                        OR ConvictedOffense LIKE '%RAPE%'
                        OR ConvictedOffense LIKE '%MOLEST%'
                        OR ConvictedOffense LIKE '%BATTERY%'
                        OR ConvictedOffense LIKE '%ASSAULT%'
                        OR ConvictedOffense LIKE '%STALKING%'
                        OR ConvictedOffense LIKE '%CRIMES AGAINST PERSON%'
                        THEN 'Y' ELSE 'N' END AS AssaultiveOffenseFlag,
        CASE WHEN ISC.ConvictedOffense LIKE '%SEX%' OR ISC.ConvictedOffense LIKE '%RAPE%' THEN 'Y' ELSE 'N' END AS SexOffenderFlag,
        ISCR.RelatedJurisidicationCounty AS ConsecutiveConvictionCounty,
        ISCR.RelatedCaseYear AS ConsecutiveCaseYear,
        ISCR.RelatedCaseNumber AS ConsecutiveCaseNumber,
        ISCR.RelatedCountNumber AS ConsecutiveCountNumber,
        ISC.ISCSentencyType as ISCSentencyType,
        CASE WHEN ISC.Sentence LIKE '%LIFE%' THEN 'is_life' END as lifetime_flag,
        CAST(NULL as STRING) as ReleaseEligibilityDate,
        'ISC' AS sentence_source
    FROM {{ISCSentence@ALL}} ISC
    LEFT JOIN consecutive_ISCRelated_sentences ISCR ON 
        ISC.OffenderID = ISCR.OffenderID AND
        ISC.Jurisdiction = ISCR.Jurisdication AND
        ISC.CaseYear = ISC.CaseYear AND
        ISC.CaseNumber = ISCR.CaseNumber AND
        ISC.CountNumber = ISCR.CountNumber
),
all_sentence_sources_joined AS (
    SELECT *
    FROM cleaned_Sentence_view 

    UNION ALL 

    SELECT *
    FROM cleaned_Diversion_view 

    UNION ALL 

    SELECT * 
    FROM cleaned_ISCSentence_view
),
all_latest_sentences_joined AS (
    SELECT 
        OffenderID,
        ConvictionCounty,
        CaseYear,
        CaseNumber,
        CountNumber
    FROM {{Sentence}} 

    UNION ALL 

    SELECT 
        OffenderID,
        ConvictionCounty,
        CaseYear,
        CaseNumber,
        CountNumber
    FROM {{Diversion}}

    UNION ALL 

    SELECT 
        OffenderID,
        Jurisdiction AS ConvictionCounty,
        CaseYear,
        CaseNumber,
        CountNumber
    FROM {{ISCSentence}}
),
most_recent_sentence_information AS (
    SELECT t.* 
    FROM (
        SELECT 
            OffenderID,
            Sentences.ConvictionCounty,
            Sentences.CaseYear,
            CaseNumber,
            Sentences.CountNumber,
            MostRecentSentenceAction,
            Sentences.SentenceStatus,
            JOCharge.SentencedTo,
            JOCharge.SuspendedToProbation,
            Sentences.SentenceEffectiveDate,
            CASE WHEN Sentences.EarliestPossibleReleaseDate > '9998-01-01 00:00:00' THEN NULL ELSE Sentences.EarliestPossibleReleaseDate END as EarliestPossibleReleaseDate,
            CASE WHEN Sentences.FullExpirationDate > '9998-01-01 00:00:00' THEN NULL ELSE Sentences.FullExpirationDate END as FullExpirationDate,
            CASE WHEN Sentences.ExpirationDate > '9998-01-01 00:00:00' THEN NULL ELSE Sentences.ExpirationDate END as ExpirationDate,
            JOSpecialConditions.Conditions as Conditions,
            Sentences.SentenceImposeDate as SentenceImposeDate,
            DATE_DIFF(CAST(Sentences.EarliestPossibleReleaseDate AS DATETIME), CAST(COALESCE(Sentences.SentenceEffectiveDate, Sentences.SentenceImposeDate) as DATETIME), DAY) as CalculatedMinimumSentenceDays,
            DATE_DIFF(CAST(Sentences.FullExpirationDate AS DATETIME), CAST(COALESCE(Sentences.SentenceEffectiveDate,Sentences.SentenceImposeDate) as DATETIME), DAY) as CalculatedMaximumSentenceDays,
            GREATEST(JOSentence.PretrialJailCredits, JOSentence.CalculatedPretrialCredits) as PretrialJailCredits,
            -- The most accurate consecutive sentence information can be found in the `Sentence` table.
            Sentences.ConsecutiveConvictionCounty,
            Sentences.ConsecutiveCaseYear,
            Sentences.ConsecutiveCaseNumber,
            Sentences.ConsecutiveCountNumber,
            -- For Charges and Court Cases
            Sentences.OffenseDate,
            JOCharge.PleaDate,
            JOCharge.ChargeOffense,
            Sentences.ConvictionOffense,
            JOCharge.CrimeType,
            JOCharge.ConvictionClass,
            JOCharge.Verdict,
            JOMiscellaneous.JudgeName,
            JOIdentification.JudicialDistrict,
            REGEXP_REPLACE(Sentences.OffenseDescription,'[[:space:]]+',' ') AS OffenseDescription,
            Sentences.AssaultiveOffenseFlag,
            Sentences.SexOffenderFlag,
            Sentences.ISCSentencyType,
            Sentences.lifetime_flag,
            Sentences.ReleaseEligibilityDate,
            Sentences.sentence_source,
            ROW_NUMBER() OVER(PARTITION BY OffenderId, ConvictionCounty, CaseYear, CaseNumber, CountNumber, sentence_source ORDER BY Sentences.LastUpdateDate DESC) AS seq
        FROM all_sentence_sources_joined Sentences
        LEFT JOIN order_sentence_actions_by_date_per_sentence
        USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
        LEFT JOIN {{JOSentence}} JOSentence
        USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
        LEFT JOIN {{JOCharge}} JOCharge
        USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
        LEFT JOIN  {{SentenceMiscellaneous}} SentenceMisc
        USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
        LEFT JOIN {{JOMiscellaneous}} JOMiscellaneous
        USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
        LEFT JOIN {{JOIdentification}} JOIdentification
        USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
        LEFT JOIN special_conditions_aggregation JOSpecialConditions 
        USING (OffenderID, ConvictionCounty, CaseYear, CaseNumber, CountNumber)
    ) t 
    WHERE seq = 1
 ), 
 discharge_task_deadline_array AS (
  SELECT 
    OffenderID,
    ConvictionCounty,
    CaseYear,
    CaseNumber,
    CountNumber,
    TO_JSON_STRING(
        ARRAY_AGG(STRUCT< LastUpdateDate string,
                          ExpirationDate string >
                    (LastUpdateDate,
                     ExpirationDate) ORDER BY LastUpdateDate)
                      ) AS Task_Dates
  FROM 
    (SELECT DISTINCT OffenderID,
    ConvictionCounty,
    CaseYear,
    CaseNumber,
    CountNumber,
    LastUpdateDate,
    ExpirationDate 
    FROM all_sentence_sources_joined) Sentences
  GROUP BY 1,2,3,4,5
 )
SELECT DISTINCT
    a1.OffenderID,
    a1.ConvictionCounty,
    a1.CaseYear,
    a1.CaseNumber,
    a1.CountNumber,
    MostRecentSentenceAction,
    SentenceStatus,
    SentencedTo,
    SuspendedToProbation,
    SentenceEffectiveDate,
    EarliestPossibleReleaseDate,
    FullExpirationDate,
    ExpirationDate,
    Conditions,
    SentenceImposeDate,
    CalculatedMinimumSentenceDays,
    CalculatedMaximumSentenceDays,
    PretrialJailCredits,
    ConsecutiveConvictionCounty,
    ConsecutiveCaseYear,
    ConsecutiveCaseNumber,
    ConsecutiveCountNumber,
    OffenseDate,
    PleaDate,
    ChargeOffense,
    ConvictionOffense,
    CrimeType,
    ConvictionClass,
    Verdict,
    JudgeName,
    JudicialDistrict,
    OffenseDescription,
    AssaultiveOffenseFlag,
    SexOffenderFlag,
    ISCSentencyType,
    lifetime_flag,
    ReleaseEligibilityDate,
    sentence_source,
    Task_Dates
FROM most_recent_sentence_information mr
LEFT JOIN discharge_task_deadline_array USING(OffenderID, ConvictionCounty,CaseYear,CaseNumber,CountNumber)
INNER JOIN all_latest_sentences_joined a1
ON a1.OffenderID = mr.OffenderID
    AND a1.ConvictionCounty = mr.ConvictionCounty
    AND a1.CaseYear = mr.CaseYear
    AND a1.CaseNumber = mr.CaseNumber
    AND a1.CountNumber = mr.CountNumber
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tn",
    ingest_view_name="SentencesChargesAndCourtCases_v4",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
