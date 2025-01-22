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
"""View logic to prepare US_ND community opportunities data for PSI tools"""

US_ND_SENTENCING_COMMUNITY_OPPORTUNITY_TEMPLATE = """
WITH
  community_opp_info AS (
  SELECT
    OpportunityName,
    Description,
    ProviderName,
    ProviderWebsite,
    ProviderAddress,
    minAge,
    maxAge,
    district,
    CONCAT(
      CASE
        WHEN NeedsAddressed LIKE '%Anger management%' THEN 'AngerManagement,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Case management%' THEN 'CaseManagement,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Clothing and toiletries%' THEN 'ClothingAndToiletries,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Education%' THEN 'Education,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Family services%' THEN 'FamilyServices,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Financial assistance%' THEN 'FinancialAssistance,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Food insecurity%' THEN 'FoodInsecurity,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%General re-entry support%' THEN 'GeneralReEntrySupport,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Domestic violence issues%' THEN 'DomesticViolenceIssues,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Healthcare%' THEN 'Healthcare,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Housing opportunities%' THEN 'HousingOpportunities,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Job training or opportunities%' THEN 'JobTrainingOrOpportunities,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Mental health%' THEN 'MentalHealth,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Substance use%' THEN 'SubstanceUse,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Transportation%' THEN 'Transportation,'
        ELSE ''
    END
      ,
      CASE
        WHEN NeedsAddressed LIKE '%Other%' THEN 'Other,'
        ELSE ''
    END
      ) AS needsAddressedConcat,
    REGEXP_REPLACE(ProviderPhoneNumber, r'[^0-9]', '') AS CleanedProviderPhoneNumber,
    CASE
      WHEN EligibilityCriteria LIKE "%Developmental disability diagnosis%" THEN TRUE
      ELSE FALSE
  END
    AS developmentalDisabilityDiagnosisCriterion,
    CASE
      WHEN EligibilityCriteria LIKE "%No current or prior sex offense convictions%" THEN TRUE
      ELSE FALSE
  END
    AS noCurrentOrPriorSexOffenseCriterion,
    CASE
      WHEN EligibilityCriteria LIKE "%No current or prior violent offense convictions%" THEN TRUE
      ELSE FALSE
  END
    AS noCurrentOrPriorViolentOffenseCriterion,
    CASE
      WHEN EligibilityCriteria LIKE "%No pending felony charges in another county/state%" THEN TRUE
      ELSE FALSE
  END
    AS noPendingFelonyChargesInAnotherCountyOrStateCriterion,
    CASE
      WHEN EligibilityCriteria LIKE "%Entry of guilty plea%" THEN TRUE
      ELSE FALSE
  END
    AS entryOfGuiltyPleaCriterion,
    CASE
      WHEN EligibilityCriteria LIKE "%Veteran status%" THEN TRUE
      ELSE FALSE
  END
    AS veteranStatusCriterion,
    CASE
      WHEN EligibilityCriteria LIKE "%Prior criminal history - NONE%" THEN "None"
      WHEN EligibilityCriteria LIKE "%Prior criminal history - SIGNIFICANT%" THEN "Significant"
      ELSE NULL
  END
    AS priorCriminalHistoryCriterion,
    CONCAT(
      CASE
        WHEN mentalHealthDisorderCriteria LIKE '%Bipolar Disorder%' THEN 'BipolarDisorder,'
        ELSE ''
    END
      ,
      CASE
        WHEN mentalHealthDisorderCriteria LIKE '%Borderline Personality Disorder%' THEN 'BorderlinePersonalityDisorder,'
        ELSE ''
    END
      ,
      CASE
        WHEN mentalHealthDisorderCriteria LIKE '%Delusional Disorder%' THEN 'DelusionalDisorder,'
        ELSE ''
    END
      ,
      CASE
        WHEN mentalHealthDisorderCriteria LIKE '%Major Depressive Disorder (severe and recurrent)%' THEN 'MajorDepressiveDisorder,'
        ELSE ''
    END
      ,
      CASE
        WHEN mentalHealthDisorderCriteria LIKE '%Psychotic Disorder%' THEN 'PsychoticDisorder,'
        ELSE ''
    END
      ,
      CASE
        WHEN mentalHealthDisorderCriteria LIKE '%Schizophrenia%' THEN 'Schizophrenia,'
        ELSE ''
    END
      ,
      CASE
        WHEN mentalHealthDisorderCriteria LIKE '%Other%' THEN 'Other,'
        ELSE ''
    END
      ,
      CASE
        WHEN mentalHealthDisorderCriteria LIKE '%Schizoaffective Disorder%' THEN 'SchizoaffectiveDisorder,'
        ELSE ''
    END
      ,
      CASE
        WHEN mentalHealthDisorderCriteria LIKE '%Any%' THEN 'Any'
        ELSE ''
    END
      ,
      CASE
        WHEN mentalHealthDisorderCriteria IS NULL THEN NULL
        ELSE ''
    END
      ) AS mentalHealthConcat,
    CONCAT(
      CASE
        WHEN genders LIKE "%Men%" THEN 'Men,'
        ELSE ''
    END
      ,
      CASE
        WHEN genders LIKE "%Women%" THEN 'Women,'
        ELSE ''
    END
      ,
      CASE
        WHEN genders IS NULL THEN NULL
        ELSE NULL
    END
      ) AS gendersConcat,
    substanceUseDisorderCriteria AS diagnosedSubstanceUseDisorderCriterion,
    AdditionalNotes AS additionalNotes,
    DATE(lastUpdatedDate) AS lastUpdatedDate,
    genericDescription,
    REGEXP_EXTRACT_ALL(countiesServed, 'Adams|Barnes|Benson|Billings|Bottineau|Bowman|Burke|Burleigh|Cass|Cavalier|Dickey|Divide|Dunn|Eddy|Emmons|Foster|Golden Valley|Grand Forks|Grant|Griggs|Hettinger|Kidder|LaMoure|Logan|McHenry|McIntosh|McKenzie|McLean|Mercer|Morton|Mountrail|Nelson|Oliver|Pembina|Pierce|Ramsey|Ransom|Renville|Richland|Rolette|Sargent|Sheridan|Sioux|Slope|Stark|Steele|Stutsman|Towner|Traill|Walsh|Ward|Wells|Williams') AS counties,
    status,
  FROM
    `{project_id}.{us_nd_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_community_opportunities_latest`)
SELECT
  "US_IX" AS state_code,
  OpportunityName,
  Description,
  ProviderName,
  ProviderWebsite,
  ProviderAddress,
  minAge,
  maxAge,
  district,
  CASE
    WHEN needsAddressedConcat = '' THEN NULL
    ELSE SPLIT(LEFT(needsAddressedConcat, LENGTH(needsAddressedConcat)-1))
END
  AS NeedsAddressed,
  CleanedProviderPhoneNumber,
  developmentalDisabilityDiagnosisCriterion,
  noCurrentOrPriorSexOffenseCriterion,
  noCurrentOrPriorViolentOffenseCriterion,
  noPendingFelonyChargesInAnotherCountyOrStateCriterion,
  entryOfGuiltyPleaCriterion,
  veteranStatusCriterion,
  priorCriminalHistoryCriterion,
  CASE
    WHEN mentalHealthConcat LIKE "%Any%" THEN SPLIT("Any")
    WHEN mentalHealthConcat IS NOT NULL THEN SPLIT(LEFT(mentalHealthConcat, LENGTH(mentalHealthConcat)-1))
END
  AS diagnosedMentalHealthDiagnosisCriterion,
  CAST(NULL AS STRING) AS asamLevelOfCareRecommendationCriterion,
  SPLIT(LEFT(gendersConcat, LENGTH(gendersConcat)-1)) AS genders,
  diagnosedSubstanceUseDisorderCriterion,
  CAST(NULL AS INT64) AS minLSIRScoreCriterion,
  CAST(NULL AS INT64) AS maxLsirScoreCriterion,
  additionalNotes,
  lastUpdatedDate,
  genericDescription,
  counties,
  status
FROM
  community_opp_info
"""
