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
WITH community_opp_info AS (
    SELECT
       OpportunityName,
       Description,
       ProviderName,
       ProviderWebsite,
       ProviderAddress,
       CapacityTotal,
       CapacityAvailable,
       minAge,
       maxAge,
       district,
       CONCAT(
            CASE WHEN NeedsAddressed LIKE '%Anger management%' THEN 'AngerManagement,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Case management%' THEN 'CaseManagement,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Clothing and toiletries%' THEN 'ClothingAndToiletries,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Education%' THEN 'Education,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Family services%' THEN 'FamilyServices,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Financial assistance%' THEN 'FinancialAssistance,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Food insecurity%' THEN 'FoodInsecurity,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%General re-entry support%' THEN 'GeneralReEntrySupport,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Domestic violence issues%' THEN 'DomesticViolenceIssues,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Healthcare%' THEN 'Healthcare,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Housing opportunities%' THEN 'HousingOpportunities,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Job training or opportunities%' THEN 'JobTrainingOrOpportunities,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Mental health%' THEN 'MentalHealth,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Substance use%' THEN 'SubstanceUse,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Transportation%' THEN 'Transportation,' ELSE '' END,
            CASE WHEN NeedsAddressed LIKE '%Other%' THEN 'Other,' ELSE '' END
        ) AS needsAddressedConcat,
       REGEXP_REPLACE(ProviderPhoneNumber, r'[^0-9]', '') AS CleanedProviderPhoneNumber,
        CASE
            WHEN EligibilityCriteria LIKE "%Developmental disability diagnosis%"
            THEN TRUE
            ELSE FALSE
        END AS developmentalDisabilityDiagnosisCriterion,
        CASE
            WHEN EligibilityCriteria LIKE "%No current or prior sex offense convictions%"
            THEN TRUE
            ELSE FALSE
        END AS noCurrentOrPriorSexOffenseCriterion,
        CASE
            WHEN EligibilityCriteria LIKE "%No current or prior violent offense convictions%"
            THEN TRUE
            ELSE FALSE
        END AS noCurrentOrPriorViolentOffenseCriterion,
        CASE
            WHEN EligibilityCriteria LIKE "%No pending felony charges in another county/state%"
            THEN TRUE
            ELSE FALSE
        END AS noPendingFelonyChargesInAnotherCountyOrStateCriterion,
        CASE
            WHEN EligibilityCriteria LIKE "%Entry of guilty plea%"
            THEN TRUE
            ELSE FALSE
        END AS entryOfGuiltyPleaCriterion,
        CASE
            WHEN EligibilityCriteria LIKE "%Veteran status%"
            THEN TRUE
            ELSE FALSE
        END AS veteranStatusCriterion,
        CASE
            WHEN EligibilityCriteria LIKE "%Prior criminal history - NONE%"
            THEN "None"
            WHEN EligibilityCriteria LIKE "%Prior criminal history - SIGNIFICANT%"
            THEN "Significant"
            ELSE NULL
        END AS priorCriminalHistoryCriterion,
        CONCAT(
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Bipolar Disorder%' THEN 'BipolarDisorder,' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Borderline Personality Disorder%' THEN 'BorderlinePersonalityDisorder,' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Delusional Disorder%' THEN 'DelusionalDisorder,' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Major Depressive Disorder (severe and recurrent)%' THEN 'MajorDepressiveDisorder,' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Psychotic Disorder%' THEN 'PsychoticDisorder,' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Schizophrenia%' THEN 'Schizophrenia,' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Other%' THEN 'Other,' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Schizoaffective Disorder%' THEN 'SchizoaffectiveDisorder,' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Any%' THEN 'Any' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria IS NULL THEN NULL ELSE '' END
        ) AS mentalHealthConcat,
        CASE
            WHEN ASAMLevelCriteria LIKE '%1.0 - Long-Term Remission Monitoring%' THEN 'LongTermRemissionMonitoring' 
            WHEN ASAMLevelCriteria LIKE '%1.5 - Outpatient Therapy%' THEN 'OutpatientTherapy'
            WHEN ASAMLevelCriteria LIKE '%1.7 - Medically Managed Outpatient%' THEN 'MedicallyManagedOutpatient' 
            WHEN ASAMLevelCriteria LIKE '%2.1 - Intensive Outpatient (IOP)%' THEN 'IntensiveOutpatient' 
            WHEN ASAMLevelCriteria LIKE '%2.5 - High-Intensity Outpatient (HIOP)%' THEN 'HighIntensityOutpatient'
            WHEN ASAMLevelCriteria LIKE '%2.7 - Medically Managed Intensive Outpatient%' THEN 'MedicallyManagedIntensiveOutpatient'
            WHEN ASAMLevelCriteria LIKE '%3.1 - Clinically Managed Low-Intensity Residential%' THEN 'ClinicallyManagedLowIntensityResidential'
            WHEN ASAMLevelCriteria LIKE '%3.5 - Clinically Managed High-Intensity Residential%' THEN 'ClinicallyManagedHighIntensityResidential' 
            WHEN ASAMLevelCriteria LIKE '%3.7 - Medically Managed Residential%' THEN 'MedicallyManagedResidential' 
            WHEN ASAMLevelCriteria LIKE '%Medically Managed Inpatient%' THEN 'MedicallyManagedInpatient'
            WHEN ASAMLevelCriteria LIKE "%Any%" THEN 'Any'
            WHEN ASAMLevelCriteria IS NULL THEN NULL
        END as asamLevelOfCareRecommendationCriterion,
        CASE 
            WHEN genders LIKE "%Men%" THEN 'Men'
            WHEN genders LIKE "%Women%" THEN 'Women'
            WHEN genders IS NULL THEN NULL
        END AS gender,
        substanceUseDisorderCriteria AS diagnosedSubstanceUseDisorderCriterion,
        CAST(minLSIRScore AS INT64) AS minLSIRScoreCriterion,
        CAST(maxLSIRScore AS INT64) AS maxLsirScoreCriterion,
        AdditionalNotes AS additionalNotes,
        DATE(lastUpdatedDate) AS lastUpdatedDate,
        genericDescription,
        CONCAT(
            CASE WHEN countiesServed LIKE "%Adams%" THEN 'Adams,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Barnes%" THEN 'Barnes,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Benson%" THEN 'Benson,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Billings%" THEN 'Billings,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Bottineau%" THEN 'Bottineau,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Bowman%" THEN 'Bowman,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Burke%" THEN 'Burke,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Burleigh%" THEN 'Burleigh,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Cass%" THEN 'Cass,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Cavalier%" THEN 'Cavalier,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Dickey%" THEN 'Dickey,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Divide%" THEN 'Divide,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Dunn%" THEN 'Dunn,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Eddy%" THEN 'Eddy,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Emmons%" THEN 'Emmons,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Foster%" THEN 'Foster,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Golden Valley%" THEN 'Golden Valley,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Grand Forks%" THEN 'Grand Forks,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Grant%" THEN 'Grant,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Griggs%" THEN 'Griggs,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Hettinger%" THEN 'Hettinger,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Kidder%" THEN 'Kidder,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%LaMoure%" THEN 'LaMoure,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Logan%" THEN 'Logan,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%McHenry%" THEN 'McHenry,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%McIntosh%" THEN 'McIntosh,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%McKenzie%" THEN 'McKenzie,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%McLean%" THEN 'McLean,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Mercer%" THEN 'Mercer,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Morton%" THEN 'Morton,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Mountrail%" THEN 'Mountrail,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Nelson%" THEN 'Nelson,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Oliver%" THEN 'Oliver,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Pembina%" THEN 'Pembina,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Pierce%" THEN 'Pierce,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Ramsey%" THEN 'Ramsey,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Ransom%" THEN 'Ransom,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Renville%" THEN 'Renville,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Richland%" THEN 'Richland,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Rolette%" THEN 'Rolette,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Sargent%" THEN 'Sargent,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Sheridan%" THEN 'Sheridan,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Sioux%" THEN 'Sioux,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Slope%" THEN 'Slope,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Stark%" THEN 'Stark,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Steele%" THEN 'Steele,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Stutsman%" THEN 'Stutsman,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Towner%" THEN 'Towner,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Traill%" THEN 'Traill,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Walsh%" THEN 'Walsh,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Ward%" THEN 'Ward,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Wells%" THEN 'Wells,' ELSE '' END,
            CASE WHEN countiesServed LIKE "%Williams%" THEN 'Williams,' ELSE '' END
        ) as countiesServedConcat,
        status
   FROM `{project_id}.{us_nd_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_community_opportunities_latest`)

   SELECT 
        "US_ND" AS state_code,
        OpportunityName,
        Description,
        ProviderName,
        ProviderWebsite,
        ProviderAddress,
        CapacityTotal,
        CapacityAvailable,
        minAge,
        maxAge,
        district,
        CASE
            WHEN needsAddressedConcat = ''
            THEN NULL
            ELSE  SPLIT(LEFT(needsAddressedConcat, LENGTH(needsAddressedConcat)-1))
        END AS NeedsAddressed,
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
        END AS diagnosedMentalHealthDiagnosisCriterion,
        asamLevelOfCareRecommendationCriterion,
        gender,
        diagnosedSubstanceUseDisorderCriterion,
        minLSIRScoreCriterion,
        maxLsirScoreCriterion,
        additionalNotes,
        lastUpdatedDate,
        genericDescription,
        SPLIT(LEFT(countiesServedConcat, LENGTH(countiesServedConcat)-1)) as countiesServed,
        status
    FROM community_opp_info
"""
