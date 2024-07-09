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
"""View logic to prepare US_IX community opportunities data for PSI tools"""

US_IX_SENTENCING_COMMUNITY_OPPORTUNITY_TEMPLATE = """
WITH community_opp_info AS (
    SELECT
       *,
       REGEXP_REPLACE(ProviderPhoneNumber, r'[^0-9]', '') AS CleanedProviderPhoneNumber,
       CASE
            WHEN EligibilityCriteria LIKE "%Ages 18+%"
            THEN TRUE
            ELSE FALSE
        END AS eighteenOrOlderCriterion,
        CASE
            WHEN EligibilityCriteria LIKE "%Developmental disability diagnosis%"
            THEN TRUE
            ELSE FALSE
        END AS developmentalDisabilityDiagnosisCriterion,
        CASE
            WHEN EligibilityCriteria LIKE "%Minors (<18 years old) only%"
            THEN TRUE
            ELSE FALSE
        END AS minorCriterion,
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
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Bipolar Disorder%' THEN 'BipolarDisorder, ' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Borderline Personality Disorder%' THEN 'BorderlinePersonalityDisorder, ' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Delusional Disorder%' THEN 'DelusionalDisorder, ' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Major Depressive Disorder (severe and recurrent)%' THEN 'MajorDepressiveDisorder, ' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Psychotic Disorder%' THEN 'PsychoticDisorder, ' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Schizophrenia%' THEN 'Schizophrenia, ' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Other%' THEN 'Other, ' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria LIKE '%Schizoaffective Disorder%' THEN 'SchizoaffectiveDisorder, ' ELSE '' END,
            CASE WHEN mentalHealthDisorderCriteria IS NULL THEN 'Any' ELSE '' END
        ) AS mentalHealthConcat,
        CONCAT(
            CASE WHEN ASAMLevelCriteria LIKE '%1.0 - Long-Term Remission Monitoring%' THEN 'LongTermRemissionMonitoring, ' ELSE '' END,
            CASE WHEN ASAMLevelCriteria LIKE '%1.5 - Outpatient Therapy%' THEN 'OutpatientTherapy, ' ELSE '' END,
            CASE WHEN ASAMLevelCriteria LIKE '%1.7 - Medically Managed Outpatient%' THEN 'MedicallyManagedOutpatient, ' ELSE '' END,
            CASE WHEN ASAMLevelCriteria LIKE '%2.1 - Intensive Outpatient (IOP)%' THEN 'IntensiveOutpatient, ' ELSE '' END,
            CASE WHEN ASAMLevelCriteria LIKE '%2.5 - High-Intensity Outpatient (HIOP)%' THEN 'HighIntensityOutpatient, ' ELSE '' END,
            CASE WHEN ASAMLevelCriteria LIKE '%2.7 - Medically Managed Intensive Outpatient%' THEN 'MedicallyManagedIntensiveOutpatient, ' ELSE '' END,
            CASE WHEN ASAMLevelCriteria LIKE '%3.1 - Clinically Managed Low-Intensity Residential%' THEN 'ClinicallyManagedLowIntensityResidential, ' ELSE '' END,
            CASE WHEN ASAMLevelCriteria LIKE '%3.5 - Clinically Managed High-Intensity Residential%' THEN 'ClinicallyManagedHighIntensityResidential, ' ELSE '' END,
            CASE WHEN ASAMLevelCriteria LIKE '%3.7 - Medically Managed Residential%' THEN 'MedicallyManagedResidential, ' ELSE '' END,
            CASE WHEN ASAMLevelCriteria LIKE '%Medically Managed Inpatient%' THEN 'MedicallyManagedInpatient, ' ELSE '' END,
            CASE WHEN ASAMLevelCriteria IS NULL THEN 'Any' ELSE '' END
          ) as ASAMLevelConcat,
          CONCAT(
            CASE WHEN substanceUseDisorderCriteria LIKE "%Mild%" THEN 'Mild, ' ELSE '' END,
            CASE WHEN substanceUseDisorderCriteria LIKE "%Moderate%" THEN 'Moderate, ' ELSE '' END,
            CASE WHEN substanceUseDisorderCriteria LIKE "%Severe%" THEN 'Severe, ' ELSE '' END,
            CASE WHEN substanceUseDisorderCriteria IS NULL THEN 'Any' ELSE '' END
          ) AS substanceUseConcat,
        minLSIRScore AS minLSIRScoreCriterion,
        maxLSIRScore AS maxLsirScoreCriterion
   FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_community_opportunities_latest`)

   SELECT 
        *,
        TO_JSON(STRUCT(
            CASE 
              WHEN mentalHealthConcat = 'Any' THEN mentalHealthConcat 
              ELSE LEFT(mentalHealthConcat, LENGTH(mentalHealthConcat) - 2) 
            END AS diagnosedMentalHealthDiagnosisCriterion
        )) AS diagnosedMentalHealthDiagnosisCriterion,
        TO_JSON(STRUCT(
            CASE 
              WHEN ASAMLevelConcat = 'Any' THEN ASAMLevelConcat 
              ELSE LEFT(ASAMLevelConcat, LENGTH(ASAMLevelConcat) - 2) 
            END AS asamLevelOfCareRecommendationCriterion
        ))AS asamLevelOfCareRecommendationCriterion,
        TO_JSON(STRUCT(
            CASE 
              WHEN substanceUseConcat = 'Any' THEN substanceUseConcat 
              ELSE LEFT(substanceUseConcat, LENGTH(substanceUseConcat) - 2) 
            END AS diagnosedSubstanceUseDisorderCriterion
          )) AS diagnosedSubstanceUseDisorderCriterion
    FROM community_opp_info
"""
