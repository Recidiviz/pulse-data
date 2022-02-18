# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""State-specific preprocessing for TN raw sentence data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
    US_TN_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SENTENCES_PREPROCESSED_VIEW_NAME = "us_tn_sentences_preprocessed"

US_TN_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = (
    """State-specific preprocessing for TN raw sentence data"""
)

US_TN_SENTENCES_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/
    -- TODO(#10746): Remove sentencing pre-processing when TN sentences are ingested  
    SELECT 
        person_lk.person_id,
        person_lk.state_code,
        COALESCE(inc_sen.incarceration_sentence_id, sup_sen.supervision_sentence_id) AS sentence_id,
        CONCAT(js.OffenderID, js.ConvictionCounty, js.CaseYear, js.CaseNumber, FORMAT('%03d', CAST(js.CountNumber AS INT64))) AS external_id,
        js.OffenderID AS offender_id,
        js.ConvictionCounty AS conviction_county,
        CAST(js.CaseYear AS INT64) AS case_year,
        js.CaseNumber AS case_number,
        CAST(js.CountNumber AS INT64) AS count_number,
        CAST(jo_id.JudicialDistrict AS INT64) AS judicial_district,
        CAST(CAST(s.SentenceEffectiveDate AS datetime) AS DATE) AS sentence_effective_date,
        CAST(CAST(charge.OffenseDate AS datetime) AS DATE) AS offense_date,
        CAST(CAST(charge.PleaDate AS datetime) AS DATE) AS plea_date,
        COALESCE(CAST(CAST(misc.AlternateSentenceImposeDate AS datetime) AS DATE), CAST(CAST(charge.SentenceImposedDate AS datetime) AS DATE)) AS sentence_imposed_date,
        CAST(CAST(s.ExpirationDate AS datetime) AS DATE) AS expiration_date,
        CAST(CAST(s.ReleaseEligibilityDate AS datetime) AS DATE) AS release_eligibility_date,
        CAST(CAST(s.EarliestPossibleReleaseDate AS datetime) AS DATE) AS earliest_possible_release_date,
        CAST(CAST(s.FullExpirationDate AS datetime) AS DATE) AS full_expiration_date,
        CAST(js.MaximumSentenceYears AS INT64)*365 + CAST(js.MaximumSentenceMonths AS INT64)*30 + CAST(js.MaximumSentenceDays AS INT64) AS max_sentence_length_days_calculated,
        CAST(RangePercent AS NUMERIC)*.01 AS parole_eligibility_percent,
        CAST(js.PretrialJailCredits AS INT64) AS pretrial_jail_credits,
        CAST(js.CalculatedPretrialCredits AS INT64) AS calculated_pretrial_jail_credits,
        CAST(s.TotalProgramCredits AS INT64) AS total_program_credits,
        CAST(s.TotalBehaviorCredits AS INT64) AS total_behavior_credits,
        CAST(s.TotalPPSCCredits AS INT64) AS total_ppsc_credits,        
        CAST(TotalGEDCredits AS INT64) total_ged_credits,
        CAST(TotalLiteraryCredits AS INT64) total_literary_credits,
        CAST(TotalDrugAlcoholCredits AS INT64) total_drug_alcohol_credits,
        CAST(TotalEducationAttendanceCredits AS INT64) total_education_attendance_credits,
        CAST(TotalTreatmentCredits AS INT64) total_treatment_credits,
        s.ConsecutiveCaseNumber AS consecutive_case_number,
        CAST(s.ConsecutiveCountNumber AS INT64) AS consecutive_count_number,
        COALESCE(inc_sen_consec.incarceration_sentence_id, sup_sen_consec.supervision_sentence_id) AS consecutive_sentence_id,
        CONCAT(js.OffenderID, s.ConsecutiveConvictionCounty, s.ConsecutiveCaseYear, s.ConsecutiveCaseNumber, FORMAT('%03d', CAST(s.ConsecutiveCountNumber AS INT64))) AS consecutive_sentence_external_id,
        charge.ConvictionOffense as conviction_offense,
        charge.ConvictionClass AS conviction_class,
        conviction_offense.OffenseDescription AS offense_description,
        s.SentenceStatus AS sentence_status,
    FROM `{project_id}.{raw_dataset}.JOSentence_latest`js
    JOIN `{project_id}.{raw_dataset}.Sentence_latest` s
        USING(OffenderID, ConvictionCounty,CaseYear,CaseNumber, CountNumber)
    JOIN  `{project_id}.{raw_dataset}.JOCharge_latest` charge
        USING (OffenderID, ConvictionCounty,CaseYear,CaseNumber, CountNumber)
    JOIN `{project_id}.{raw_dataset}.OffenderStatute_latest` conviction_offense
        ON conviction_offense.Offense = charge.ConvictionOffense
    JOIN `{project_id}.{raw_dataset}.JOIdentification_latest` jo_id
        USING(OffenderID, ConvictionCounty,CaseYear,CaseNumber, CountNumber)
    LEFT JOIN `{project_id}.{raw_dataset}.SentenceMiscellaneous_latest` misc
        USING(OffenderID, ConvictionCounty,CaseYear,CaseNumber, CountNumber)
    JOIN `{project_id}.{state_base_dataset}.state_person_external_id` person_lk
        ON js.OffenderID = person_lk.external_id
    LEFT JOIN `{project_id}.{state_base_dataset}.state_incarceration_sentence` inc_sen
        ON inc_sen.external_id = CONCAT(js.OffenderID, '-', js.ConvictionCounty,'-', js.CaseYear, '-', js.CaseNumber, '-', js.CountNumber)
    LEFT JOIN `{project_id}.{state_base_dataset}.state_supervision_sentence` sup_sen
        ON sup_sen.external_id = CONCAT(js.OffenderID, '-', js.ConvictionCounty,'-', js.CaseYear, '-', js.CaseNumber, '-', js.CountNumber)
    LEFT JOIN `{project_id}.{state_base_dataset}.state_incarceration_sentence` inc_sen_consec
        ON inc_sen_consec.external_id = CONCAT(s.OffenderID, '-', s.ConsecutiveConvictionCounty,'-', s.ConsecutiveCaseYear, '-', s.ConsecutiveCaseNumber, '-', s.ConsecutiveCountNumber)
    LEFT JOIN `{project_id}.{state_base_dataset}.state_supervision_sentence` sup_sen_consec
        ON sup_sen_consec.external_id = CONCAT(s.OffenderID, '-', s.ConsecutiveConvictionCounty,'-', s.ConsecutiveCaseYear, '-', s.ConsecutiveCaseNumber, '-', s.ConsecutiveCountNumber)
"""

US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=US_TN_SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=US_TN_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    raw_dataset=US_TN_RAW_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
