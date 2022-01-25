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
    WITH cte AS 
    (
    SELECT 
        person_lk.person_id,
        sentence.OffenderID AS external_id,
        sentence.CaseNumber AS case_number,
        CAST(sentence.CountNumber AS INT64) AS count_number,
        --TODO(#10785): Research use of sentence effective date vs sentence date imposed for deduping and joining
        CAST(CAST(sentence.SentenceEffectiveDate AS DATETIME) AS DATE) AS sentence_effective_date,
        CAST(jo_id.JudicialDistrict AS INT64) AS judicial_district,
        CAST(jo_sentence.MaximumSentenceYears AS INT64)*365 + CAST(MaximumSentenceMonths AS INT64)*30 + CAST(MaximumSentenceDays AS INT64) AS max_sentence_length_days,
        charge.ConvictionOffense AS offense,
        offense.OffenseDescription AS description,
        offense.FelonyClass AS felony_class
    FROM `{project_id}.{raw_dataset}.Sentence_latest` sentence
    JOIN `{project_id}.{raw_dataset}.JOSentence_latest` jo_sentence
        USING(OffenderID, ConvictionCounty,CaseYear,CaseNumber, CountNumber)
    JOIN `{project_id}.{raw_dataset}.JOIdentification_latest` jo_id
        USING(OffenderID, ConvictionCounty,CaseYear,CaseNumber, CountNumber)
    JOIN `{project_id}.{raw_dataset}.JOCharge_latest` charge
        USING (OffenderID, ConvictionCounty,CaseYear,CaseNumber, CountNumber )
    JOIN `{project_id}.{raw_dataset}.OffenderStatute_latest` offense
        ON offense.Offense = charge.ConvictionOffense
    JOIN `{project_id}.{state_base_dataset}.state_person_external_id` person_lk
        ON sentence.OffenderID = person_lk.external_id
    )
    SELECT 
        *,
        ROW_NUMBER() OVER(ORDER BY sentence_effective_date, person_id) AS sentence_id,
    FROM
        (
        SELECT DISTINCT
            person_id,
            'US_TN' AS state_code,
            case_number,
            sentence_effective_date,
            judicial_district,
            offense,
            description,
            felony_class
        FROM 
            (
            SELECT 
                * EXCEPT (sentence_effective_date),
                MIN(sentence_effective_date) OVER(PARTITION BY person_id, case_number) AS sentence_effective_date,
            FROM cte
            )
        WHERE TRUE
        QUALIFY RANK() OVER(PARTITION BY person_id, sentence_effective_date
            ORDER BY 
                max_sentence_length_days DESC,
                felony_class ASC,
                offense ASC,
                judicial_district ASC,
                case_number ASC) = 1
        )
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
