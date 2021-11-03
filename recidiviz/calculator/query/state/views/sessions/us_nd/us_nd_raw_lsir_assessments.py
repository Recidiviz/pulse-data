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
"""Individual questions and subscale components of the LSI-R assessment in ND, derived from raw tables"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.calculator.query.state.views.sessions.assessment_lsir_scoring_key import (
    ASSESSMENT_LSIR_PROTECTIVE_QUESTION_LIST,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_RAW_LSIR_ASSESSMENTS_VIEW_NAME = "us_nd_raw_lsir_assessments"

US_ND_RAW_LSIR_ASSESSMENTS_VIEW_DESCRIPTION = """Individual questions and subscale components of the LSI-R assessment in ND, derived from raw tables"""

US_ND_RAW_LSIR_ASSESSMENTS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        p.state_code, 
        p.person_id, 
        SAFE.PARSE_DATE('%F', SPLIT(AssessmentDate, ' ')[OFFSET(0)]) assessment_date,
        -- Populate numeric scale responses from raw data, and input NULL for other questions.
        {lsir_question_columns},
        -- Total scores for each needs component in the LSI-R
        SAFE_CAST(CHtotal AS INT64) as criminal_history_total,
        SAFE_CAST(EETotal AS INT64) as education_employment_total,
        SAFE_CAST(FnclTotal AS INT64) as financial_total,
        SAFE_CAST(FMTotal AS INT64) as family_marital_total,
        SAFE_CAST(AccomTotal AS INT64) as accommodation_total,
        SAFE_CAST(LRTotal AS INT64) as leisure_recreation_total,
        SAFE_CAST(Cptotal AS INT64) as companions_total,
        SAFE_CAST(AdTotal AS INT64) as alcohol_drug_total,
        SAFE_CAST(EPTotal AS INT64) as emotional_personal_total,
        SAFE_CAST(AOTotal AS INT64) as attitudes_orientation_total,
        -- Sum all responses indicating protective factors
        {lsir_protective_question_sum} as protective_factors_score_total,
    FROM `{project_id}.us_nd_raw_data_up_to_date_views.docstars_lsi_chronology_latest` a
    LEFT JOIN `{project_id}.{base_dataset}.state_person_external_id` p
        ON a.SID = p.external_id
        AND p.state_code = 'US_ND'
        AND p.id_type = 'US_ND_SID'
    """

US_ND_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ND_RAW_LSIR_ASSESSMENTS_VIEW_NAME,
    view_query_template=US_ND_RAW_LSIR_ASSESSMENTS_QUERY_TEMPLATE,
    description=US_ND_RAW_LSIR_ASSESSMENTS_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    lsir_question_columns=",\n\t\t".join(
        [
            (
                "SAFE_CAST(Q" + str(x) + "value AS INT64)"
                # Raw data tables only include responses for numeric scale responses
                if x in ASSESSMENT_LSIR_PROTECTIVE_QUESTION_LIST
                else "NULL"
            )
            + " as lsir_response_"
            + str(x)
            for x in range(1, 55)
        ]
    ),
    lsir_protective_question_sum=" + ".join(
        [
            "SAFE_CAST(Q" + str(x) + "value AS INT64)"
            for x in ASSESSMENT_LSIR_PROTECTIVE_QUESTION_LIST
        ]
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER.build_and_print()
