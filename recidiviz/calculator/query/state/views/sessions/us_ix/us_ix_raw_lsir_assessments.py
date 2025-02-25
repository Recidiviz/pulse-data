# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Individual subscale components of the LSI-R assessment in ID (ATLAS), derived from raw tables"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_RAW_LSIR_ASSESSMENTS_VIEW_NAME = "us_ix_raw_lsir_assessments"

US_IX_RAW_LSIR_ASSESSMENTS_VIEW_DESCRIPTION = """Individual questions and components of the LSI-R assessment in ID (ATLAS), derived from raw tables"""

US_IX_SUBSCORE_ELEMENTS_LEGACY_DICT = {
    "criminal_history": "CRIMINAL HISTORY",
    "education_employment": "EDUCATION/EMPLOYMENT",
    "financial": "FINANCIAL",
    "family_marital": "FAMILY/MARITAL",
    "accommodation": "ACCOMMODATION",
    "leisure_recreation": "LEISURE/RECREATION",
    "companions": "COMPANIONS",
    "alcohol_drug": "ALCOHOL/DRUG PROBLEMS",
    "emotional_personal": "EMOTIONAL/PERSONAL",
    "attitudes_orientation": "ATTITUDE/ORIENTATION",
}

US_IX_SUBSCORE_ELEMENTS_ATLAS_DICT = {
    "criminal_history": "Criminal History",
    "education_employment": "Education/Employment",
    "financial": "Financial",
    "family_marital": "Family/Marital",
    "accommodation": "Accomodation",
    "leisure_recreation": "Leisure/recreation",
    "companions": "Companions",
    "alcohol_drug": "Alcohol/Drug Problem",
    "emotional_personal": "Emotional/Personal",
    "attitudes_orientation": "Attitudes/Orientations",
}


def extract_subscore_query_fragment(
    # Name of the raw text column
    raw_text_field: str,
    # Name of the subscore element
    element_name: str,
    # Flag for whether data format is LEGACY. If not, use ATLAS logic
    legacy_data_format: bool,
) -> str:
    """
    Extracts subscore from raw text field for the specified element name, with different logic
    applied to legacy vs. new ATLAS data formats.
    """
    if legacy_data_format:
        query_fragment = f"""
        SAFE_CAST(
            SAFE_CAST(
                REGEXP_EXTRACT(
                    {raw_text_field}, 
                    r'\\[{US_IX_SUBSCORE_ELEMENTS_LEGACY_DICT[element_name]}\\] Number of Answered/Questions: [0-9]+/[0-9]+ Section Score:([0-9.]+)'
                )
            AS FLOAT64) * SAFE_CAST(
                REGEXP_EXTRACT(
                    {raw_text_field}, 
                    r'\\[{US_IX_SUBSCORE_ELEMENTS_LEGACY_DICT[element_name]}\\] Number of Answered/Questions: ([0-9]+)/[0-9]+ Section Score:[0-9.]+'
                )
            AS FLOAT64)
        AS INT64)
        """
    else:
        query_fragment = f"""
        SAFE_CAST(
            REGEXP_EXTRACT(
                {raw_text_field}, 
                r'\\[{US_IX_SUBSCORE_ELEMENTS_ATLAS_DICT[element_name]}\\][a-zA-Z0-9/:, ]+Score: ([0-9.]+)'
            ) 
        AS INT64)
        """
    return f"{query_fragment} AS {element_name}_total,"


US_IX_RAW_LSIR_ASSESSMENTS_QUERY_TEMPLATE = (
    """
WITH lsir_scores_raw AS (
    SELECT
        "US_IX" AS state_code,
        p.person_id,
        CAST(SAFE_CAST(CompletionDate AS DATETIME) AS DATE) AS assessment_date,
        ResultNote,
        CASE
            WHEN REGEXP_CONTAINS(ResultNote, r'^Conversion:') THEN "LEGACY"
            WHEN REGEXP_CONTAINS(ResultNote, r'^\\[Overall Total Score\\]') THEN "ATLAS"
        END AS data_format,
    FROM
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.asm_Assessment_latest` a
    LEFT JOIN
        `{project_id}.{normalized_state_dataset}.state_person_external_id` p
    ON
        a.OffenderId = p.external_id
        AND p.state_code = "US_IX"
    WHERE
        AssessmentToolId = "174"
        AND CompletionDate IS NOT NULL
)
,
legacy_subscores AS (
    SELECT
        state_code,
        person_id,
        assessment_date,
"""
    + "\n".join(
        [
            extract_subscore_query_fragment(
                raw_text_field="ResultNote",
                element_name=element_name,
                legacy_data_format=True,
            )
            for element_name in US_IX_SUBSCORE_ELEMENTS_LEGACY_DICT
        ]
    )
    + """
    FROM
        lsir_scores_raw
    WHERE
        data_format = "LEGACY"
)
,
atlas_subscores AS (
    SELECT
        state_code,
        person_id,
        assessment_date,
"""
    + "\n".join(
        [
            extract_subscore_query_fragment(
                raw_text_field="ResultNote",
                element_name=element_name,
                legacy_data_format=False,
            )
            for element_name in US_IX_SUBSCORE_ELEMENTS_ATLAS_DICT
        ]
    )
    + """
    FROM
        lsir_scores_raw
    WHERE
        data_format = "ATLAS"
)
,
all_subscores AS (
    SELECT * FROM legacy_subscores
    UNION ALL
    SELECT * FROM atlas_subscores
)
,
raw_assessments_pivot AS (
    /* Turn assessment questions and responses from "long" to "wide" format, with columns for each question*/
    SELECT * FROM
    (
        SELECT DISTINCT
            state_code,
            person_id,
            assessment_date,
            assessment_question, 
            NULL AS assessment_response 
        FROM all_subscores,
        UNNEST(GENERATE_ARRAY(1, 55)) assessment_question
    )
    PIVOT(
        MAX(assessment_response) AS lsir_response
        FOR assessment_question IN ({lsir_question_array})
    )
) 

SELECT DISTINCT
    raw_assessments_pivot.*,
    """
    + ",\n\t".join(
        [f"{element_name}_total" for element_name in US_IX_SUBSCORE_ELEMENTS_ATLAS_DICT]
        + ["NULL AS protective_factors_score_total"]
    )
    + """
FROM
    all_subscores
LEFT JOIN 
    raw_assessments_pivot
USING 
    (state_code, person_id, assessment_date)

"""
)

US_IX_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_IX_RAW_LSIR_ASSESSMENTS_VIEW_NAME,
    view_query_template=US_IX_RAW_LSIR_ASSESSMENTS_QUERY_TEMPLATE,
    description=US_IX_RAW_LSIR_ASSESSMENTS_VIEW_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    lsir_question_array=",".join([str(x) for x in range(1, 55)]),
    should_materialize=False,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER.build_and_print()
