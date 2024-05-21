# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query that generates additional information for violation reports from different sources
than view_supervision_violation
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- The violation survey responses are coded in Atlas using a series of SectionIds, QuestionIds, and AnswerIds.
-- This CTE compiles a mapping of all those Section, Question, and Answer Ids to their respective descriptions for easier readability later on
survey_template AS (
  SELECT DISTINCT
    JSON_EXTRACT_SCALAR(sections, "$.questionnaireId") AS QuestionnaireTemplateId,
    JSON_EXTRACT_SCALAR(sections, "$.sectionId") AS sectionId,
    JSON_EXTRACT_SCALAR(sections, "$.title") AS section_title,
    JSON_EXTRACT_SCALAR(questions, "$.title") AS question_title,
    JSON_EXTRACT_SCALAR(answers, "$.answerId") AS answerId,
    JSON_EXTRACT_SCALAR(answers, "$.title") AS answer_title
  FROM {ref_QuestionnaireTemplate} template,
  UNNEST(JSON_EXTRACT_ARRAY(QuestionnaireDefinition, "$.sections")) AS sections,
  UNNEST(JSON_EXTRACT_ARRAY(sections, "$.questions")) AS questions,
  UNNEST(JSON_EXTRACT_ARRAY(questions, "$.answers")) AS answers
  WHERE UPPER(QuestionnaireDefinition) LIKE '%VIOLATION SURVEY%'
),
-- This CTE pulls together all the survey responses (with their QuestionnaireTemplateId, QuestionIds, AnswerIds, and answer values)
-- for all survey templates that correspond with the violation survey.  We also pull in an external id for the 
-- violation survey itself (OffenderQuestionnaireTemplateId), the JII associated with the survey, and the employee information
-- for the employee that entered the survey.  The result of this CTE is one row per survey-section-question-answer combination.
survey_responses AS (
  SELECT 
    OffenderQuestionnaireTemplateId,
    QuestionnaireTemplateId,
    OffenderId,
    JSON_EXTRACT_SCALAR(questions, "$.sectionId") AS sectionId,
    JSON_EXTRACT_SCALAR(questions, "$.answerId") AS answerId,
    JSON_EXTRACT_SCALAR(questions, "$.value") AS value,
    CompletedByEmployeeId,
    CompletedDate,
  FROM {ind_Offender_QuestionnaireTemplate},
  UNNEST(JSON_EXTRACT_ARRAY(Response)) AS questions
  WHERE QuestionnaireTemplateId IN (
    "1022",
    "55",
    "1025",
    "1031",
    "1030"
  )
),
-- This CTE filters down to relevant sections/questions and then joins on the survey_template CTE
-- to get the survey answers in a more readable format.  This CTE also filters out survey-section-question-answer rows where the
-- question or answer was NULL.
survey_answer_per_question AS (
SELECT DISTINCT *
FROM (
  SELECT 
    resp.CompletedByEmployeeId,
    resp.CompletedDate,
    resp.OffenderQuestionnaireTemplateId,
    resp.OffenderId,
    resp.sectionId,
    tem.question_title,
    CASE 
      WHEN sectionId = '1' THEN value
      ELSE answer_title
      END AS answer,
  FROM survey_responses resp
  LEFT JOIN survey_template tem USING(QuestionnaireTemplateId, SectionId, AnswerId)
  WHERE (
    (QuestionnaireTemplateId = '1022' and sectionId in ('1', '3', '4')) OR
    (QuestionnaireTemplateId = '55' and sectionId in ('1', '2', '3')) OR
    (QuestionnaireTemplateId = '1025' and sectionId in ('1', '3', '4')) OR
    (QuestionnaireTemplateId = '1031' and sectionId in ('1', '3', '4')) OR
    (QuestionnaireTemplateId = '1030' and sectionId in ('1', '3', '4'))
  )
)
WHERE answer IS NOT NULL AND question_title IS NOT NULL
ORDER BY OffenderQuestionnaireTemplateId, SectionId),

-- This CTE aggregates all the results into one row pers survey.
-- For violation type (which is a "check all that apply" question), we string agg
-- all distinct violation type values.
-- We also take the MIN of violation date across all survey-section-question-answer rows, 
-- but we only expect one non-null value for violation date per survey anyways.
aggregated_results AS (
  SELECT
    CompletedByEmployeeId,
    CompletedDate,
    OffenderQuestionnaireTemplateId,
    OffenderId,
    MIN(violation_date) as violation_date,
    STRING_AGG(DISTINCT violation_type, "," ORDER BY violation_type) AS violation_types
  FROM (
  SELECT 
    DISTINCT
    CompletedByEmployeeId,
    CompletedDate,
    OffenderQuestionnaireTemplateId,
    OffenderId,
    CASE
      WHEN question_title = "Report of Violation Date" 
      OR question_title = "Violation Date" 
      THEN DATE(PARSE_DATE('%m/%d/%Y', TRIM(answer))) END AS violation_date,
    CASE
      WHEN question_title = "Violation Type" 
      OR question_title = "Violation Type (check all that apply)"
      THEN answer END AS violation_type,
    FROM survey_answer_per_question
  )
  GROUP BY CompletedByEmployeeId,CompletedDate,OffenderQuestionnaireTemplateId,OffenderId
)

-- finally, only keep violations after the atlas migration date (because we'll use the legacy view for violations before then)
SELECT * 
FROM aggregated_results
WHERE violation_date >= DATE(2022,11,10);

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="supervision_violation_supplemental",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
