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
"""Pulls out various relevant date fields for TN Custody Classification from StateAssessment metadata"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_PREPROCESSED_VIEW_NAME = (
    "us_tn_custody_classification_assessment_dates_preprocessed"
)

US_TN_CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_PREPROCESSED_VIEW_DESCRIPTION = """Pulls out various relevant date fields for TN Custody Classification from StateAssessment metadata"""

US_TN_CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_PREPROCESSED_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        /* In TN during the custody classification process we see a few different relevant dates: 
        CAFDate, ClassificationDate, and ClassificationDecisionDate. CAFDate is the
        date of the CAF form, and what we ingest as assessment_date. ClassificationDate is the date the
        classification was submitted, and ClassificationDecisionDate is when a decision was made.        
        */
        assessment_date,
        classification_date,
        classification_decision_date,
        classification_decision,
        DATE_ADD(classification_date, INTERVAL 12 MONTH) AS assessment_due_date,
    FROM (
        SELECT
            state_code,
            person_id,
            assessment_date,
            DATE(NULLIF(JSON_EXTRACT_SCALAR(assessment_metadata,"$.CLASSIFICATIONDATE"),"")) AS classification_date,
            DATE(NULLIF(JSON_EXTRACT_SCALAR(assessment_metadata,"$.CLASSIFICATIONDECISIONDATE"),"")) AS classification_decision_date,
            JSON_EXTRACT_SCALAR(assessment_metadata,"$.CLASSIFICATIONDECISION") AS classification_decision,
            assessment_type,
            assessment_score
        FROM
            `{project_id}.sessions.assessment_score_sessions_materialized`
    )
    WHERE
        assessment_type = "CAF"
        -- Only keep classifications that were approved
        AND classification_decision = 'A'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, assessment_type, assessment_date 
                              ORDER BY assessment_score DESC) = 1
        
"""

US_TN_CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_PREPROCESSED_VIEW_NAME,
    description=US_TN_CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_TN_CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_PREPROCESSED_VIEW_BUILDER.build_and_print()
