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
"""Face to Face contact and assessment data

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.line_staff_validation.contacts_and_assessments
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CONTACTS_AND_ASSESSMENTS_VIEW_NAME = "contacts_and_assessments"

CONTACTS_AND_ASSESSMENTS_DESCRIPTION = """
"""

CONTACTS_AND_ASSESSMENTS_QUERY_TEMPLATE = """
WITH dates AS (
    SELECT 
        state_code, year, month, person_id, person_external_id,
        supervising_officer_external_id AS officer_external_id,
        most_recent_assessment_date,
        most_recent_face_to_face_date,
        most_recent_home_visit_date,
        next_recommended_assessment_date,
        next_recommended_face_to_face_date,
        next_recommended_home_visit_date,
        assessment_type,
        assessment_score_bucket,
        date_of_evaluation,
        -- There should only be one compliance metric output per month/supervising_officer_external_id/person_id,
        -- but we do this to ensure a person-based count, prioritizing a case being out of compliance.
        ROW_NUMBER() OVER (PARTITION BY state_code, year, month, supervising_officer_external_id, person_id
         ORDER BY next_recommended_assessment_date DESC NULLS FIRST, next_recommended_face_to_face_date DESC NULLS FIRST) as inclusion_order
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_case_compliance_metrics_materialized`
    WHERE supervising_officer_external_id IS NOT NULL
        AND date_of_evaluation = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)
SELECT DISTINCT
        dates.state_code, 
        dates.person_external_id,
        dates.most_recent_assessment_date,
        state_assessment.assessment_type,
        dates.assessment_score_bucket,
        state_assessment.assessment_score,
        dates.most_recent_face_to_face_date,
        dates.most_recent_home_visit_date,
        dates.next_recommended_assessment_date,
        dates.next_recommended_face_to_face_date,
        dates.next_recommended_home_visit_date,
        IF(dates.next_recommended_assessment_date < CURRENT_DATE(), TRUE, FALSE) AS overdue_for_assessment,
        IF(dates.next_recommended_face_to_face_date < CURRENT_DATE(), TRUE, FALSE) AS overdue_for_face_to_face,
        IF(dates.next_recommended_home_visit_date < CURRENT_DATE(), TRUE, FALSE) AS overdue_for_home_visit,
        FROM dates
        JOIN `{project_id}.state.state_assessment` state_assessment
        ON dates.most_recent_assessment_date = state_assessment.assessment_date
        AND dates.state_code = state_assessment.state_code
        AND dates.person_id = state_assessment.person_id
        AND dates.assessment_type = state_assessment.assessment_type
    WHERE inclusion_order = 1
    """

CONTACTS_AND_ASSESSMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.LINESTAFF_DATA_VALIDATION,
    view_id=CONTACTS_AND_ASSESSMENTS_VIEW_NAME,
    should_materialize=True,
    view_query_template=CONTACTS_AND_ASSESSMENTS_QUERY_TEMPLATE,
    description=CONTACTS_AND_ASSESSMENTS_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CONTACTS_AND_ASSESSMENTS_VIEW_BUILDER.build_and_print()
