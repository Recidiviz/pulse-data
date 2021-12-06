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
"""Validation looker dashboard view

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.line_staff_validation.looker_dashboard
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LOOKER_DASHBOARD_VIEW_NAME = "looker_dashboard"

LOOKER_DASHBOARD_DESCRIPTION = """
"""

LOOKER_DASHBOARD_QUERY_TEMPLATE = """
SELECT 
    CURRENT_TIMESTAMP() AS report_date,
    caseload.state_code,
    officer_name,
    supervising_officer_external_id AS officer_external_id,
    client_name,
    client_external_id, 
    caseload.district,
    current_address,
    supervision_level,
    supervision_type,
    recommended_downgrades.person_external_id IS NOT NULL AS supervision_level_mismatch,
    recommended_downgrades.recommended_supervision_level,
    overdue_discharges.is_overdue AS overdue_for_discharge,
    overdue_discharges.projected_end_date,
    contacts_and_assessments.overdue_for_assessment,
    contacts_and_assessments.most_recent_assessment_date,
    contacts_and_assessments.assessment_score AS most_recent_assessment_score,
    contacts_and_assessments.next_recommended_assessment_date,
    contacts_and_assessments.overdue_for_face_to_face,
    contacts_and_assessments.most_recent_face_to_face_date,
    contacts_and_assessments.next_recommended_face_to_face_date,
    contacts_and_assessments.overdue_for_home_visit,
    contacts_and_assessments.most_recent_home_visit_date,
    contacts_and_assessments.next_recommended_home_visit_date,
    offense_types.offense_type,
    violations.revocation_violation_type,
    violations.revocation_report_date
FROM `{project_id}.{linestaff_data_validation_dataset}.caseload_and_district_materialized` caseload
LEFT JOIN `{project_id}.{linestaff_data_validation_dataset}.contacts_and_assessments_materialized` contacts_and_assessments
    ON contacts_and_assessments.state_code = caseload.state_code
    AND contacts_and_assessments.person_external_id = caseload.client_external_id
LEFT JOIN `{project_id}.{linestaff_data_validation_dataset}.offense_types_materialized` offense_types
    ON offense_types.state_code = contacts_and_assessments.state_code
    AND offense_types.person_external_id = contacts_and_assessments.person_external_id
LEFT JOIN `{project_id}.{linestaff_data_validation_dataset}.overdue_discharges_materialized` overdue_discharges
    ON overdue_discharges.state_code = contacts_and_assessments.state_code
    AND overdue_discharges.person_external_id = contacts_and_assessments.person_external_id
LEFT JOIN `{project_id}.{linestaff_data_validation_dataset}.violations_materialized` violations
    ON violations.state_code = contacts_and_assessments.state_code
    AND violations.person_external_id = contacts_and_assessments.person_external_id
LEFT JOIN `{project_id}.{linestaff_data_validation_dataset}.recommended_downgrades_materialized` recommended_downgrades
    ON recommended_downgrades.state_code = contacts_and_assessments.state_code
    AND recommended_downgrades.person_external_id = contacts_and_assessments.person_external_id
ORDER BY supervising_officer_external_id;
"""

LOOKER_DASHBOARD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.LINESTAFF_DATA_VALIDATION,
    view_id=LOOKER_DASHBOARD_VIEW_NAME,
    should_materialize=True,
    view_query_template=LOOKER_DASHBOARD_QUERY_TEMPLATE,
    description=LOOKER_DASHBOARD_DESCRIPTION,
    linestaff_data_validation_dataset=dataset_config.LINESTAFF_DATA_VALIDATION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LOOKER_DASHBOARD_VIEW_BUILDER.build_and_print()
