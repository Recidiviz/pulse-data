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
"""Person-level data to populate the monthly PO report email."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.dataset_config import PO_REPORT_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REPORT_DATA_BY_PERSON_BY_MONTH_VIEW_NAME = "report_data_by_person_by_month"

REPORT_DATA_BY_PERSON_BY_MONTH_DESCRIPTION = """
 Person-level data regarding early discharges, successful supervision completions, reported recommendations for
 absconsions and revocations, and case compliance statuses.
 """

REPORT_DATA_BY_PERSON_BY_MONTH_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      person.state_code, year, month, person.person_id,
      person_external_id.external_id AS person_external_id,
      CONCAT(
        REPLACE(JSON_EXTRACT(person.full_name, '$.surname'), '"', ''),
        ', ',
        REPLACE(JSON_EXTRACT(person.full_name, '$.given_names'), '"', '')
      ) AS full_name,
      officer_external_id,
      successful_completion_date,
      latest_supervision_downgrade_date,
      previous_supervision_level,
      supervision_level,
      violation_type AS revocation_violation_type,
      revocation_report_date,
      absconsion_report_date,
      earned_discharge_date,
      IFNULL(assessment_count, 0) AS assessment_count,
      next_recommended_assessment_date,
      IFNULL(face_to_face_count, 0) AS face_to_face_count,
      next_recommended_face_to_face_date

    FROM `{project_id}.{po_report_dataset}.successful_supervision_completions_by_person_by_month_materialized` completions

    FULL OUTER JOIN `{project_id}.{po_report_dataset}.supervision_downgrade_by_person_by_month_materialized` downgrades
      USING (state_code, year, month, person_id, officer_external_id)

    FULL OUTER JOIN `{project_id}.{po_report_dataset}.supervision_compliance_by_person_by_month_materialized` compliance
      USING (state_code, year, month, person_id, officer_external_id)

    FULL OUTER JOIN `{project_id}.{po_report_dataset}.revocation_reports_by_person_by_month_materialized` revocations
      USING (state_code, year, month, person_id, officer_external_id)

    FULL OUTER JOIN `{project_id}.{po_report_dataset}.absconsion_reports_by_person_by_month_materialized` absconsions
      USING (state_code, year, month, person_id, officer_external_id)

    FULL OUTER JOIN `{project_id}.{po_report_dataset}.supervision_earned_discharge_requests_by_person_by_month_materialized` earned_discharges
      USING (state_code, year, month, person_id, officer_external_id)

    JOIN `{project_id}.{state_dataset}.state_person` person
      USING (person_id, state_code)
    
    JOIN `{project_id}.{state_dataset}.state_person_external_id` person_external_id
      ON person.person_id = person_external_id.person_id
      AND {state_specific_external_id_type} = person_external_id.id_type 
    """

REPORT_DATA_BY_PERSON_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=REPORT_DATA_BY_PERSON_BY_MONTH_VIEW_NAME,
    should_materialize=True,
    view_query_template=REPORT_DATA_BY_PERSON_BY_MONTH_QUERY_TEMPLATE,
    description=REPORT_DATA_BY_PERSON_BY_MONTH_DESCRIPTION,
    po_report_dataset=PO_REPORT_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    state_specific_external_id_type=state_specific_query_strings.state_specific_external_id_type(
        state_code_table_prefix="person"
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REPORT_DATA_BY_PERSON_BY_MONTH_VIEW_BUILDER.build_and_print()
