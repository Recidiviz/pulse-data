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
"""Client conditions that a PO could take action on as of the current day.

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.po_report.current_action_items_by_person
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CURRENT_ACTION_ITEMS_BY_PERSON_VIEW_NAME = "current_action_items_by_person"

CURRENT_ACTION_ITEMS_BY_PERSON_DESCRIPTION = """
Client conditions that a PO could take action on as of the current day. 
Unlike retrospective report data, this should be as up to date as possible.
"""

CURRENT_ACTION_ITEMS_BY_PERSON_QUERY_TEMPLATE = """
    /*{description}*/
    WITH current_mismatches AS (
        SELECT 
            state_code,
            person_external_id, 
            etl_opportunities_materialized.supervising_officer_external_id AS officer_external_id,
            JSON_EXTRACT_SCALAR(opportunity_metadata, '$.recommendedSupervisionLevel') as recommended_level,
            supervision_level AS current_supervision_level,
        FROM `{project_id}.{case_triage_dataset}.etl_opportunities_materialized` etl_opportunities_materialized
        INNER JOIN `{project_id}.{case_triage_dataset}.etl_clients_materialized` USING (state_code, person_external_id)
        WHERE opportunity_type = 'OVERDUE_DOWNGRADE'
    )
    SELECT 
      person.full_name,
      current_mismatches.state_code, 
      current_mismatches.person_external_id, 
      current_mismatches.officer_external_id,
      recommended_level,
      current_supervision_level,
    FROM current_mismatches
    INNER JOIN `{project_id}.{state_dataset}.state_person_external_id` state_person_external_id
        ON (current_mismatches.person_external_id = state_person_external_id.external_id)
    INNER JOIN `{project_id}.{state_dataset}.state_person` person USING (person_id)
"""

CURRENT_ACTION_ITEMS_BY_PERSON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=CURRENT_ACTION_ITEMS_BY_PERSON_VIEW_NAME,
    should_materialize=True,
    view_query_template=CURRENT_ACTION_ITEMS_BY_PERSON_QUERY_TEMPLATE,
    description=CURRENT_ACTION_ITEMS_BY_PERSON_DESCRIPTION,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    case_triage_dataset=VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_ACTION_ITEMS_BY_PERSON_VIEW_BUILDER.build_and_print()
