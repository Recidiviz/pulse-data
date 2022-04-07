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
"""Supervision caseload and district information

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.line_staff_validation.caseload_and_district
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CASELOAD_AND_DISTRICT_VIEW_NAME = "caseload_and_district"

CASELOAD_AND_DISTRICT_DESCRIPTION = """
"""

CASELOAD_AND_DISTRICT_QUERY_TEMPLATE = """
WITH districts as (
    SELECT
        state_code,
        officer_external_id as supervising_officer_external_id,
        ARRAY_AGG(distinct district) as district,
    FROM
    `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
    group by 1,2
),
officers as (
    select 
        state_code,
        external_id as supervising_officer_external_id,
        full_name as officer_name,
    from `{project_id}.{reference_dataset}.augmented_agent_info`
    group by 1,2,3
), caseload as (
    SELECT
        state_code,
        supervising_officer_external_id,
        person_id
    FROM `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized`
    WHERE end_date IS NULL
    group by 1,2,3
), details as (
    SELECT
        state_code,
        supervising_officer_external_id,
        person_id,
        person_external_id,
        supervision_level,
        supervision_type,
        case_type
    from `{project_id}.{materialized_metrics_dataset}.most_recent_single_day_supervision_population_metrics_materialized`
), clients AS (
    select
        state_person.state_code, 
        state_person.person_id,
        full_name,
        current_address,
        external_id AS person_external_id,
    from `{project_id}.{base_dataset}.state_person` state_person
    JOIN `{project_id}.{base_dataset}.state_person_external_id` state_person_external_id
        ON state_person_external_id.state_code = state_person.state_code
        AND state_person_external_id.person_id = state_person.person_id
        AND IF(state_person_external_id.state_code = 'US_PA', state_person_external_id.id_type = 'US_PA_PBPP', TRUE)
)
SELECT
    officers.state_code,
    officer_name,
    supervising_officer_external_id,
    ARRAY_TO_STRING(district, ',') AS district,
    clients.person_external_id AS client_external_id,
    CONCAT(
        COALESCE(JSON_EXTRACT_SCALAR(full_name, '$.given_names'), ""),
        " ",
        COALESCE(JSON_EXTRACT_SCALAR(full_name, '$.middle_names'), ""),
        " ",
        COALESCE(JSON_EXTRACT_SCALAR(full_name, '$.surname'), "")
    ) AS client_name,
    current_address,
    supervision_level,
    supervision_type,
    case_type
FROM officers
LEFT JOIN districts
USING (state_code, supervising_officer_external_id)
LEFT JOIN caseload
USING (state_code, supervising_officer_external_id)
LEFT JOIN details
USING (state_code, person_id, supervising_officer_external_id)
JOIN clients 
USING (state_code, person_id)
ORDER BY supervising_officer_external_id
    """

CASELOAD_AND_DISTRICT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.LINESTAFF_DATA_VALIDATION,
    view_id=CASELOAD_AND_DISTRICT_VIEW_NAME,
    should_materialize=True,
    view_query_template=CASELOAD_AND_DISTRICT_QUERY_TEMPLATE,
    description=CASELOAD_AND_DISTRICT_DESCRIPTION,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    po_report_dataset=dataset_config.PO_REPORT_DATASET,
    reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    base_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASELOAD_AND_DISTRICT_VIEW_BUILDER.build_and_print()
