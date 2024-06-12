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
"""Maine resident metadata"""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_RESIDENT_METADATA_VIEW_NAME = "us_me_resident_metadata"

US_ME_RESIDENT_METADATA_VIEW_DESCRIPTION = """
Maine resident metadata
"""

US_ME_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE = """
    with portion_needed AS (
        SELECT state_code, person_id,
        JSON_VALUE(reason, '$.x_portion_served')
            AS portion_served_needed,
        DATE(JSON_VALUE(reason, '$.eligible_date'))
            AS portion_needed_eligible_date,
        FROM `{project_id}.{us_me_task_eligibility_criteria_dataset}.served_x_portion_of_sentence_materialized`
            AS served_x
        WHERE CURRENT_DATE("US/Eastern")
        BETWEEN served_x.start_date AND IFNULL(DATE_SUB(served_x.end_date, INTERVAL 1 DAY), "9999-12-31")
    ),
    months_remaining AS (
        SELECT state_code, person_id,
        DATE(JSON_VALUE(reason, '$.eligible_date'))
            AS months_remaining_eligible_date,
        FROM `{project_id}.{us_me_task_eligibility_criteria_dataset}.x_months_remaining_on_sentence_materialized` 
            AS months_remaining
        WHERE CURRENT_DATE("US/Eastern")
        BETWEEN months_remaining.start_date AND IFNULL(DATE_SUB(months_remaining.end_date, INTERVAL 1 DAY), "9999-12-31")
    )
    SELECT 
        person_id,
        portion_served_needed, 
        portion_needed_eligible_date,
        GREATEST(portion_needed_eligible_date, months_remaining_eligible_date) AS sccp_eligibility_date
    FROM `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized` AS sessions
    LEFT JOIN portion_needed USING(person_id)
    LEFT JOIN months_remaining USING(person_id)
    WHERE sessions.state_code = "US_ME" 
    AND sessions.compartment_level_1 = "INCARCERATION"
    AND sessions.end_date IS NULL
"""

US_ME_RESIDENT_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=US_ME_RESIDENT_METADATA_VIEW_NAME,
    view_query_template=US_ME_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE,
    description=US_ME_RESIDENT_METADATA_VIEW_DESCRIPTION,
    should_materialize=True,
    sessions_dataset=SESSIONS_DATASET,
    us_me_task_eligibility_criteria_dataset=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_ME
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_RESIDENT_METADATA_VIEW_BUILDER.build_and_print()
