# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Maine state-specific preprocessing for work-release sessions."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_NAME = (
    "us_me_work_release_sessions_preprocessing"
)

US_ME_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION = """
Maine state-specific preprocessing for work_release sessions. We are assuming that all
people in 'Community' custody level are in allowed to go into work release. This is a 
safe assumption according to MEDOC"""

# TODO(#16722): pull custody level from ingested data once it is hydrated in our schema
US_ME_WORK_RELEASE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE = f"""
WITH cl_is_community AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date AS end_date_exclusive,
        meets_criteria
    FROM `{{project_id}}.{{task_eligibility_criteria_us_me}}.custody_level_is_community_materialized`
    WHERE meets_criteria
)

SELECT 
    state_code, 
    person_id,
    start_date,
    end_date_exclusive,
FROM ({aggregate_adjacent_spans(table_name='cl_is_community',
                                end_date_field_name="end_date_exclusive",
                                attribute = 'meets_criteria')})
"""

US_ME_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ME_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_NAME,
    description=US_ME_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_ME_WORK_RELEASE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE,
    task_eligibility_criteria_us_me=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_ME
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_BUILDER.build_and_print()
