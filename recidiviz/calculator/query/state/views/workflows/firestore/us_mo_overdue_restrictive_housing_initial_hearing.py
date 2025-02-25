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
"""Query for relevant metadata needed to support initial Restrictive Housing hearing opportunity in Missouri
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_VIEW_NAME = (
    "us_mo_overdue_restrictive_housing_initial_hearing_record"
)

US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support initial Restrictive Housing hearing opportunity in Missouri 
    """

US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_QUERY_TEMPLATE = f"""
    WITH base_query AS ({join_current_task_eligibility_spans_with_external_id(
        state_code='"US_MO"',
        tes_task_query_view="overdue_restrictive_housing_initial_hearing_materialized",
        id_type='"US_MO_DOC"',
    )})
    ,
    eligible_and_almost_eligible AS (

        SELECT
            IF(
                is_eligible,
                "OVERDUE",
                IF(
                    is_almost_eligible,
                    "THIS_WEEK",
                    "UPCOMING"
                )
            ) AS eligibility_category,
            base_query.*
        FROM base_query
        WHERE 
            'US_MO_IN_RESTRICTIVE_HOUSING' NOT IN UNNEST(ineligible_criteria)
            AND 'US_MO_NO_HEARING_AFTER_RESTRICTIVE_HOUSING_START' NOT IN UNNEST(ineligible_criteria)
            AND 'US_MO_NO_D1_SANCTION_AFTER_RESTRICTIVE_HOUSING_START' NOT IN UNNEST(ineligible_criteria)
    )
    SELECT
        base.external_id,
        base.state_code,
        base.reasons,
        base.is_eligible,
        base.eligibility_category,
        base.ineligible_criteria,
        full_record.* EXCEPT(external_id, person_id, state_code)
    FROM eligible_and_almost_eligible base
    LEFT JOIN `{{project_id}}.analyst_data.us_mo_restrictive_housing_record_materialized` full_record
    USING (person_id)
"""

US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_VIEW_NAME,
    view_query_template=US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_QUERY_TEMPLATE,
    description=US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_MO
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_OVERDUE_RESTRICTIVE_HOUSING_INITIAL_HEARING_RECORD_VIEW_BUILDER.build_and_print()
