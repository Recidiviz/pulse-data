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
"""Query for clients eligible or almost eligible for reclassification to general population from
solitary confinement in Michigan"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ID_TYPE_BOOK = "US_MI_DOC_BOOK"
ID_TYPE = "US_MI_DOC"

US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_NAME = (
    "us_mi_complete_reclassification_to_general_population_request_record"
)

US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_DESCRIPTION = """
    Query for clients eligible or almost eligible for reclassification to general population from 
    solitary confinement in Michigan
"""

US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_QUERY_TEMPLATE = f"""

WITH eligible_and_almost_eligible AS (
/* Queries all current eligible or almost eligible spans */
    {join_current_task_eligibility_spans_with_external_id(
        state_code= "'US_MI'",
        tes_task_query_view = 'complete_reclassification_to_general_population_request_materialized',
        id_type = "'US_MI_DOC'",
        eligible_and_almost_eligible_only=True,
    )})
SELECT 
    a.external_id,
    a.state_code,
    a.reasons,
    a.is_eligible,
    a.ineligible_criteria,
    h.housing_unit_type AS metadata_solitary_confinement_type,
    DATE_DIFF(CURRENT_DATE('US/Eastern'), h.start_date, DAY) AS metadata_days_in_solitary
FROM eligible_and_almost_eligible a
LEFT JOIN
    `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized` h
        ON h.person_id = a.person_id 
        AND CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_exclusive_clause('h.end_date_exclusive')}
"""

US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_NAME,
    view_query_template=US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_QUERY_TEMPLATE,
    description=US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_MI
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_COMPLETE_RECLASSIFICATION_TO_GENERAL_POPULATION_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
