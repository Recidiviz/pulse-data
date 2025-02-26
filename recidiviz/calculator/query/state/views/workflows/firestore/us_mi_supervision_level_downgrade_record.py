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
"""Query for clients eligible for supervision level downgrade in Michigan
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_NAME = (
    "us_mi_supervision_level_downgrade_record"
)

US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_DESCRIPTION = """
    Query for clients eligible for supervision level downgrade in Michigan
    """
US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_QUERY_TEMPLATE = f"""
WITH compas_recommended_preprocessed AS (
SELECT 
    person_id,
    IF(assessment_level = 'MEDIUM_HIGH', 'MEDIUM', assessment_level) AS recommended_supervision_level 
FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` sc
WHERE state_code = "US_MI" 
QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id ORDER BY assessment_date DESC, assessment_id)=1
) 
SELECT
    pei.external_id,
    tes.state_code,
    TO_JSON([STRUCT(NULL AS note_title, COALESCE(c.recommended_supervision_level, 'MEDIUM') AS note_body, NULL as event_date, 
                                                        "Recommended supervision level" AS criteria)]) AS case_notes,
    reasons,
FROM `{{project_id}}.{{task_eligibility_dataset}}.supervision_level_downgrade_materialized` tes
INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    ON tes.state_code = pei.state_code 
    AND tes.person_id = pei.person_id
    AND pei.id_type = "US_MI_DOC"
--left join since a client might not have assessment data, if that's the case, they should be recommended for MEDIUM
LEFT JOIN compas_recommended_preprocessed c
    ON c.person_id = tes.person_id
WHERE CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND {nonnull_end_date_exclusive_clause('tes.end_date')}
    AND tes.is_eligible
    AND tes.state_code = 'US_MI'
"""

US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_NAME,
    view_query_template=US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_QUERY_TEMPLATE,
    description=US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_MI
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_SUPERVISION_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.build_and_print()
