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
# ============================================================================
"""This criteria view builder defines spans of time where clients are on a non electronic monitoring supervision level
preceded immediately by a supervision level that includes electronic monitoring.
"""
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_COMPLETED_ELECTRONIC_MONITORING"

_DESCRIPTION = """This criteria view builder defines spans of time where clients are on a non electronic monitoring supervision level
preceded immediately by a supervision level that includes electronic monitoring. 
"""

_QUERY_TEMPLATE = f"""
WITH em_spans AS (
/* This CTE creates an is_electronic_monitoring variable that sets a boolean for whether the supervision level
includes electronic monitoring */
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        COALESCE(map.is_em, FALSE) AS is_electronic_monitoring
    FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_raw_text_sessions_materialized` sls
    LEFT JOIN `{{project_id}}.{{analyst_data_dataset}}.us_mi_supervision_level_raw_text_mappings` map
        ON sls.supervision_level_raw_text = map.supervision_level_raw_text
    WHERE state_code = "US_MI"
),
/* This boolean is then used to aggregate electronic monitoring supervision levels and non electronic monitoring
supervision levels */
sessionized_cte AS (
    {aggregate_adjacent_spans(table_name='em_spans',
                       attribute=['is_electronic_monitoring'],
                       session_id_output_name='supervision_level_session_id')}
)
/* Finally, if the current supervision level does not include electronic monitoring, but the previous level does,
then meets_criteria is TRUE. This logic takes into account that when a person on EM has a previous supervision level 
that is also EM there is a gap in dates of supervision and therefore meets_criteria will be FALSE.
Therefore, we look for cases when a person is not on EM and is preceded by a period of time on EM because that 
represents time on supervision immediately following EM. */
    SELECT
        * EXCEPT (is_electronic_monitoring),
        (COALESCE(LAG(is_electronic_monitoring) OVER w, FALSE) AND NOT is_electronic_monitoring) AS meets_criteria,
        TO_JSON(STRUCT(start_date AS eligible_date)) AS reason,
    FROM sessionized_cte
    WINDOW w AS (PARTITION BY person_id, state_code ORDER BY start_date)
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        sessions_dataset=SESSIONS_DATASET,
        analyst_data_dataset=ANALYST_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
