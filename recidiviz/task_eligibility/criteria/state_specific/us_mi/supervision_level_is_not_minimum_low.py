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
"""This criteria view builder defines spans of time that a client is on Minimum Low Supervision
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

_CRITERIA_NAME = "US_MI_SUPERVISION_LEVEL_IS_NOT_MINIMUM_LOW"

_DESCRIPTION = """This criteria view builder defines spans of time that a client is on Minimum Low Supervision
"""

_QUERY_TEMPLATE = f"""
#TODO(#22511) refactor to build off of a general criteria view builder
WITH minimum_low_spans AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
    #TODO(#20035) replace with supervision level raw text sessions once views agree
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized` sls
    /* Using regex to match digits in sls.correctional_level_raw_text
   so imputed values with the form d*##IMPUTED are correctly joined */
    LEFT JOIN `{{project_id}}.{{analyst_data_dataset}}.us_mi_supervision_level_raw_text_mappings` omni_map
        ON REGEXP_EXTRACT(sls.correctional_level_raw_text, r'(\\d+)') \
         = omni_map.supervision_level_raw_text \
        AND omni_map.source = "OMNI"
    LEFT JOIN `{{project_id}}.{{analyst_data_dataset}}.us_mi_supervision_level_raw_text_mappings` coms_map
        ON REPLACE(sls.correctional_level_raw_text, "##IMPUTED", "") \
        = coms_map.supervision_level_raw_text \
        AND coms_map.source = 'COMS'
    WHERE state_code = "US_MI"
    AND compartment_level_1 IN ('SUPERVISION')
    AND COALESCE(omni_map.is_minimum_low, coms_map.is_minimum_low)
)
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
    TO_JSON(STRUCT(TRUE AS supervision_level_is_minimum_low)) AS reason,
    FROM ({aggregate_adjacent_spans(table_name='minimum_low_spans')})
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        sessions_dataset=SESSIONS_DATASET,
        analyst_data_dataset=ANALYST_VIEWS_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
