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
"""This criteria view builder defines spans of time that clients are on supervision after participating in
 SAI (Special Alternative Incarceration) based on supervision level raw text values that contain SAI.
"""
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

_CRITERIA_NAME = "US_MI_SUPERVISION_LEVEL_IS_SAI"

_DESCRIPTION = """This criteria view builder defines spans of time that clients are on supervision after participating
in SAI (Special Alternative Incarceration).
"""

_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        TRUE AS meets_criteria,
        TO_JSON(STRUCT(map.description AS supervision_level_raw_text)) AS reason,
    FROM `{project_id}.{sessions_dataset}.supervision_level_raw_text_sessions_materialized` sls
    LEFT JOIN `{project_id}.{analyst_data_dataset}.us_mi_supervision_level_raw_text_mappings` map
        ON sls.supervision_level_raw_text = map.supervision_level_raw_text
    WHERE state_code = "US_MI"
    AND map.is_sai
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
