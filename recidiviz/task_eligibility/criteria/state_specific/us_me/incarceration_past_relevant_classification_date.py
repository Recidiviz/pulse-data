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
# ============================================================================
"""Defines a criteria span view that shows spans of time during which
someone is past their annual or semi-annual reclassification date"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_INCARCERATION_PAST_RELEVANT_CLASSIFICATION_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is past their annual or semi-annual reclassification date"""

_QUERY_TEMPLATE = """
SELECT *
FROM `{project_id}.{task_eligibility_criteria_us_me}.incarceration_past_annual_classification_date_materialized`

UNION ALL

SELECT *
FROM `{project_id}.{task_eligibility_criteria_us_me}.incarceration_past_semi_annual_classification_date_materialized`
"""
VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_ME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=True,
    task_eligibility_criteria_us_me=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_ME
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
