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
"""
Defines a criteria span view that shows spans of time during which
someone has not been involved with absconsion, escape, or eluding police 
in the previous 10 years.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    TASK_ELIGIBILITY_CRITERIA_GENERAL,
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    ix_combining_several_criteria_into_one_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "US_IX_NO_ABSCONSION_ESCAPE_AND_ELUDING_POLICE_OFFENSES_WITHIN_10_YEARS"
)

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone has not been involved with absconsion, escape, or eluding police 
in the previous 10 years.
"""

_CRITERIA_QUERY_1 = """
    SELECT 
        * EXCEPT (reason),
        SAFE_CAST(JSON_VALUE(reason, '$.most_recent_absconded_date') AS DATE) AS most_recent_absconded_date,
        NULL AS most_recent_escape_date,
        NULL AS most_recent_eluding_police_date,
    FROM `{project_id}.{task_eligibility_criteria_general}.no_absconsion_within_10_years_materialized`
    WHERE state_code = 'US_IX'
"""

_CRITERIA_QUERY_2 = """
    SELECT 
        * EXCEPT (reason),
        NULL AS most_recent_absconded_date,
        NULL AS most_recent_escape_date,
        SAFE_CAST(JSON_VALUE(reason, '$.most_recent_statute_date') AS DATE) AS most_recent_eluding_police_date,
    FROM `{project_id}.{task_eligibility_criteria_us_ix}.no_eluding_police_offense_within_10_years_materialized`
"""

_CRITERIA_QUERY_3 = """
    SELECT 
        * EXCEPT (reason),
        NULL AS most_recent_absconded_date,
        SAFE_CAST(JSON_VALUE(reason, '$.most_recent_statute_date') AS DATE) AS most_recent_escape_date,
        NULL AS most_recent_eluding_police_date,
    FROM `{project_id}.{task_eligibility_criteria_us_ix}.no_escape_offense_within_10_years_materialized`
"""

_JSON_CONTENT = """MAX(most_recent_escape_date) AS most_recent_escape_date,
                   MAX(most_recent_eluding_police_date) AS most_recent_eluding_police_date,
                   MAX(most_recent_absconded_date) AS most_recent_absconded_date"""

_QUERY_TEMPLATE = f"""
{ix_combining_several_criteria_into_one_view_builder(
        select_statements_for_criteria_lst=[_CRITERIA_QUERY_1,
                                            _CRITERIA_QUERY_2,
                                            _CRITERIA_QUERY_3],
        meets_criteria="LOGICAL_AND(num_criteria>=3)",
        json_content=_JSON_CONTENT,
    )}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_IX,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    task_eligibility_criteria_general=TASK_ELIGIBILITY_CRITERIA_GENERAL,
    task_eligibility_criteria_us_ix=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_IX
    ),
    meets_criteria_default=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
