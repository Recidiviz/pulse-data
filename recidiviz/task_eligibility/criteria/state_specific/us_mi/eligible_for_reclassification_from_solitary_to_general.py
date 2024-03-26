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
"""This criteria view builder defines spans of time when a resident is eligible for reclassification to GENERAL
POPULATION because either their detention sanction timeframe has expired while in DISCIPLINARY SOLITARY CONFINEMENT
OR they have been in TEMPORARY SOLITARY CONFINEMENT for over 30 days.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    TASK_ELIGIBILITY_CRITERIA_GENERAL,
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    combining_several_criteria_into_one,
    extract_object_from_json,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MI_ELIGIBLE_FOR_RECLASSIFICATION_FROM_SOLITARY_TO_GENERAL"

_DESCRIPTION = """This criteria view builder defines spans of time where a resident is eligible for reclassification to
GENERAL POPULATION because either their detention sanction timeframe has expired while in DISCIPLINARY SOLITARY
CONFINEMENT OR they have been in TEMPORARY SOLITARY CONFINEMENT for over 30 days.
"""

_CRITERIA_QUERY_1 = f"""
    SELECT
        * EXCEPT (reason),
        meets_criteria AS detention_sanction_has_expired,
        {extract_object_from_json(object_column = 'eligible_date', 
                                  object_type = 'DATE')} AS sanction_expiration_date, 
        NULL AS overdue_in_temporary,
        NULL AS overdue_in_temporary_date,
    FROM `{{project_id}}.{{task_eligibility_criteria_us_mi}}.detention_sanction_timeframe_has_expired_materialized`
    """

_CRITERIA_QUERY_2 = f"""
    SELECT
        * EXCEPT (reason),
        NULL AS detention_sanction_has_expired,
        NULL AS sanction_expiration_date,
        meets_criteria AS overdue_in_temporary,
        {extract_object_from_json(object_column = 'eligible_date', 
                                  object_type = 'DATE')} AS overdue_in_temporary_date,        
    FROM `{{project_id}}.{{task_eligibility_criteria_general}}.in_temporary_solitary_confinement_at_least_30_days_materialized`
"""


_JSON_CONTENT = """LOGICAL_OR(detention_sanction_has_expired) AS detention_sanction_has_expired,
                    LOGICAL_OR(overdue_in_temporary) AS overdue_in_temporary,
                    MAX(sanction_expiration_date) AS sanction_expiration_date,
                    MAX(overdue_in_temporary_date) AS overdue_in_temporary_date"""

_QUERY_TEMPLATE = f"""
{combining_several_criteria_into_one(
        select_statements_for_criteria_lst=[_CRITERIA_QUERY_1,
                                             _CRITERIA_QUERY_2],
        meets_criteria="LOGICAL_OR(meets_criteria)",
        json_content=_JSON_CONTENT,
    )}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_MI,
    task_eligibility_criteria_us_mi=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_MI
    ),
    task_eligibility_criteria_general=TASK_ELIGIBILITY_CRITERIA_GENERAL,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
