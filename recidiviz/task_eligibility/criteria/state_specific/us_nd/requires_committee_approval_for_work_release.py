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
Certain cases will need further approval from a work/education committee. This is true for 
folks who satisfy one of the following three conditions:
    1. The person is required to serve 85% of their sentence
    2. The person has an Armed Offender Minimum Mandatory Sentence (AOMMS)
    3. The person has registration requirements (e.g. sexual, violent, among others)
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    combining_several_criteria_into_one,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_REQUIRES_COMMITTEE_APPROVAL_FOR_WORK_RELEASE"

_DESCRIPTION = """
Certain cases will need further approval from a work/education committee. This is true for 
folks who satisfy one of the following three conditions:
    1. The person has registration requirements (e.g. sexual, violent, among others)
    2. The person is required to serve 85% of their sentence
    3. The person has an Armed Offender Minimum Mandatory Sentence (AOMMS)
"""
_CRITERIA_QUERY_1 = """
    SELECT
        * EXCEPT (reason),
        TRUE AS has_registration_requirements,
        FALSE AS has_to_serve_85_percent_of_sentence,
        FALSE AS has_an_aomms_sentence,
    FROM `{project_id}.{task_eligibility_criteria_us_nd}.has_registration_requirements_materialized`"""

_CRITERIA_QUERY_2 = """
    SELECT
        * EXCEPT (reason),
        TRUE AS has_to_serve_85_percent_of_sentence,
        FALSE AS has_registration_requirements,
        FALSE AS has_an_aomms_sentence,
    FROM `{project_id}.{task_eligibility_criteria_us_nd}.has_to_serve_85_percent_of_sentence_materialized`"""

_CRITERIA_QUERY_3 = """
    SELECT
        * EXCEPT (reason),
        FALSE AS has_to_serve_85_percent_of_sentence,
        FALSE AS has_registration_requirements,
        TRUE AS has_an_aomms_sentence,
    FROM `{project_id}.{task_eligibility_criteria_us_nd}.has_an_aomms_sentence_materialized`"""

_JSON_CONTENT = """LOGICAL_OR(has_registration_requirements) AS has_registration_requirements,
                    LOGICAL_OR(has_to_serve_85_percent_of_sentence) AS has_to_serve_85_percent_of_sentence,
                    LOGICAL_OR(has_an_aomms_sentence) AS has_an_aomms_sentence"""

_QUERY_TEMPLATE = f"""
{combining_several_criteria_into_one(
        select_statements_for_criteria_lst=[_CRITERIA_QUERY_1,
                                             _CRITERIA_QUERY_2,
                                             _CRITERIA_QUERY_3],
        meets_criteria="LOGICAL_OR(meets_criteria)",
        json_content=_JSON_CONTENT,
    )}"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_ND,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    task_eligibility_criteria_us_nd=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_ND
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
