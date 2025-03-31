# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
someone meets all criteria for proximity to release points.
"""
from recidiviz.task_eligibility.criteria.state_specific.us_ix import (
    meets_date_based_criteria_for_proximity_to_release_points,
    not_mandatory_overrides_for_reclassification,
    q1_score_is_high,
    q5_score_is_zero_or_negative,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    AndTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which
someone meets all criteria for proximity to release points: 
    - currently serving a high severity crime (score of 9 for q1)
    - score of 0 or -1 for q5
    - meets date based criteria
    - no mandatory overrides
"""

VIEW_BUILDER = AndTaskCriteriaGroup(
    criteria_name="US_IX_ELIGIBLE_FOR_PROXIMITY_TO_RELEASE_POINTS",
    sub_criteria_list=[
        q5_score_is_zero_or_negative.VIEW_BUILDER,
        q1_score_is_high.VIEW_BUILDER,
        not_mandatory_overrides_for_reclassification.VIEW_BUILDER,
        meets_date_based_criteria_for_proximity_to_release_points.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "full_term_completion_date",
        "group_projected_parole_release_date",
        "eligible_offenses",
        "next_parole_hearing_date",
    ],
    reasons_aggregate_function_override={"eligible_offenses": "ARRAY_CONCAT_AGG"},
).as_criteria_view_builder

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
