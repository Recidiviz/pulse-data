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
# =============================================================================
"""Defines a criteria view that shows spans of time for which supervision clients
need a risk assessment conducted
"""
from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    meets_risk_assessment_applicable_conditions,
    meets_risk_assessment_event_triggers,
    not_supervision_within_6_months_of_release_date,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    AndTaskCriteriaGroup,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_NEEDS_RISK_ASSESSMENT"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which supervision clients
need a risk assessment conducted.
"""

VIEW_BUILDER = AndTaskCriteriaGroup(
    criteria_name=_CRITERIA_NAME,
    sub_criteria_list=[
        meets_risk_assessment_applicable_conditions.VIEW_BUILDER,
        meets_risk_assessment_event_triggers.VIEW_BUILDER,
        not_supervision_within_6_months_of_release_date.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "case_type",
    ],
).as_criteria_view_builder

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
