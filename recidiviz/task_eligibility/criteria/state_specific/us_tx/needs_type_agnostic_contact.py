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
"""
Defines a criteria span view that shows spans of time during which a client
requires a scheduled home contact, taking into account both standard and
critical understaffing policies.
"""

from recidiviz.task_eligibility.criteria.state_specific.us_tx import (
    needs_type_agnostic_contact_monthly_home_critical_understaffing,
    needs_type_agnostic_contact_standard_policy,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_NEEDS_TYPE_AGNOSTIC_CONTACT"


VIEW_BUILDER = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name=_CRITERIA_NAME,
    sub_criteria_list=[
        needs_type_agnostic_contact_standard_policy.VIEW_BUILDER,
        needs_type_agnostic_contact_monthly_home_critical_understaffing.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "last_contact_date",
        "contact_due_date",
        "types_and_amounts_due",
        "types_and_amounts_done",
        "period_type",
        "overdue_flag",
        "frequency",
        "contact_types_accepted",
        "supervision_level",
        "case_type",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
