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
    critical_understaffing_needs_quarterly_scheduled_home_contact,
    monthly_home_contact_required,
    needs_scheduled_home_contact_monthly_critical_understaffing,
    needs_scheduled_home_contact_standard_policy,
    needs_scheduled_home_contact_virtual_override_critical_understaffing,
    quarterly_home_contact_required,
    supervision_officer_in_critically_understaffed_location,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateSpecificInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_NEEDS_SCHEDULED_HOME_CONTACT"


VIEW_BUILDER = StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    logic_type=TaskCriteriaGroupLogicType.OR,
    criteria_name=_CRITERIA_NAME,
    sub_criteria_list=[
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.AND,
            criteria_name="US_TX_NEEDS_SCHEDULED_HOME_CONTACT_NOT_CRITICAL_UNDERSTAFFING",
            sub_criteria_list=[
                needs_scheduled_home_contact_standard_policy.VIEW_BUILDER,
                StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
                    sub_criteria=StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
                        logic_type=TaskCriteriaGroupLogicType.AND,
                        criteria_name="US_TX_QUARTERLY_HOME_CONTACT_REQUIRED_AND_SUPERVISION_OFFICER_IN_CRITICALLY_UNDERSTAFFED_LOCATION",
                        sub_criteria_list=[
                            quarterly_home_contact_required.VIEW_BUILDER,
                            supervision_officer_in_critically_understaffed_location.VIEW_BUILDER,
                        ],
                    ),
                ),
                StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
                    sub_criteria=StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
                        logic_type=TaskCriteriaGroupLogicType.AND,
                        criteria_name="US_TX_MONTHLY_HOME_CONTACT_REQUIRED_AND_SUPERVISION_OFFICER_IN_CRITICALLY_UNDERSTAFFED_LOCATION",
                        sub_criteria_list=[
                            monthly_home_contact_required.VIEW_BUILDER,
                            supervision_officer_in_critically_understaffed_location.VIEW_BUILDER,
                        ],
                    ),
                ),
            ],
            allowed_duplicate_reasons_keys=[
                "frequency",
                "type_of_contact",
                "contact_type",
                "frequency_in_months",
                "officer_in_critically_understaffed_location",
            ],
        ),
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
            logic_type=TaskCriteriaGroupLogicType.AND,
            criteria_name="US_TX_NEEDS_SCHEDULED_HOME_CONTACT_QUARTERLY_CRITICAL_UNDERSTAFFING",
            sub_criteria_list=[
                critical_understaffing_needs_quarterly_scheduled_home_contact.VIEW_BUILDER,
                supervision_officer_in_critically_understaffed_location.VIEW_BUILDER,
            ],
            allowed_duplicate_reasons_keys=[
                "frequency",
                "type_of_contact",
                "officer_in_critically_understaffed_location",
            ],
        ),
        needs_scheduled_home_contact_monthly_critical_understaffing.VIEW_BUILDER,
        needs_scheduled_home_contact_virtual_override_critical_understaffing.VIEW_BUILDER,
    ],
    allowed_duplicate_reasons_keys=[
        "frequency",
        "type_of_contact",
        "contact_count",
        "contact_due_date",
        "last_contact_date",
        "contact_type",
        "officer_in_critically_understaffed_location",
        "overdue_flag",
        "period_type",
        "override_contact_type",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
