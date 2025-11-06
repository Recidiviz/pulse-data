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
"""Describes the spans of time when a SO case type NE client has just started supervision (therefore they need their
first STABLE assessment), or when 12 months have passed since a SO case type NE client's last STABLE assessment
(therefore they need an updated STABLE assessment)
"""

from recidiviz.calculator.query.state.views.tasks.compliance_type import ComplianceType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.candidate_populations.general import (
    parole_active_supervision_including_null_supervision_level_population,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria.general import (
    on_parole_at_least_10_years,
    supervision_case_type_is_not_sex_offense,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ne import (
    meets_stable_assessment_event_triggers,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_NE,
    task_name="needs_stable_assessment",
    candidate_population_view_builder=parole_active_supervision_including_null_supervision_level_population.VIEW_BUILDER,
    criteria_spans_view_builders=[
        meets_stable_assessment_event_triggers.VIEW_BUILDER,
        StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=supervision_case_type_is_not_sex_offense.VIEW_BUILDER
        ),
        StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=on_parole_at_least_10_years.VIEW_BUILDER
        ),
    ],
    compliance_type=ComplianceType.ASSESSMENT,
    due_date_field="assessment_due_date",
    last_task_completed_date_field="most_recent_assessment_date",
    due_date_criteria_builder=meets_stable_assessment_event_triggers.VIEW_BUILDER,
    last_task_completed_date_criteria_builder=meets_stable_assessment_event_triggers.VIEW_BUILDER,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
