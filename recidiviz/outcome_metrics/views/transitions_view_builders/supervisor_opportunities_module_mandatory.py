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
"""View containing mandatory overdue transition events associated with the supervisor opportunities module
for org-wide impact tracking"""

from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType
from recidiviz.outcome_metrics.impact_transitions_view_builder import (
    ImpactTransitionsBigQueryViewBuilder,
)
from recidiviz.outcome_metrics.product_transition_type import ProductTransitionType
from recidiviz.outcome_metrics.views.transitions_metric_utils import (
    get_opportunities_module_transitions_query_template_for_observations,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = """View containing mandatory overdue transition events associated with the supervisor opportunities module
for org-wide impact tracking"""

_SOURCE_DATA_QUERY_TEMPLATE = get_opportunities_module_transitions_query_template_for_observations(
    event_selector=EventSelector(
        event_type=EventType.TASK_COMPLETED,
        event_conditions_dict={
            "task_type_is_fully_launched": ["true"],
            # For mandatory workflows, the `is_eligible` flag indicates that the client
            # was overdue for the opportunity.
            "is_eligible": ["true"],
            "has_mandatory_due_date": ["true"],
            "system_type": ["SUPERVISION"],
        },
    ),
    output_attribute_columns=[
        "task_type",
        "decarceral_impact_type",
        "system_type",
        "has_mandatory_due_date",
        "is_jii_decarceral_transition",
    ],
)

VIEW_BUILDER: ImpactTransitionsBigQueryViewBuilder = ImpactTransitionsBigQueryViewBuilder(
    description=_VIEW_DESCRIPTION,
    query_template=_SOURCE_DATA_QUERY_TEMPLATE,
    weight_factor=1,
    delta_direction_factor=-1,
    product_transition_type=ProductTransitionType.SUPERVISOR_OPPORTUNITIES_MODULE_MANDATORY,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
