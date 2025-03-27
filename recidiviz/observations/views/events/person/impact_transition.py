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
# =============================================================================
"""View for an event that has some impact (positive or negative) on a JII's status 
in the corrections system, which we can link (correlationally) with a launched product 
and want to roll up to our org-wide transitions KPI"""


from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.outcome_metrics.view_config import get_unioned_transitions_view_builder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = """View for an event that has some impact (positive or negative) on a JII's status
in the corrections system, which we can link (correlationally) with a launched product
and want to roll up to our org-wide transitions KPI"""


VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.IMPACT_TRANSITION,
    description=_VIEW_DESCRIPTION,
    sql_source=get_unioned_transitions_view_builder().table_for_query,
    attribute_cols=[
        "experiment_id",
        "decarceral_impact_type",
        "system_type",
        "has_mandatory_due_date",
        "is_jii_transition",
        "is_during_or_after_full_state_launch_month",
        "is_within_one_year_before_full_state_launch_month",
        "full_state_launch_date",
        "weight_factor",
        "delta_direction_factor",
        "product_transition_type",
    ],
    event_date_col="event_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
