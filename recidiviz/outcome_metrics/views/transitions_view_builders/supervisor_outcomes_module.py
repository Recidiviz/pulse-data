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
"""View containing transition events associated with the supervisor outcomes module
for org-wide impact tracking"""

from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType
from recidiviz.observations.observation_selector import ObservationSelector
from recidiviz.outcome_metrics.impact_transitions_view_builder import (
    ImpactTransitionsBigQueryViewBuilder,
)
from recidiviz.outcome_metrics.product_transition_type import ProductTransitionType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = """View containing transition events associated with the supervisor outcomes module
for org-wide impact tracking"""

_OBSERVATIONS_QUERY_TEMPLATE = "\nUNION ALL\n".join(
    [
        ObservationSelector.build_selected_observations_query_template(
            observation_type=EventType.INCARCERATION_START_AND_INFERRED_START,
            observation_selectors=[
                EventSelector(
                    event_type=EventType.INCARCERATION_START_AND_INFERRED_START,
                    event_conditions_dict={},
                ),
            ],
            output_attribute_columns=[],
        ),
        ObservationSelector.build_selected_observations_query_template(
            observation_type=EventType.ABSCONSION_BENCH_WARRANT,
            observation_selectors=[
                EventSelector(
                    event_type=EventType.ABSCONSION_BENCH_WARRANT,
                    event_conditions_dict={},
                ),
            ],
            output_attribute_columns=[],
        ),
    ]
)

_SOURCE_DATA_QUERY_TEMPLATE = f"""
WITH transitions AS (
    {_OBSERVATIONS_QUERY_TEMPLATE}
)
,
transitions_with_experiment_assignments AS (
    SELECT
        transitions.*,
        launches.experiment_id,
        -- The flags below are only relevant to the Workflows product, so we leave them
        -- as null.
        "NOT_APPLICABLE" AS decarceral_impact_type,
        FALSE AS has_mandatory_due_date,
        FALSE AS is_jii_transition,
        "SUPERVISION" AS system_type,
        launches.launch_date AS full_state_launch_date,
    FROM
        transitions
    -- Join to experiment associated with full-state supervisor homepage launch
    INNER JOIN
        `{{project_id}}.transitions.all_full_state_launch_dates_materialized` launches
    ON
        transitions.state_code = launches.state_code
        AND (launches.experiment_id LIKE "%OUTLIERS%"
            -- For MI, exclude the SUPERVISOR_HOMEPAGE_OUTCOMES_V2 launch and use US_MI_OUTLIERS_WEB_TOOL_CARCERAL_SUPERVISION_METRICS instead
            OR (launches.experiment_id LIKE '%SUPERVISOR_HOMEPAGE_OUTCOMES_V2%' AND launches.state_code != 'US_MI'))
)
SELECT * FROM transitions_with_experiment_assignments
"""

VIEW_BUILDER: ImpactTransitionsBigQueryViewBuilder = (
    ImpactTransitionsBigQueryViewBuilder(
        description=_VIEW_DESCRIPTION,
        query_template=_SOURCE_DATA_QUERY_TEMPLATE,
        weight_factor=0.3,
        delta_direction_factor=-1,
        product_transition_type=ProductTransitionType.SUPERVISOR_OUTCOMES_MODULE,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
