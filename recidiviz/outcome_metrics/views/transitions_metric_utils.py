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
"""Contains helpers for calculating metrics using transitions observations"""


from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.observation_selector import ObservationSelector


def get_workflows_transitions_query_template_for_observations(
    event_selector: EventSelector,
    output_attribute_columns: list[str],
) -> str:
    """Returns a query template that joins the input set of observations to the
    relevant launch and experiments information.
    """
    observations_query_template = (
        ObservationSelector.build_selected_observations_query_template(
            observation_type=event_selector.event_type,
            observation_selectors=[event_selector],
            output_attribute_columns=output_attribute_columns,
        )
    )
    query_template = f"""
WITH transitions AS (
    {observations_query_template}
)
,
transitions_with_experiment_assignments AS (
    SELECT
        transitions.* EXCEPT(is_jii_decarceral_transition), 
        is_jii_decarceral_transition AS is_jii_transition,
        experiments.experiment_id,
        launches.launch_date AS full_state_launch_date,
    FROM
        transitions
    LEFT JOIN
        `{{project_id}}.reference_views.workflows_opportunity_configs_materialized` experiments
    ON
        transitions.state_code = experiments.state_code
        AND transitions.task_type = experiments.completion_event_type
    LEFT JOIN
        `{{project_id}}.transitions.all_full_state_launch_dates_materialized` launches
    ON
        transitions.state_code = launches.state_code
        AND experiments.experiment_id = launches.experiment_id
    QUALIFY 
        ROW_NUMBER() OVER (
            PARTITION BY transitions.person_id, transitions.state_code, transitions.task_type, transitions.event_date, experiments.experiment_id
            ORDER BY experiments.opportunity_type
        ) = 1
)
SELECT * FROM transitions_with_experiment_assignments"""
    return query_template
