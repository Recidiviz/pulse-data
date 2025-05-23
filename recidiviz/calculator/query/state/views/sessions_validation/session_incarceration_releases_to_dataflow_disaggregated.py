# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""A view which provides a person / day level comparison between incarceration session ends and dataflow releases"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    SESSIONS_VALIDATION_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME = (
    "session_incarceration_releases_to_dataflow_disaggregated"
)

SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION = """
    A view which provides a person / day level comparison between incarceration session ends and dataflow releases. For 
    each person / day there are a set of binary variables that indicate whether that record meets a criteria. The
    key binary indicator columns are the following:

    - session_transition:   indicates a session compartment transition occurring for a given person on given day
    - dataflow_event:       indicates a valid dataflow event (defined in compartment_session_start_reasons or
                            compartment_session_end_reasons). This excludes (1) events labeled as transfers within state,
                            (2) events labeled with unknown reasons, (3) events that are same day admission/releases,
                            (4) supervision events that occur while a person is incarcerated.
    - event_in_both:        indicates a person/day is both associated with a session transition and a valid dataflow event
    - session_transition_inferred_reason:   indicates that the session transition has an inferred start/end reason
    - session_transition_hydrated:  indicates that the session transition has a start/end reason hydration either from
                            dataflow or from inference.
"""

SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE = """
    WITH dataflow AS 
    (
    SELECT 
        *,
        CAST(valid_dataflow_event AS INT64) AS dataflow_event,
    FROM `{project_id}.{sessions_dataset}.compartment_session_end_reasons_materialized`
    WHERE compartment_level_1 IN ('INCARCERATION','INCARCERATION_OUT_OF_STATE')
    )
    ,
    sessions AS
    (
    SELECT 
        *,
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` 
    WHERE compartment_level_1 IN ('INCARCERATION','INCARCERATION_OUT_OF_STATE')
    )
    ,
    joined_cte AS
    (
    SELECT
        person_id,
        state_code,
        end_date AS date,
        sessions.session_id,
        dataflow.end_reason AS dataflow_reason,
        dataflow.end_reason_raw_text AS dataflow_reason_raw_text,
        CASE WHEN sessions.person_id IS NOT NULL THEN 1 ELSE 0 END as session_transition,
        COALESCE(dataflow.dataflow_event, 0) AS dataflow_event,
        CASE WHEN dataflow.dataflow_event = 1 AND sessions.person_id IS NOT NULL THEN 1 ELSE 0 END AS event_in_both,
        COALESCE(sessions.is_inferred_end_reason, 0) AS session_transition_inferred_reason,
        in_incarceration_population_on_date AS dataflow_event_in_incarceration_population_on_date,
        in_supervision_population_on_date AS dataflow_event_in_supervision_population_on_date,
        same_day_start_end AS dataflow_event_same_day_start_end,
        sessions.compartment_level_1,
        sessions.compartment_level_2,
        sessions.outflow_to_level_1 AS transition_level_1,
        sessions.outflow_to_level_2 AS transition_level_2,
    FROM sessions
    FULL OUTER JOIN dataflow
        USING(person_id, end_date, state_code)
    )
    SELECT 
        'INCARCERATION_RELEASES' AS metric,
        *,
        GREATEST(session_transition_inferred_reason, event_in_both) AS session_transition_hydrated
    FROM joined_cte
    WHERE EXTRACT(YEAR FROM date) > EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 20 YEAR))
    """

SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_VALIDATION_DATASET,
    view_id=SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME,
    view_query_template=SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE,
    description=SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.build_and_print()
