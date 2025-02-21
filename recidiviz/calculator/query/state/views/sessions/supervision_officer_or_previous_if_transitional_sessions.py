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
"""Sessionized view of each individual on supervision. Session defined as continuous time on caseload of a given
supervising officer, including for non-transitional officers, adjacent sessions where a clients is supervised by
a transitional officer.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL_SESSIONS_VIEW_NAME = (
    "supervision_officer_or_previous_if_transitional_sessions"
)

SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL_SESSIONS_VIEW_DESCRIPTION = """
Sessionized view of each individual on supervision. Session defined as continuous time on the caseload of a given 
supervising officer, including for non-transitional officers, adjacent sessions where a clients is supervised by
a transitional officer.

Transitional caseloads specify officers that are assigned clients in transition, for example, after warrants are served 
and a client is transitioning to sentencing. Some examples include p&p specialists, absconsion specialists, and the
parole commission. 

For these officers, we often want to know the most recent officer they were assigned to for purposes
of tracking events by officer, so when we see clients assigned to a transitional officer, we extend the session of the
previous officer to encompass the time spent assigned to the transitional officer, and don't include
the session with the transitional officer.

Officer sessions are unique on person_id, and officer_id, and may be overlapping.
"""

SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL_SESSIONS_QUERY_TEMPLATE = f"""
    WITH prioritized_supervision_sessions AS (
        SELECT * FROM `{{project_id}}.sessions.prioritized_supervision_sessions_materialized`
    )
    ,
    collapsed_prioritized_supervision_sessions AS (
        {aggregate_adjacent_spans(
            table_name="prioritized_supervision_sessions",
            attribute=["compartment_level_1"],
            session_id_output_name='prioritized_supervision_session_id',
            end_date_field_name="end_date_exclusive"
        )}
    )
    , supervision_officer_sessions_lookback AS (
    /* This CTE associates the previous officer within a prioritized_supervision_session for each supervision officer session,
     if the previous officer session has no date gap. */
    SELECT 
        so.* EXCEPT (supervising_officer_external_id),
        supervising_officer_external_id AS officer_id,
        --select the last non null officer within the same prioritized_supervision_session_id
        LAST_VALUE(so.supervising_officer_external_id IGNORE NULLS) 
            OVER (PARTITION BY so.person_id, prioritized_supervision_session_id 
                ORDER BY so.start_date  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                                                            AS previous_officer_id
    FROM `{{project_id}}.sessions.supervision_officer_sessions_materialized` so
    INNER JOIN collapsed_prioritized_supervision_sessions ss
        ON so.person_id = ss.person_id 
        AND so.state_code = ss.state_code
        --join all supervision officer sessions that start within the prioritized_supervision_session
        AND so.start_date BETWEEN ss.start_date AND {nonnull_end_date_exclusive_clause('ss.end_date_exclusive')}
    ),
    caseload_type_sessions AS (
        SELECT * FROM `{{project_id}}.sessions.supervision_staff_primary_specialized_caseload_type_sessions_materialized`
    ),
    intersection_spans_cte AS (
    /* This CTE takes the intersection of supervision_officer_sessions_lookback and 
    supervision_staff_primary_specialized_caseload_type_sessions to add the field of specialized_caseload_type_primary
    onto each supervision_officer_session. For sessions where specialized_caseload_type_primary changes, that session
     is subsessionized. */
        {create_intersection_spans(
            table_1_name= "supervision_officer_sessions_lookback",
            table_2_name= "caseload_type_sessions",
            index_columns= ["state_code", "officer_id"],
            use_left_join = True,
            table_1_columns=["person_id"],
            table_2_columns=["specialized_caseload_type_primary"],
    )}
    ),
    all_spans AS (    
    /* This cte unions all supervision officer sessions from the lookback cte above, and then joins an additional duplicate
     session for any supervision officer session where the officer has a "TRANSITIONAL" specialized caseload type at 
     the start of the supervision officer session, for each supervision officer/person pair, and assigns the officer_id
     to be the previous officer id identified in the supervision_officer_lookback_cte. 
     
     Therefore for supervision officer sessions that start with a transitional specialized caseload type, we will see
     overlapping sessions with one session for the transitional officer (is_transitional = True) and one sessions for 
     the previous officer (is_transitional = False) */
        # This is the full set of existing default spans, with a flag for whether or not the officer is transitional
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            officer_id AS supervising_officer_external_id,
            COALESCE(specialized_caseload_type_primary, "UNKNOWN") = "TRANSITIONAL" AS is_transitional
        FROM intersection_spans_cte
        
        UNION ALL
        
        # These are additional imputed rows that will overlap with transitional officers if they are transitional 
        # at the start of their assignment to a person
        SELECT
            state_code,
            person_id,
            start_date,
            i.end_date_exclusive,
            previous_officer_id AS supervising_officer_external_id,
            FALSE AS is_transitional
        FROM intersection_spans_cte i
        # Add an inner join to check that this transitional period aligns with the start of a supervision officer session
        INNER JOIN supervision_officer_sessions_lookback
            USING (state_code, person_id, start_date, officer_id)
        WHERE COALESCE(specialized_caseload_type_primary, "UNKNOWN") = "TRANSITIONAL"
    ),
    remove_overlapping_transitional AS (
        -- If we have spans with the same end_date (indicative of carry forward logic) where one is
        -- transitional and others are not, remove the transitional one so we only have the
        -- non-transitional spans for that person_id and time period.
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            supervising_officer_external_id
        FROM
            all_spans
        -- Use RANK instead of ROW_NUMBER because there can be overlapping officer spans that aren't
        -- because of the transitional carry-forward logic.
        QUALIFY RANK() OVER (
            PARTITION BY state_code, person_id, end_date_exclusive
            ORDER BY is_transitional -- false comes before true
        ) = 1
    )
    SELECT 
        state_code, 
        person_id, 
        supervising_officer_session_id,
        start_date, 
        end_date_exclusive,
        supervising_officer_external_id,
    FROM
    ({aggregate_adjacent_spans(table_name='remove_overlapping_transitional', 
                                end_date_field_name = 'end_date_exclusive',
                                attribute=['supervising_officer_external_id'],
                                session_id_output_name = 'supervising_officer_session_id')})
    """

SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL_SESSIONS_VIEW_BUILDER.build_and_print()
