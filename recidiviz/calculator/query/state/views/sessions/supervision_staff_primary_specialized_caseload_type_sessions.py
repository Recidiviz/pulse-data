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
"""Sessionized view of the primary specialized caseload type for each supervision officer
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_STAFF_PRIMARY_SPECIALIZED_CASELOAD_TYPE_SESSIONS_VIEW_NAME = (
    "supervision_staff_primary_specialized_caseload_type_sessions"
)

SUPERVISION_STAFF_PRIMARY_SPECIALIZED_CASELOAD_TYPE_SESSIONS_VIEW_DESCRIPTION = """
Sessionized view of the primary specialized caseload type for each supervision officer. 

Supervision_staff_attribute_sessions is unioned with supervision_officer_sessions to fill in gaps with 
specialized_caseload_type_primary = NULL for spans of time where we see an officer is supervising a client but 
does not have any spans in supervision_staff_attribute_sessions. 
"""

SUPERVISION_STAFF_PRIMARY_SPECIALIZED_CASELOAD_TYPE_SESSIONS_QUERY_TEMPLATE = f"""
WITH primary_caseload_sessions AS (
/* This CTE unions all supervision_staff_attribute_sessions to get specialized_caseload_type_primary with all 
supervision_officer_sessions, to get full coverage of where officer sessions are observed in our data */
#TODO(#30655) refactor once upstream view is created 
    SELECT
        state_code,
        officer_id,
        start_date,
        end_date_exclusive,
        specialized_caseload_type_primary
    FROM `{{project_id}}.{{sessions_dataset}}.supervision_staff_attribute_sessions_materialized`
    
    UNION ALL 
    
    SELECT 
        state_code,
        supervising_officer_external_id AS officer_id,
        start_date,
        end_date_exclusive,
        NULL AS specialized_caseload_type_primary
    FROM `{{project_id}}.{{sessions_dataset}}.supervision_officer_sessions_materialized` so
),
{create_sub_sessions_with_attributes(table_name ='primary_caseload_sessions',
                                     end_date_field_name='end_date_exclusive',
                                     index_columns=['state_code', 'officer_id'])},
primary_caseload_spans AS (
/* After creating sub_sessions from all sources of officer sessions, we dedup to always take the non null value
for specialized_caseload_type_primary, and then aggregate adjacent spans */
    SELECT 
        state_code,
        officer_id,
        start_date,
        end_date_exclusive,
        --take nonnull value for overlapping spans
        MAX(CASE WHEN specialized_caseload_type_primary IS NOT NULL THEN specialized_caseload_type_primary END) AS specialized_caseload_type_primary
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
)
{aggregate_adjacent_spans(
    table_name="primary_caseload_spans", 
    index_columns=["state_code","officer_id"], 
    attribute=["specialized_caseload_type_primary"],
    session_id_output_name="specialized_caseload_type_session_id",
    end_date_field_name="end_date_exclusive",
)}
"""

SUPERVISION_STAFF_PRIMARY_SPECIALIZED_CASELOAD_TYPE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    view_id=SUPERVISION_STAFF_PRIMARY_SPECIALIZED_CASELOAD_TYPE_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_STAFF_PRIMARY_SPECIALIZED_CASELOAD_TYPE_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_STAFF_PRIMARY_SPECIALIZED_CASELOAD_TYPE_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "officer_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_STAFF_PRIMARY_SPECIALIZED_CASELOAD_TYPE_SESSIONS_VIEW_BUILDER.build_and_print()
