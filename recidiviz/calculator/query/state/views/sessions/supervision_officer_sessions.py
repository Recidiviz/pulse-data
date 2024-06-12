# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Sessionized view of each individual on supervision. Session defined as continuous
time on caseload of a given supervising officer.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_SESSIONS_VIEW_NAME = "supervision_officer_sessions"

SUPERVISION_OFFICER_SESSIONS_VIEW_DESCRIPTION = """
Sessionized view of each individual. Session defined as continuous stay on supervision 
associated with a given officer. Officer sessions may be overlapping.
"""

SUPERVISION_OFFICER_SESSIONS_QUERY_TEMPLATE = f"""
    WITH supervision_spans AS (
    SELECT DISTINCT
        state_code, person_id, 
        session_attributes.supervising_officer_external_id,
        start_date,
        end_date_exclusive,
        dataflow_session_id,
    FROM `{{project_id}}.{{sessions_dataset}}.dataflow_sessions_materialized`,
    UNNEST(session_attributes) session_attributes
    WHERE compartment_level_1 = 'SUPERVISION' OR compartment_level_1 = 'SUPERVISION_OUT_OF_STATE'
    )
    SELECT *,
    DATE_SUB(end_date_exclusive, INTERVAL 1 DAY) AS end_date,
    FROM 
    ({aggregate_adjacent_spans(
        table_name="supervision_spans",
        attribute=["supervising_officer_external_id"],
        end_date_field_name='end_date_exclusive',
        session_id_output_name = 'supervising_officer_session_id'
    )})
    """

SUPERVISION_OFFICER_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_OFFICER_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_SESSIONS_VIEW_BUILDER.build_and_print()
