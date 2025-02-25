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
"""Sessionized view of periods of continuous stay under a given custodial
authority, using Recidiviz schema mappings"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CUSTODIAL_AUTHORITY_SESSIONS_VIEW_NAME = "custodial_authority_sessions"

CUSTODIAL_AUTHORITY_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of periods of continuous stay under a given custodial
authority, using Recidiviz schema mappings"""

CUSTODIAL_AUTHORITY_SESSIONS_QUERY_TEMPLATE = f"""
    WITH sub_sessions_cte AS
    (
    SELECT 
        state_code,
        person_id,
        custodial_authority,
        start_date,
        end_date_exclusive,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`
    )    
    SELECT *
    FROM (
    {aggregate_adjacent_spans(table_name='sub_sessions_cte',
                       attribute=['custodial_authority'],
                       session_id_output_name='custodial_authority_session_id',
                       end_date_field_name='end_date_exclusive')}
    )
    """

CUSTODIAL_AUTHORITY_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=CUSTODIAL_AUTHORITY_SESSIONS_VIEW_NAME,
    view_query_template=CUSTODIAL_AUTHORITY_SESSIONS_QUERY_TEMPLATE,
    description=CUSTODIAL_AUTHORITY_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CUSTODIAL_AUTHORITY_SESSIONS_VIEW_BUILDER.build_and_print()
