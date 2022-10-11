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
"""Sessionization of housing stays to confinement type"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_NAME = "us_mo_confinement_type_sessions"

US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_DESCRIPTION = (
    """Sessionization of housing stays to confinement type"""
)

US_MO_CONFINEMENT_TYPE_SESSIONS_QUERY_TEMPLATE = f"""
    /* {{description}} */
    WITH housing_stay_cte AS
    (
    SELECT 
        *
    FROM `{{project_id}}.{{sessions_dataset}}.us_mo_housing_stay_sessions_materialized`
    )
   {aggregate_adjacent_spans(table_name='housing_stay_cte',
                       attribute='confinement_type',
                       session_id_output_name='confinement_type_session_id')}
"""

US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    view_id=US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_NAME,
    description=US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=US_MO_CONFINEMENT_TYPE_SESSIONS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_BUILDER.build_and_print()
