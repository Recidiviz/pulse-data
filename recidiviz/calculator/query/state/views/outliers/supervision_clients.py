# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Information about supervision clients."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    get_pseudonymized_id_query_str,
    list_to_query_string,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    state_specific_supervision_external_id_type,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_clients"

_DESCRIPTION = """Information about supervision clients."""

_QUERY_TEMPLATE = f"""
WITH supervision_clients AS (
    SELECT DISTINCT
        person.state_code,
        id.external_id AS client_id,
        {get_pseudonymized_id_query_str("IF(person.state_code = 'US_IX', 'US_ID', person.state_code) || id.external_id")} AS pseudonymized_client_id,
        person.full_name AS client_name,
        d.birthdate,
        d.gender,
        d.prioritized_race_or_ethnicity AS race_or_ethnicity,
    FROM `{{project_id}}.normalized_state.state_person` person
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` id 
        USING (state_code, person_id)
    INNER JOIN `{{project_id}}.sessions.person_demographics_materialized` d 
        USING (state_code, person_id)
    -- Only include clients who have events in the events product view
    INNER JOIN `{{project_id}}.outliers_views.supervision_client_events_materialized` events
        ON events.state_code = id.state_code AND events.client_id = id.external_id
    WHERE
        person.state_code IN ({list_to_query_string(get_outliers_enabled_states_for_bigquery(), quoted=True)})
        AND id.id_type = {{state_id_type}}
)

SELECT
    {{columns}}
FROM supervision_clients
"""

SUPERVISION_CLIENTS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    should_materialize=True,
    state_id_type=state_specific_supervision_external_id_type("id"),
    columns=[
        "state_code",
        "client_id",
        "pseudonymized_client_id",
        "client_name",
        "birthdate",
        "gender",
        "race_or_ethnicity",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_CLIENTS_VIEW_BUILDER.build_and_print()
