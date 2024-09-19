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
"""A view that checks whether across all states, entities in both the state
and normalized_state datasets have unique primary keys."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.utils.entities_validation_utils import (
    unique_primary_keys_values_across_all_states_query,
)

PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_NAME = (
    "primary_keys_unique_across_all_states"
)

PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_DESCRIPTION = """Checks whether across all states,
entities in the normalized_state datasets have unique primary keys."""

PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_QUERY_TEMPLATE = f"""
{unique_primary_keys_values_across_all_states_query()}
"""

PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_NAME,
    view_query_template=PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_QUERY_TEMPLATE,
    description=PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_BUILDER.build_and_print()
