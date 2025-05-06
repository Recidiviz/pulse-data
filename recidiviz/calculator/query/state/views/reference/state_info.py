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
"""One row per state with generic information about the state."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode


def get_state_info_view_builder() -> SimpleBigQueryViewBuilder:
    query_fragments = []
    for state_code in StateCode:
        state = state_code.get_state()
        query_fragments.append(
            f"""
SELECT
  "{state_code.value}" as state_code, 
  "{state.fips}" as fips,
  "{state.name}" as name
"""
        )
    return SimpleBigQueryViewBuilder(
        dataset_id=REFERENCE_VIEWS_DATASET,
        view_id="state_info",
        description="General information about the state.",
        view_query_template="\nUNION ALL\n".join(query_fragments),
    )
