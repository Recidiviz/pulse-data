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
"""All views in `sentence_sessions` dataset but including all states, not just the migrated states """

from copy import deepcopy

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import (
    SENTENCE_SESSIONS_V2_ALL_DATASET,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_sessions_views import (
    SENTENCE_SESSIONS_VIEW_BUILDERS,
)

_STATES_TO_NOT_INCLUDE_IN_V2_ALL_DATASET = [""]

SENTENCE_SESSIONS_V2_ALL_VIEW_BUILDERS = deepcopy(SENTENCE_SESSIONS_VIEW_BUILDERS)
for view_builder in SENTENCE_SESSIONS_V2_ALL_VIEW_BUILDERS:
    # set the dataset_id to the new v2 all dataset
    view_builder.dataset_id = SENTENCE_SESSIONS_V2_ALL_DATASET
    # if there are references to `sentence_sessions` in the view, update those to also pull from the v2 all dataset
    if "sentence_sessions_dataset" in view_builder.query_format_kwargs:
        view_builder.query_format_kwargs[
            "sentence_sessions_dataset"
        ] = SENTENCE_SESSIONS_V2_ALL_DATASET
    # if the view references migrated states, update the non-migrated list to be an empty string
    if "v2_non_migrated_states" in view_builder.query_format_kwargs:
        view_builder.query_format_kwargs[
            "v2_non_migrated_states"
        ] = list_to_query_string(
            string_list=_STATES_TO_NOT_INCLUDE_IN_V2_ALL_DATASET, quoted=True
        )
    # update the address for materialized views with the new dataset_id
    view_builder.materialized_address = (
        SimpleBigQueryViewBuilder.build_standard_materialized_address(
            dataset_id=view_builder.dataset_id, view_id=view_builder.view_id
        )
    )
