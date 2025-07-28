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
"""Helper for building a set of views from view builders."""
import logging
from typing import Dict, Sequence, Set

from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)


def build_views_to_update(
    view_source_table_datasets: Set[str],
    candidate_view_builders: Sequence[BigQueryViewBuilder],
    sandbox_context: BigQueryViewSandboxContext | None,
) -> Dict[BigQueryView, BigQueryViewBuilder]:
    """
    Returns a map associating view builders to the views that should be updated,
    built from builders in the |candidate_view_builders| list.
    """

    logging.info("Building [%s] views...", len(candidate_view_builders))
    views_to_builders = {}
    for view_builder in candidate_view_builders:
        # TODO(#45650): move this check out of the view update into a test
        if view_builder.dataset_id in view_source_table_datasets:
            raise ValueError(
                f"Found view [{view_builder.view_id}] in source-table-only dataset [{view_builder.dataset_id}]"
            )

        try:
            view = view_builder.build(sandbox_context=sandbox_context)
        except Exception as e:
            raise ValueError(
                f"Unable to build view at address [{view_builder.address}]"
            ) from e

        views_to_builders[view] = view_builder
    return views_to_builders
