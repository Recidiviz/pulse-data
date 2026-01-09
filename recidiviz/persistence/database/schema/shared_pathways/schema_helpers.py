# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
# ============================================================================
"""Helper function for Public and Private Pathways schema
"""

from typing import List, Optional

from sqlalchemy import Index


def build_covered_indexes(
    *,
    index_base_name: str,
    dimensions: List[str],
    includes: Optional[List[str]] = None,
) -> List[Index]:
    """Creates covered indexes for each dimension, which speeds up commonly
    used queries by allowing query results to return from the index without
    accessing the full data table.
    https://www.postgresql.org/docs/current/indexes-index-only-scans.html
    """
    dimensions = sorted(dimensions)
    dimension_set = set(dimensions)
    includes = [] if includes is None else includes
    return [
        Index(
            f"{index_base_name}_{dimension}",
            dimension,
            postgresql_include=[
                *sorted(dimension_set - {dimension}),
                *includes,
            ],
        )
        for dimension in dimensions
    ]
