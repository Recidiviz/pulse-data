# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""The point in the LLM document extraction DAG at which a graph's views can be
materialized for one extractor."""
from enum import Enum


class LLMExtractionViewGraphStage(Enum):
    """The point in the LLM document extraction DAG at which a graph's views can be
    materialized for one extractor."""

    FIRST_ORDER = "first_order"
    """After first-order extraction has run: the public views when the collection
    declares no entity groups or the pre-resolution views when it does."""

    ENTITY_RESOLUTION = "entity_resolution"
    """After entity resolution has run for the collection's entity groups: the
    entity mentions and entities views, plus the enriched public views."""
