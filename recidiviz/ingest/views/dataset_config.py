# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Dataset configuration for ingest documentation and unified referencing."""

VIEWS_DATASET: str = "ingest_metadata"

# Views that are the union of the output from the us_xx_state datasets in each state's
# Dataflow ingest pipeline.
STATE_BASE_VIEWS_DATASET: str = "state_views"

# The tables for the state schema, including output from each state's PRIMARY Dataflow
# ingest pipeline.
STATE_BASE_DATASET: str = "state"

# Views that are the union of the output from the us_xx_normalized_state datasets in
# each state's Dataflow ingest pipeline.
NORMALIZED_STATE_VIEWS_DATASET: str = "normalized_state_views"

# Where the normalized state tables live, with data from all states. For each entity
# that is not normalized, these are a copy of the corresponding table in the `state`
# dataset. For each entity that is normalized, the entity table contains the normalized
# output for that entity in each state.
NORMALIZED_STATE_DATASET: str = "normalized_state"
