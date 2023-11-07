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
"""Listing out exemptions for the state ingest pipeline."""
from typing import Dict, Set

from recidiviz.common.constants.states import StateCode

INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS = {
    # TODO(#24299) Remove this exemption once conflicts no longer appear.
    StateCode.US_PA: {"sci_incarceration_period"},
    # TODO(#24658) Remove this exemption once conflicts no longer appear.
    StateCode.US_CA: {"staff"},
}

# The names of each global uniqueness constraint with known failures by state.
# TODO(#20930) Remove this once all states are shipped to ingest in Dataflow.
GLOBAL_UNIQUE_CONSTRAINT_ERROR_EXEMPTIONS: Dict[StateCode, Set[str]] = {}

# The names of each entity uniqueness constraint with known failures by state.
# TODO(#20930) Remove this once all states are shipped to ingest in Dataflow.
ENTITY_UNIQUE_CONSTRAINT_ERROR_EXEMPTIONS: Dict[StateCode, Set[str]] = {}
