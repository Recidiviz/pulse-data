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
"""Interface for state-specific decisions about running normalization logic in ingest.
"""
import abc

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)


class StateSpecificNormalizationDelegate(abc.ABC, StateSpecificDelegate):
    """Interface for state-specific decisions about running normalization logic in
    ingest.
    """

    def extra_entities_generated_via_normalization(
        self,
        normalization_input_types: set[type[Entity]],  # pylint: disable=unused-argument
    ) -> set[type[NormalizedStateEntity]]:
        """Returns a list of Normalized* entity types that are *only* produced by
        the normalization step of ingest pipelines and are not produced directly by
        any ingest views / mappings.

        Args:
            normalization_input_types: The entity types that are produced by the
              ingest mappings step and are inputs to the normalization step.
        """
        return set()
