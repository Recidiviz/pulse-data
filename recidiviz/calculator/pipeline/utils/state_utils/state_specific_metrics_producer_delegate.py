# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Stores the base StateSpecificMetricsProducerDelegate class shared by all state-specific
metrics delegates."""
from typing import Optional

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)


class StateSpecificMetricsProducerDelegate(StateSpecificDelegate):
    """Base class for a class that defines state-specific metric specifications."""

    def primary_person_external_id_to_include(self) -> Optional[str]:
        """Determines the primary person_external_id type to include, if applicable."""
        return None

    def secondary_person_external_id_to_include(self) -> Optional[str]:
        """Determines the secondary person_external_id type to include, if applicable."""
        return None
