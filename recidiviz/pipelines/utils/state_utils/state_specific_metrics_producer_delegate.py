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

from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)


# TODO(#30363): We should be able to delete this delegate and all subclasses once we
#  stop outputting person_external_id / secondary_person_external_id from metric
#  pipelines.
class StateSpecificMetricsProducerDelegate(StateSpecificDelegate):
    """Base class for a class that defines state-specific metric specifications."""

    def primary_person_external_id_to_include(self) -> str:
        """Determines the primary person_external_id type to include."""
        raise NotImplementedError(
            "Must replace this with an external id type defined in external_id_types.py"
        )

    def secondary_person_external_id_to_include(self) -> Optional[str]:
        """Determines the secondary person_external_id type to include, if applicable."""
        return None
