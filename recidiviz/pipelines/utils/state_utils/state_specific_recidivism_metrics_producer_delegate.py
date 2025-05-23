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
"""Contains the StateSpecificRecidivismMetricsProducerDelegate, the interface
for state-specific decisions involved in generating metrics regarding
recidivism."""
from recidiviz.pipelines.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)


class StateSpecificRecidivismMetricsProducerDelegate(
    StateSpecificMetricsProducerDelegate
):
    """Interface for state-specific decisions involved in generating metrics regarding
    recidivism."""

    # TODO(#41554): Delete this delegate method and associated person_external_id column
    #  from metric output in favor of joining with a view that gives us "stable"
    #  incarceration person external_ids for each person_id.
    def primary_person_external_id_to_include(self) -> str:
        """Determines the primary person_external_id type to include."""
        raise NotImplementedError(
            "Must replace this with an external id type defined in external_id_types.py"
        )
