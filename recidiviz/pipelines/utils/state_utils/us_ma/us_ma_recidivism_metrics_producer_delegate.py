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
"""Contains US_MA implementation of the StateSpecificRecidivismMetricsProducerDelegate."""

from recidiviz.common.constants.state.external_id_types import US_MA_COMMIT_NO
from recidiviz.pipelines.utils.state_utils.state_specific_recidivism_metrics_producer_delegate import (
    StateSpecificRecidivismMetricsProducerDelegate,
)


class UsMaRecidivismMetricsProducerDelegate(
    StateSpecificRecidivismMetricsProducerDelegate
):
    """US_MA implementation of the StateSpecificRecidivismMetricsProducerDelegate."""

    def primary_person_external_id_to_include(self) -> str:
        return US_MA_COMMIT_NO
