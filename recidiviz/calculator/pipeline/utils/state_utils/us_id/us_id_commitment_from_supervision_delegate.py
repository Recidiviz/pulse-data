# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Utils for state-specific logic related to incarceration commitments from supervision
in US_ID."""
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)


class UsIdCommitmentFromSupervisionDelegate(
    StateSpecificCommitmentFromSupervisionDelegate
):
    """US_ID implementation of the StateSpecificCommitmentFromSupervisionDelegate."""

    def should_filter_out_unknown_supervision_type_in_pre_commitment_sp_search(
        self,
    ) -> bool:
        """In US_ID it's common for there to be periods with unset
        supervision_period_supervision_type values prior to an admission to
        incarceration, since these periods may signify that there is a warrant out for
        the person's arrest. So, for US_ID we need to filter
        the list of supervision periods to only include ones with a set
        supervision_period_supervision_type before looking for a
        pre-commitment supervision period."""
        return True
