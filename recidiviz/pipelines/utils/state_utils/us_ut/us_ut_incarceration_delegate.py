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
"""Contains US_UT implementation of the StateSpecificIncarcerationDelegate."""
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.ingest.direct.regions.us_ut.ingest_views.common_code_constants import (
    SUPERVISION_LEGAL_STATUS_CODES_SET,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
)
from recidiviz.pipelines.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)


class UsUtIncarcerationDelegate(StateSpecificIncarcerationDelegate):
    """US_UT implementation of the StateSpecificIncarcerationDelegate."""

    def is_period_included_in_state_population(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
    ) -> bool:
        """In Utah, people who are incarcerated in another state, a federal facility,
        or in a county jail while waiting for supervision revocation proceedings are
        not counted toward the state incarceration population. Juveniles are also excluded
        from the official count.
        """
        # These are identifiers for periods during which a person is on supervision,
        # but housed in faciliy pending a revocation investigation.
        if incarceration_period.custodial_authority_raw_text:
            if (
                incarceration_period.custodial_authority_raw_text.split("@@")[1]
                in SUPERVISION_LEGAL_STATUS_CODES_SET
            ):
                return False
        return (
            # Only count people who are in state prison or county jail without a supervision
            # legal status.
            incarceration_period.custodial_authority
            in (StateCustodialAuthority.STATE_PRISON, StateCustodialAuthority.COUNTY)
            # These are the four juvenile detention centers in the state.
            and incarceration_period.facility
            not in (
                "MILL CREEK YOUTH CENTER",
                "DECKER LAKE YOUTH CENTER",
                "SLATE CANYON JUVENILE",
                "SOUTHWEST UTAH YOUTH CENTER",
            )
            # Do not count people who are currently escaped from custody.
            and incarceration_period.admission_reason
            != StateIncarcerationPeriodAdmissionReason.ESCAPE
        )
