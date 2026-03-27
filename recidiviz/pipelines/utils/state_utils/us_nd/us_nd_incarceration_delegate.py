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
"""US_ND implementation of the incarceration delegate"""
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
)
from recidiviz.pipelines.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)


class UsNdIncarcerationDelegate(StateSpecificIncarcerationDelegate):
    """US_ND implementation of the incarceration delegate"""

    def is_period_included_in_state_population(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
    ) -> bool:
        """In US_ND, periods under state prison or other-state custodial authority are
        included in the state population. Two additional categories under county custodial
        authority are also included because ND counts them in their official reports:
          - DEFP (Deferred Placement): people awaiting facility placement, held prior to
            prison admission (e.g. parole violators not yet transferred to a state facility).
          - CJ (county jail): people physically housed at county jails under county
            custodial authority (parole violator county jail stays, CJ-PV).
        """
        if incarceration_period.custodial_authority in (
            # TODO(#3723): Stop including OOS periods once we have handled the fact
            #  that the releases to this facility are classified as transferred.
            StateCustodialAuthority.STATE_PRISON,
            StateCustodialAuthority.OTHER_STATE,
        ):
            return True
        # DEFP and CJ periods under county custodial authority are counted in ND's
        # official population reports. The raw text is stored as
        # "{bed_assignment}|{facility}|{datetime}", so we extract the facility
        # from index 1 of the pipe-delimited string.
        if incarceration_period.custodial_authority == StateCustodialAuthority.COUNTY:
            raw_text = incarceration_period.custodial_authority_raw_text or ""
            parts = raw_text.split("|")
            facility = parts[1] if len(parts) >= 2 else ""
            return facility in ("DEFP", "CJ")
        return False
