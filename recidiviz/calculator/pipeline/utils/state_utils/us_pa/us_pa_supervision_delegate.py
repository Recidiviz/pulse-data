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
"""US_PA implementation of the supervision delegate"""
from typing import Optional, Tuple

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)


class UsPaSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_PA implementation of the supervision delegate"""

    def supervision_location_from_supervision_site(
        self, supervision_site: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """In US_PA, supervision_site follows format {supervision district}|{supervision suboffice}|{supervision unit org code}"""
        # TODO(#3829): Remove this helper once we've once we've built level 1/level 2 supervision
        #  location distinction directly into our schema.
        level_1_supervision_location = None
        level_2_supervision_location = None
        if supervision_site:
            (
                level_2_supervision_location,
                level_1_supervision_location,
                _org_code,
            ) = supervision_site.split("|")
        return (
            level_1_supervision_location or None,
            level_2_supervision_location or None,
        )
