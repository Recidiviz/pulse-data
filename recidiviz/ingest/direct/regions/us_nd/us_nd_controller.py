# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Direct ingest controller implementation for us_nd."""
from typing import List

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class UsNdController(BaseDirectIngestController):
    """Direct ingest controller implementation for us_nd."""

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_ND

    def __init__(self, ingest_instance: DirectIngestInstance):
        super().__init__(ingest_instance)

    @classmethod
    def _get_ingest_view_rank_list(
        cls, ingest_instance: DirectIngestInstance
    ) -> List[str]:
        """Returns a list of string ingest view names in the order they should be
        processed for data we received on a particular date.
        """

        # NOTE: The order of ingest here is important! Do not change unless you know what you're doing!
        tags = [
            # Elite - incarceration-focused
            "elite_offenderidentifier",
            "elite_offenders",
            "elite_alias",
            "elite_offenderbookingstable",
            "elite_externalmovements_incarceration_periods",
            "elite_offense_in_custody_and_pos_report_data",
            # Docstars - supervision-focused
            "docstars_offenders",
            "docstars_offendercasestable_with_officers",
            "docstars_offensestable",
            "docstars_ftr_episode",
            "docstars_lsi_chronology",
            "docstars_contacts_v2",
            "docstars_offenders_early_term",
            "docstars_staff",
            "docstars_staff_location_periods",
            "docstars_staff_role_periods",
            "docstars_staff_supervisor_periods",
            "elite_incarceration_sentences",
        ]

        # TODO(#1918): Integrate bed assignment / location history
        return tags
