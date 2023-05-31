# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Direct ingest controller implementation for US_PA."""
from typing import List

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class UsPaController(BaseDirectIngestController):
    """Direct ingest controller implementation for US_PA."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_PA.value.lower()

    @classmethod
    def _get_ingest_view_rank_list(
        cls, ingest_instance: DirectIngestInstance
    ) -> List[str]:
        """Returns a list of string ingest view names in the order they should be
        processed for data we received on a particular date.
        """
        return [
            # Data source: Mixed
            "person_external_ids",
            # Data source: DOC
            "doc_person_info",
            "dbo_tblInmTestScore",
            "dbo_Senrec_v2",
            "sci_incarceration_period",
            "dbo_Miscon",
            # Data source: CCIS
            "ccis_incarceration_period",
            # Data source: PBPP
            "dbo_Offender_v2",
            "dbo_LSIHistory",
            "supervision_violation",
            "supervision_violation_response",
            "board_action",
            "supervision_contacts",
            "supervision_period_v4",
            "program_assignment",
            "supervision_staff",
            "supervision_staff_location_period",
            "supervision_staff_supervisor_period",
            "supervision_staff_role_period",
            "supervision_staff_caseload_type_period",
        ]
