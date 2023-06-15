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
"""Direct ingest controller implementation for US_MI."""
from typing import List

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import environment


class UsMiController(BaseDirectIngestController):
    """Direct ingest controller implementation for US_MI."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_MI.value.lower()

    def __init__(self, ingest_instance: DirectIngestInstance):
        super().__init__(ingest_instance)

    @classmethod
    def _get_ingest_view_rank_list(
        cls, ingest_instance: DirectIngestInstance
    ) -> List[str]:
        """Returns a list of string ingest view names in the order they should be
        processed for data we received on a particular date.
        """

        tags = [
            "person_external_ids",
            "state_persons_v2",
            "supervision_periods_v2",
            "sentences_v2",
            "state_charge_v2",
            "state_drug_screen",
            "employment_period",
            "supervision_violations",
            "supervision_violations_probation",
            "incarceration_periods_v3",
        ]

        if environment.in_gcp_production():
            tags.extend(["assessments_v2"])

        else:
            tags.extend(["assessments_v3"])

        return tags
