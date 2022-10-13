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
"""Direct ingest controller implementation for US_MO."""

from typing import List

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class UsMoController(BaseDirectIngestController):
    """Direct ingest controller implementation for US_MO."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_MO.value.lower()

    def __init__(self, ingest_instance: DirectIngestInstance):
        super().__init__(ingest_instance)

    def get_ingest_view_rank_list(self) -> List[str]:
        """Returns a list of string ingest view names in the order they should be
        processed for data we received on a particular date.
        """
        return [
            # SQL Preprocessing View
            "tak001_offender_identification",
            "oras_assessments_weekly_v2",
            "offender_sentence_institution",
            "offender_sentence_supervision",
            "tak158_tak026_incarceration_periods",
            "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
            "tak028_tak042_tak076_tak024_violation_reports",
            "tak291_tak292_tak024_citations",
        ]
