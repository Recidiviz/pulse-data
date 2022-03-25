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
"""Direct ingest controller implementation for US_TN."""
from typing import List

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.utils import environment


class UsTnController(BaseDirectIngestController):
    """Direct ingest controller implementation for US_TN."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_TN.value.lower()

    def __init__(self, ingest_bucket_path: GcsfsBucketPath):
        super().__init__(ingest_bucket_path)

    def get_ingest_view_rank_list(self) -> List[str]:
        """Returns a list of string ingest view names in the order they should be
        processed for data we received on a particular date.
        """
        tags = []
        # TODO(#10740): Remove gating once ingest views are ready to be launched in prod.
        if not environment.in_gcp_production():
            tags.extend(
                [
                    "OffenderName",
                    "OffenderMovementIncarcerationPeriod",
                    "AssignedStaffSupervisionPeriod_v2",
                    "VantagePointAssessments",
                    "SentencesChargesAndCourtCases",
                ]
            )

            # TODO(#11679): Remove gating once we are ready to ingest ContactNote file sizes faster than current infra allows.
            if not environment.in_gcp():
                tags.extend(
                    [
                        "SupervisionContactsPre1990",
                        "SupervisionContacts1990to1995",
                        "SupervisionContacts1995to1997",
                        "SupervisionContacts1997to2000",
                        "SupervisionContacts2000to2003",
                        "SupervisionContacts2003to2005",
                        "SupervisionContacts2005to2007",
                        "SupervisionContacts2007to2010",
                        "SupervisionContacts2010to2013",
                        "SupervisionContacts2013to2015",
                        "SupervisionContacts2015to2017",
                        "SupervisionContacts2017to2020",
                        "SupervisionContactsPost2020",
                    ]
                )

        return tags
