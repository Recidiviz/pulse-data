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

"""Direct ingest controller implementation for us_tx_brazos."""
from typing import List, Optional

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.common.constants.county.charge import ChargeDegree
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller \
    import CsvGcsfsDirectIngestController


class UsTxBrazosController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for us_tx_brazos."""

    def __init__(self,
                 ingest_directory_path: Optional[str] = None,
                 storage_directory_path: Optional[str] = None):
        super(UsTxBrazosController, self).__init__(
            'us_tx_brazos',
            SystemLevel.COUNTY,
            ingest_directory_path,
            storage_directory_path)

    def _get_file_tag_rank_list(self) -> List[str]:
        return ['daily_data']

    def get_enum_overrides(self):
        overrides_builder = super(UsTxBrazosController,
                                  self).get_enum_overrides().to_builder()

        overrides_builder.ignore(lambda x: True, CustodyStatus)
        overrides_builder.ignore('CLASS A MISDEMEANOR', ChargeDegree)
        overrides_builder.ignore('CLASS B MISDEMEANOR', ChargeDegree)
        overrides_builder.ignore('FELONY UNASSIGNED', ChargeDegree)
        overrides_builder.ignore('NOT APPLICABLE', ChargeDegree)
        overrides_builder.ignore('STATE JAIL FELONY', ChargeDegree)

        return overrides_builder.build()
