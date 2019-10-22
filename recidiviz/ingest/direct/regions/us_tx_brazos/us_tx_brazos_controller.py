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
import re
from io import StringIO
from typing import List, Optional, Iterable

import pandas as pd

from recidiviz import IngestInfo
from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.common.constants.county.charge import ChargeDegree, ChargeClass
from recidiviz.common.constants.person_characteristics import Race
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller \
    import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs


class UsTxBrazosController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for us_tx_brazos."""

    def __init__(self,
                 ingest_directory_path: Optional[str] = 'us-tx-brazos',
                 storage_directory_path: Optional[str] = None):
        super(UsTxBrazosController, self).__init__(
            'us_tx_brazos',
            SystemLevel.COUNTY,
            ingest_directory_path,
            storage_directory_path)

    def _get_file_tag_rank_list(self) -> List[str]:
        return ['VERABrazosJailData']

    def _parse(self,
               args: GcsfsIngestArgs,
               contents: Iterable[str]) -> IngestInfo:
        # Preprocess raw data.
        df = pd.read_csv(StringIO('\n'.join(contents)), dtype=str).fillna('')
        # People who are rearrested can have multiple bonds for the same charge;
        # the bond with the greatest ID is the most current one.
        df = df[
            df.groupby('Charge ID')['BondID'].transform('max') == df['BondID']]
        # Some people have booking number collisions, so make these unique
        # per person.
        df['Booking Number'] += ' (Individual ID: ' + df['Individual ID'] + ')'

        ingest_info = super()._parse(args, df.to_csv(index=False))

        # Postprocess IngestInfo
        for charge in ingest_info.get_all_charges():
            if charge.degree:
                match = re.match(r'CLASS (.+) MISDEMEANOR', charge.degree,
                                 re.IGNORECASE)
                if match:
                    charge.level = match.group(1)
                    charge.degree = None
                elif 'STATE JAIL' in charge.degree.upper():
                    charge.level = charge.degree
                    charge.degree = None
        return ingest_info

    def get_enum_overrides(self):
        overrides_builder = super().get_enum_overrides().to_builder()

        overrides_builder.ignore(lambda x: True, CustodyStatus)
        overrides_builder.add('ACTIVE', ChargeStatus.PRETRIAL)
        overrides_builder.add('ATTORNEY BOND', BondType.SECURED)
        overrides_builder.add('BARRED FROM PROSECUTION', ChargeStatus.DROPPED)
        overrides_builder.add('CAPITAL FELONY', ChargeDegree.FIRST)
        overrides_builder.ignore('CHANGED', ChargeStatus)
        overrides_builder.ignore('CLASS A MISDEMEANOR', ChargeDegree)
        overrides_builder.ignore('CLASS B MISDEMEANOR', ChargeDegree)
        overrides_builder.ignore('CLASS C MISDEMEANOR', ChargeDegree)

        # TODO: We are hoping this is attached to a charge that has changed in
        #  some other way that we can keep track of, but verify that we're not
        #  losing information here.
        overrides_builder.ignore('CLOSED', BondStatus)

        overrides_builder.ignore('CONVERSION', ChargeDegree)
        overrides_builder.add('DISCHARGED', BondStatus.POSTED)
        overrides_builder.add('DISPOSED', ChargeStatus.DROPPED)
        overrides_builder.ignore('FELONY UNASSIGNED', ChargeDegree)
        overrides_builder.add('FIRST DEGREE FELONY', ChargeDegree.FIRST)
        overrides_builder.add('FORFEITED', BondStatus.REVOKED)
        overrides_builder.add('INACTIVE', ChargeStatus.DROPPED)
        overrides_builder.add('INFORMATION FILED', ChargeStatus.PENDING)
        overrides_builder.ignore('M', ChargeDegree)
        overrides_builder.add('MISDEMEANOR TRAFFIC', ChargeClass.INFRACTION,
                              ChargeDegree)
        overrides_builder.ignore('MISDEMEANOR UNASSIGNED', ChargeDegree)
        overrides_builder.add('NO BILLED', ChargeStatus.DROPPED)
        overrides_builder.ignore('NOT APPLICABLE', ChargeDegree)
        overrides_builder.ignore('OTHER INACTIVE', BondStatus)
        overrides_builder.add('PRE INDICTMENT PLEA', ChargeStatus.SENTENCED)
        overrides_builder.add('READY FOR GRAND JURY', ChargeStatus.PENDING)
        overrides_builder.add('REFUSED REJECTED', ChargeStatus.DROPPED)
        overrides_builder.add('RELEASED', BondStatus.POSTED)
        overrides_builder.add('REJECTED', ChargeStatus.DROPPED)
        overrides_builder.add('SECOND DEGREE FELONY', ChargeDegree.SECOND)
        overrides_builder.ignore('STATE JAIL FELONY', ChargeDegree)
        overrides_builder.add('THIRD DEGREE FELONY', ChargeDegree.THIRD)
        overrides_builder.add('TRANS TO FELONY', ChargeClass.FELONY,
                              ChargeStatus)
        overrides_builder.add('TRANS TO MISD', ChargeClass.MISDEMEANOR,
                              ChargeStatus)
        overrides_builder.add('UNAVAILABLE', Race.EXTERNAL_UNKNOWN)
        overrides_builder.add('VOID', BondStatus.REVOKED)

        return overrides_builder.build()
