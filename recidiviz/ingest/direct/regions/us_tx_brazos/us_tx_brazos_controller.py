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
from datetime import datetime
from typing import List, Optional, cast, Iterator

import attr
import pandas as pd
from more_itertools import partition

from recidiviz import IngestInfo
from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.common.constants.county.charge import ChargeDegree, ChargeClass
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.person_characteristics import Race
from recidiviz.common.ingest_metadata import SystemLevel, IngestMetadata
from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller \
    import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs, filename_parts_from_path
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence import persistence
from recidiviz.utils.environment import get_gae_environment


class UsTxBrazosController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for us_tx_brazos."""

    def __init__(self,
                 ingest_directory_path: Optional[str] = 'us-tx-brazos',
                 storage_directory_path: Optional[str] = None,
                 max_delay_sec_between_files: Optional[int] = None):
        gae_environment = get_gae_environment()
        if ingest_directory_path and gae_environment:
            ingest_directory_path += f'-{gae_environment}'
        super().__init__(
            'us_tx_brazos',
            SystemLevel.COUNTY,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files)

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return ['VERABrazosJailData']

    class DataFrameContentsHandle(GcsfsFileContentsHandle):
        def __init__(self, local_path: str, df: pd.DataFrame):
            super().__init__(local_path)
            self.df = df

        def get_contents_iterator(self) -> Iterator[str]:
            return self.df.to_csv(index=False)

    def _parse(self,
               args: GcsfsIngestArgs,
               contents_handle: GcsfsFileContentsHandle) -> IngestInfo:
        # Preprocess raw data.
        df = pd.read_csv(contents_handle.local_file_path, dtype=str).fillna('')
        df = df[df['Custody Status'] != 'Released']
        # People who are rearrested can have multiple bonds for the same charge;
        # the bond with the greatest ID is the most current one.
        df = df[
            df.groupby('Charge ID')['BondID'].transform('max') == df['BondID']]
        # Some people have booking number collisions, so make these unique
        # per person.
        df['Booking Number'] += ' (Individual ID: ' + df['Individual ID'] + ')'

        ingest_info = super()._parse(args,
                                     self.DataFrameContentsHandle(
                                         contents_handle.local_file_path, df))

        # Postprocess IngestInfo
        for charge in ingest_info.get_all_charges():
            if charge.name:
                charge_parts = charge.name.lstrip('*').split('/')
                name, notes = partition(self._is_charge_note, charge_parts)
                charge.name = '/'.join(name)
                charge.charge_notes = '/'.join(notes)

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

    def _is_charge_note(self, charge_name: str) -> bool:
        CHARGE_NOTES = ('17.16', 'DET ORD', 'DET PEN', 'JUD NISI', 'MTP',
                        'SURETY SURRENDER', 'BENCH WARRANT')
        return any(charge_name.startswith(charge_note)
                   for charge_note in CHARGE_NOTES)

    def _file_meets_file_line_limit(
            self,
            _line_limit: int,
            _path: GcsfsFilePath) -> bool:
        """The CSV files must be processed all at once, so do not split."""
        return True

    def _get_ingest_metadata(self, args: GcsfsIngestArgs) -> IngestMetadata:
        parts = filename_parts_from_path(args.file_path)
        ingest_time = datetime.strptime(cast(str, parts.filename_suffix),
                                        '%m%d%Y_%H%M%S')

        return attr.evolve(super()._get_ingest_metadata(args),
                           ingest_time=ingest_time)

    def _do_cleanup(self, args: GcsfsIngestArgs) -> None:
        """If this job is the last for the day, call infer_release before
        continuing to further jobs."""
        self.fs.mv_path_to_processed_path(args.file_path)

        if self._is_last_job_for_day(args):
            persistence.infer_release_on_open_bookings(
                self.region.region_code,
                self._get_ingest_metadata(args).ingest_time,
                CustodyStatus.INFERRED_RELEASE)

        parts = filename_parts_from_path(args.file_path)
        self._move_processed_files_to_storage_as_necessary(
            last_processed_date_str=parts.date_str)

    def get_enum_overrides(self) -> EnumOverrides:
        overrides_builder = super().get_enum_overrides().to_builder()

        overrides_builder.ignore_with_predicate(lambda x: True, CustodyStatus)
        overrides_builder.add('ACTIVE', ChargeStatus.PRETRIAL)
        overrides_builder.add('ATTORNEY BOND', BondType.SECURED)
        overrides_builder.add('BARRED FROM PROSECUTION', ChargeStatus.DROPPED)
        overrides_builder.add('CAPITAL FELONY', ChargeDegree.FIRST)
        overrides_builder.ignore('CHANGED', ChargeStatus)
        overrides_builder.ignore('CLASS A MISDEMEANOR', ChargeDegree)
        overrides_builder.ignore('CLASS B MISDEMEANOR', ChargeDegree)
        overrides_builder.ignore('CLASS C MISDEMEANOR', ChargeDegree)

        # TODO(#4153): We are hoping this is attached to a charge that has changed in
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
