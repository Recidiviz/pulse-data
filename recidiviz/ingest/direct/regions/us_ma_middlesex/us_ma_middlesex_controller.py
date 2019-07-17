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

"""Direct ingest for us_ma_middlesex.
"""
import logging
from typing import Iterable, Dict

import pandas as pd
import sqlalchemy
from more_itertools import one

from recidiviz import IngestInfo
from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.booking import AdmissionReason, \
    ReleaseReason
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController, IngestArgs
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.ingest.direct.regions.us_ma_middlesex.us_ma_middlesex_parser \
    import UsMaMiddlesexParser
from recidiviz.utils import secrets, environment

_DATABASE_TYPE = 'postgresql'


class UsMaMiddlesexController(BaseDirectIngestController[IngestArgs,
                                                         Iterable[Dict]]):
    """Reads tables from our us_ma_middlesex postgres db, which is an extract of
    the county's own sql database. The data flows through several formats, from
    postgres tables to dataframes to python dictionaries to IngestInfo objects.
    """

    def __init__(self):
        """Initialize the controller and read tables from cloud SQL."""
        super(UsMaMiddlesexController, self).__init__('us_ma_middlesex',
                                                      SystemLevel.COUNTY)
        self._connect_to_db()
        self._load_data()
        self.parser = UsMaMiddlesexParser()

    def _connect_to_db(self):
        db_user = secrets.get_secret('us_ma_middlesex_db_user')
        db_password = secrets.get_secret('us_ma_middlesex_db_password')
        db_name = secrets.get_secret('us_ma_middlesex_db_name')
        cloudsql_instance_id = secrets.get_secret('us_ma_middlesex_instance_id')
        self.engine = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL(
                drivername=_DATABASE_TYPE,
                username=db_user,
                password=db_password,
                database=db_name,
                query={'host': '/cloudsql/{}'.format(cloudsql_instance_id)}))
        self.meta = sqlalchemy.MetaData()
        self.meta.reflect(bind=self.engine)

    def _load_data(self):
        self.address_df = pd.read_sql_table('address', self.engine)
        self.booking_df = pd.read_sql_table('booking', self.engine)
        self.admission_df = pd.read_sql_table('admission', self.engine)
        self.charge_df = pd.read_sql_table('charge', self.engine)
        self.bond_df = pd.read_sql_table('bond', self.engine)
        self.hold_df = pd.read_sql_table('hold', self.engine)

    def _parse(self,
               args: IngestArgs,
               contents: Iterable[Dict]) -> IngestInfo:
        return self.parser.parse(contents)

    def go(self):
        """Iterates through data exports in chronological order and persists
        converted data. After each export is successfully persisted its rows are
        dropped from the cloud SQL database. If conversion or persistence run
        into an error, all following exports are not processed."""
        for export_time in sorted(set(self.booking_df['export_time'])):
            self.run_ingest(IngestArgs(ingest_time=export_time))
        logging.info("Successfully persisted all data exports.")

    def _job_tag(self, args: IngestArgs) -> str:
        return f'{self.region.region_code}:{args.ingest_time}'

    def _read_contents(self, args: IngestArgs) -> Iterable[Dict]:
        """Queries the postgresql database for the most recent extract and reads
        the results to JSON."""

        export_time = args.ingest_time

        def get_current_export(df):
            curr = df[df['export_time'] == export_time].fillna('').astype(str)
            if curr.empty:
                raise DirectIngestError(
                    error_type=DirectIngestErrorType.READ_ERROR,
                    msg="Table missing data for export time [{export_time}]")
            return curr

        address_df = get_current_export(self.address_df)
        booking_df = get_current_export(self.booking_df)
        admission_df = get_current_export(self.admission_df)
        charge_df = get_current_export(self.charge_df)
        bond_df = get_current_export(self.bond_df)
        hold_df = get_current_export(self.hold_df)

        for sysid in booking_df['sysid']:
            person_bookings = booking_df[booking_df['sysid'] == sysid]
            person_admissions = admission_df[
                admission_df['sysid'] == sysid]
            person_address = address_df[address_df['sysid'] == sysid]
            person_charges = charge_df[charge_df['sysid'] == sysid]
            person_bonds = bond_df[bond_df['sysid'] == sysid]
            person_holds = hold_df[hold_df['sysid'] == sysid]
            person_dict = {
                'booking': one(person_bookings.to_dict('records')),
                'admission': person_admissions.to_dict('records'),
                'address': person_address.to_dict('records'),
                'bond': person_bonds.to_dict('records'),
                'charge': person_charges.to_dict('records'),
                'hold': person_holds.to_dict('records')}
            yield person_dict

    def _do_cleanup(self, args: IngestArgs):
        """Removes all rows in all tables for a single export time."""

        export_time = args.ingest_time
        if environment.in_gae_production():
            for table in reversed(self.meta.sorted_tables):
                result = self.engine.execute(
                    table.delete().where(table.c.export_time == export_time))
                logging.debug(
                    "Deleted [%d] rows from table [%s] with export time [%s]",
                    result.rowcount, table, export_time)
        else:
            logging.debug("Skipping row deletion for us_ma_middlesex tables "
                          "and export time [%s] outside of prod environment.",
                          export_time)

    def get_enum_overrides(self) -> EnumOverrides:
        """Overridden values for enum fields."""
        builder = super(
            UsMaMiddlesexController, self).get_enum_overrides().to_builder()
        builder.add('15 DAY PAROLE DETAINER', AdmissionReason.PAROLE_VIOLATION)
        builder.add('2', ChargeStatus.SENTENCED)
        builder.add('BAIL', ReleaseReason.BOND)
        builder.add('BAILED OUT', ChargeStatus.PRETRIAL)
        builder.ignore('C', BondStatus)
        builder.add('CASU', BondType.CASH)
        builder.ignore('CIVIL CAPIAS', AdmissionReason)  # Civil bench warrant.
        builder.add('CONTEMPT OF COURT', AdmissionReason.NEW_COMMITMENT)
        builder.add('COURT ORDERED', ReleaseReason.OWN_RECOGNIZANCE)
        builder.add('END OF SENTENCE', ReleaseReason.EXPIRATION_OF_SENTENCE)
        builder.add('HWO', BondType.DENIED)
        builder.ignore('N', BondStatus)
        builder.add('NB', BondType.NOT_REQUIRED)
        builder.add('NOL PROS', ChargeStatus.DROPPED)
        builder.add('O', BondStatus.SET)
        builder.ignore('OTH', BondType)
        builder.add('PERMANENT PAROLE DETAINER',
                    AdmissionReason.PAROLE_VIOLATION)
        builder.add('P', BondStatus.POSTED)
        builder.add('PERSONAL RECOGNIZANCE', ChargeStatus.PRETRIAL)
        builder.add('PT', ChargeStatus.PRETRIAL)
        builder.add('RELEASED BY AUTHORITY OF COURT', ChargeStatus.PRETRIAL)
        builder.ignore('S', BondStatus)
        builder.add('SENTENCED', ReleaseReason.TRANSFER)
        builder.ignore('TURNED OVER TO NEW JURISDICTION', ChargeStatus)
        # TODO (#1897) make sure charge class is filled in. This is a
        # parole violation.
        builder.add('VL', ChargeStatus.PRETRIAL)
        builder.add('WARRANT MANAGEMENT SYSTEM', AdmissionReason.NEW_COMMITMENT)

        return builder.build()
