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
import datetime
import logging
from typing import Dict, Optional, Set, Iterator

import pandas as pd
import sqlalchemy
from more_itertools import one, peekable

from recidiviz import IngestInfo
from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.booking import AdmissionReason, \
    ReleaseReason
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.cloud_storage.content_types import FileContentsHandle
from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.ingest.direct.regions.us_ma_middlesex.us_ma_middlesex_parser \
    import UsMaMiddlesexParser
from recidiviz.utils import secrets, environment

_DATABASE_TYPE = 'postgresql'


def _create_engine():
    db_user = secrets.get_secret('us_ma_middlesex_db_user')
    db_password = secrets.get_secret('us_ma_middlesex_db_password')
    db_name = secrets.get_secret('us_ma_middlesex_db_name')
    cloudsql_instance_id = secrets.get_secret('us_ma_middlesex_instance_id')
    return sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL(
            drivername=_DATABASE_TYPE,
            username=db_user,
            password=db_password,
            database=db_name,
            query={'host': '/cloudsql/{}'.format(cloudsql_instance_id)}))


class UsMaMiddlesexContentsHandle(FileContentsHandle[Dict]):
    """Class that provides an iterator over Middlesex SQL data."""

    def __init__(self, args: IngestArgs):
        self.args = args

    def get_contents_iterator(self) -> Iterator[Dict]:
        """Queries the postgresql database for the most recent extract and reads
        the results to JSON."""

        export_time = self.args.ingest_time
        engine = _create_engine()

        def query_table(table_name: str) -> pd.DataFrame:
            query = (f'select * from {table_name} '
                     f"WHERE export_time = TIMESTAMP '{export_time}'")
            return pd.read_sql_query(query, con=engine).fillna('').astype(str)

        address_df = query_table('address')
        booking_df = query_table('booking')
        admission_df = query_table('admission')
        charge_df = query_table('charge')
        bond_df = query_table('bond')
        hold_df = query_table('hold')

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


class UsMaMiddlesexController(
        BaseDirectIngestController[IngestArgs, UsMaMiddlesexContentsHandle]):
    """Reads tables from our us_ma_middlesex postgres db, which is an extract of
    the county's own sql database. The data flows through several formats, from
    postgres tables to dataframes to python dictionaries to IngestInfo objects.

    Each job reads the earliest data export in the database and persists
    converted data. After each export is successfully persisted its rows are
    dropped from the cloud SQL database. If conversion or persistence run
    into an error, all following exports are not processed. Otherwise, another
    job is queued and the next-oldest export is processed, until no rows are
    left in the cloud SQL database.
    """

    def __init__(self):
        super().__init__('us_ma_middlesex',
                                                      SystemLevel.COUNTY)
        self.scheduled_ingest_times: Set[datetime.datetime] = set()

    # ============== #
    # JOB SCHEDULING #
    # ============== #

    def _get_next_job_args(self) -> Optional[IngestArgs]:
        df = pd.read_sql_query('SELECT MIN(export_time) FROM booking',
                               _create_engine())
        ingest_time = df[min][0]
        if not ingest_time:
            logging.info("No more export times - successfully persisted all "
                         "data exports.")
            return None
        if ingest_time in self.scheduled_ingest_times:
            raise DirectIngestError(
                msg=f"Received a second job for ingest time [{ingest_time}]. "
                "Did the previous job delete this export from the database?",
                error_type=DirectIngestErrorType.CLEANUP_ERROR)

        return IngestArgs(ingest_time=ingest_time)

    def _on_job_scheduled(self, ingest_args: IngestArgs):
        self.scheduled_ingest_times.add(ingest_args.ingest_time)

    # =================== #
    # SINGLE JOB RUN CODE #
    # =================== #

    def _get_contents_handle(
            self, args: IngestArgs) -> Optional[UsMaMiddlesexContentsHandle]:
        return UsMaMiddlesexContentsHandle(args)

    def _can_proceed_with_ingest_for_contents(
            self, _args: IngestArgs, _contents_handle: UsMaMiddlesexContentsHandle):
        return True

    def _parse(self,
               args: IngestArgs,
               contents_handle: UsMaMiddlesexContentsHandle) -> IngestInfo:
        return UsMaMiddlesexParser().parse(
            contents_handle.get_contents_iterator())

    def _job_tag(self, args: IngestArgs) -> str:
        return f'{self.region.region_code}:{args.ingest_time}'

    def _are_contents_empty(
            self,
            args: IngestArgs,
            contents_handle: UsMaMiddlesexContentsHandle) -> bool:
        """Checks if there are any content Dicts to process, returns True if not
        (i.e. there are no elements in the Iterable).
        """
        return not bool(peekable(contents_handle.get_contents_iterator()))

    def _do_cleanup(self, args: IngestArgs):
        """Removes all rows in all tables for a single export time."""

        export_time = args.ingest_time
        if environment.in_gae_production():
            engine = _create_engine()
            meta = sqlalchemy.MetaData()
            meta.reflect(bind=engine)
            for table in reversed(meta.sorted_tables):
                result = engine.execute(
                    table.delete().where(table.c.export_time == export_time))
                logging.info(
                    "Deleted [%d] rows from table [%s] with export time [%s]",
                    result.rowcount, table, export_time)
        else:
            logging.info("Skipping row deletion for us_ma_middlesex tables and "
                         "export time [%s] outside of prod environment.",
                         export_time)

    def get_enum_overrides(self) -> EnumOverrides:
        """Overridden values for enum fields."""
        builder = super().get_enum_overrides().to_builder()
        builder.add('15 DAY PAROLE DETAINER', AdmissionReason.PAROLE_VIOLATION)
        builder.add('2', ChargeStatus.SENTENCED)
        builder.add('BAIL', ReleaseReason.BOND)
        builder.add('BAILED OUT', ChargeStatus.PRETRIAL)
        builder.ignore('C', BondStatus)
        builder.add('CASU', BondType.CASH)
        builder.ignore('CIVIL CAPIAS', AdmissionReason)  # Civil bench warrant.
        builder.add('CONTEMPT OF COURT', AdmissionReason.NEW_COMMITMENT)
        builder.add('CRIMINAL COMPLAINT', AdmissionReason.NEW_COMMITMENT)
        builder.add('COURT ORDERED', ReleaseReason.OWN_RECOGNIZANCE)
        builder.add('END OF SENTENCE', ReleaseReason.EXPIRATION_OF_SENTENCE)
        builder.add('FEDERAL DETAINER', AdmissionReason.NEW_COMMITMENT)
        builder.add('HWO', BondType.DENIED)
        builder.add('MITTIMUS FOR FINES', AdmissionReason.NEW_COMMITMENT)
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
        builder.add('REMOVED TO A MENTAL HOSPITAL', ChargeStatus.PRETRIAL)
        builder.ignore('S', BondStatus)
        builder.add('SENTENCED', ReleaseReason.TRANSFER)
        builder.ignore('TRANSFERRED TO CORRECTIONAL INSTI', ChargeStatus)
        builder.ignore('TURNED OVER TO NEW JURISDICTION', ChargeStatus)
        # TODO(#1897) make sure charge class is filled in. This is a
        #  parole violation.
        builder.add('VL', ChargeStatus.PRETRIAL)
        builder.add('WARRANT MANAGEMENT SYSTEM', AdmissionReason.NEW_COMMITMENT)

        return builder.build()
