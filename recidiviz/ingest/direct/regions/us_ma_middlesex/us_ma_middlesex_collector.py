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
from typing import Iterable, Dict

import pandas as pd
import sqlalchemy
from more_itertools import one

from recidiviz import IngestInfo
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.direct.collector import DirectIngestCollector
from recidiviz.ingest.direct.errors import DirectIngestError
from recidiviz.ingest.direct.regions.us_ma_middlesex.us_ma_middlesex_parser \
    import UsMaMiddlesexParser
from recidiviz.ingest.scrape import ingest_utils
from recidiviz.persistence import persistence
from recidiviz.utils import secrets, environment

_DATABASE_TYPE = 'postgresql'
_DB_USER = secrets.get_secret('us_ma_middlesex_db_user')
_DB_PASSWORD = secrets.get_secret('us_ma_middlesex_db_password')
_DB_NAME = secrets.get_secret('us_ma_middlesex_db_name')
_CLOUDSQL_INSTANCE_ID = secrets.get_secret('us_ma_middlesex_instance_id')


class UsMaMiddlesexCollector(DirectIngestCollector):
    """Reads tables from our us_ma_middlesex postgres db, which is an extract of
    the county's own sql database. The data flows through several formats, from
    postgres tables to dataframes to python dictionaries to IngestInfo objects.
    """

    def __init__(self):
        """Initialize the collector and read tables from cloud SQL."""
        super(UsMaMiddlesexCollector, self).__init__('us_ma_middlesex')

        self.engine = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL(
                drivername=_DATABASE_TYPE,
                username=_DB_USER,
                password=_DB_PASSWORD,
                database=_DB_NAME,
                query={'host': '/cloudsql/{}'.format(_CLOUDSQL_INSTANCE_ID)}))
        self.meta = sqlalchemy.MetaData()
        self.meta.reflect(bind=self.engine)

        self.address_df = pd.read_sql_table('address', self.engine)
        self.booking_df = pd.read_sql_table('booking', self.engine)
        self.admission_df = pd.read_sql_table('admission', self.engine)
        self.charge_df = pd.read_sql_table('charge', self.engine)
        self.bond_df = pd.read_sql_table('bond', self.engine)
        self.hold_df = pd.read_sql_table('hold', self.engine)

        self.parser = UsMaMiddlesexParser()

    # pylint: disable=arguments-differ  # TODO(#1825)
    def parse(self, json_people: Iterable[Dict]) -> IngestInfo:
        return self.parser.parse(json_people)

    def go(self):
        """Iterates through data exports in chronological order and persists
        converted data. After each export is successfully persisted its rows are
        dropped from the cloud SQL database. If conversion or persistence run
        into an error, all following exports are not processed."""
        for export_time in sorted(set(self.booking_df['export_time'])):
            ii = self.parse(self.read_tables_to_json(export_time))
            success = self.persist(ii, export_time.to_pydatetime())
            if success:
                self.clear(export_time)
            else:
                raise DirectIngestError("Persisting failed for export time "
                                        f"[{export_time}], halting ingest.")

    def read_tables_to_json(self, export_time: pd.Timestamp) -> Iterable[Dict]:
        """Queries the postgresql database for the most recent extract and reads
        the results to JSON."""

        def get_current_export(df):
            curr = df[df['export_time'] == export_time].fillna('').astype(str)
            if curr.empty:
                raise DirectIngestError("Table missing data for export time "
                                        f"[{export_time}]")
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

    # pylint: disable=arguments-differ  # TODO(#1825)
    def persist(self, ingest_info: IngestInfo,  # type: ignore  # TODO(#1825)
                export_time: datetime.datetime) -> bool:
        """Writes ingested people to the database, using the data's export time
        as the ingest time."""
        ingest_info_proto = ingest_utils.convert_ingest_info_to_proto(
            ingest_info)

        metadata = IngestMetadata(self.region.region_code,
                                  self.region.jurisdiction_id,
                                  export_time,
                                  self.region.get_enum_overrides())

        return persistence.write(ingest_info_proto, metadata)

    def clear(self, export_time: pd.Timestamp):
        """Removes all rows in all tables for a single export time."""
        if environment.get_gae_environment() == 'production':
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
