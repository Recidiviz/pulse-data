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
# ============================================================================
"""Metadata used for the extract and merge step of ingest."""

from datetime import datetime

import attr

from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


@attr.s(frozen=True, kw_only=True)
class IngestMetadata:
    """Metadata used for the extract and merge step of ingest."""

    # The region code for the region that this data was ingested from.
    # e.g. us_nd or us_ca
    region: str = attr.ib()

    # The time the given ingest work started. In the normal scraping pipeline,
    # for example, this is the scraper_start_time.
    ingest_time: datetime = attr.ib()

    # The key for the database that ingest data should be written to.
    database_key: SQLAlchemyDatabaseKey = attr.ib()
