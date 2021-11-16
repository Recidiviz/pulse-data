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
"""Metadata used to construct entity objects from ingest_info objects."""

import enum
from datetime import datetime
from typing import Optional

import attr

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import (
    DirectIngestSchemaType,
    SchemaType,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils.regions import Region


@enum.unique
class SystemLevel(enum.Enum):
    """Distinguishes between the STATE (state schema) and COUNTY (jails schema) parts
    of our system.
    """

    COUNTY = "COUNTY"
    STATE = "STATE"

    def schema_type(
        self,
    ) -> DirectIngestSchemaType:
        if self == SystemLevel.STATE:
            return SchemaType.STATE
        if self == SystemLevel.COUNTY:
            return SchemaType.JAILS

        raise ValueError(f"Unsupported SystemLevel type: {self}")

    @classmethod
    def for_region(cls, region: Region) -> "SystemLevel":
        return cls.for_region_code(region.region_code, region.is_direct_ingest)

    @classmethod
    def for_region_code(cls, region_code: str, is_direct_ingest: bool) -> "SystemLevel":
        if is_direct_ingest is None:
            raise ValueError(
                "Region flag is_direct_ingest is None, expected boolean value."
            )
        if not is_direct_ingest:
            # There are some scrapers that scrape state jails websites (e.g.
            # recidiviz/ingest/scrape/regions/us_pa/us_pa_scraper.py) which we always
            # write to the Vera county jails database.
            return SystemLevel.COUNTY

        if StateCode.is_state_code(region_code.upper()):
            return SystemLevel.STATE
        return SystemLevel.COUNTY


@attr.s(frozen=True, kw_only=True)
class IngestMetadata:
    """Metadata used to construct entity objects from ingest_info objects."""

    # The region code for the region that this ingest_info was ingested from.
    # e.g. us_nd or us_ca or us_va_prince_william or us_ny_westchester
    region: str = attr.ib()

    # The time the given ingest work started. In the normal scraping pipeline,
    # for example, this is the scraper_start_time.
    ingest_time: datetime = attr.ib()

    # The system level from which data is being ingested, e.g. COUNTY or STATE
    system_level: SystemLevel = attr.ib()

    # The key for the database that ingest data should be written to.
    database_key: SQLAlchemyDatabaseKey = attr.ib()


# TODO(#8905): Move this to a scraper specific package once we have migrated all
#  direct ingest states to ingest mappings v2.
@attr.s(frozen=True, kw_only=True)
class LegacyStateAndJailsIngestMetadata(IngestMetadata):
    # The jurisdiction id for the region that this ingest_info was ingested
    # from.
    jurisdiction_id: str = attr.ib()

    # Region specific mapping which takes precedence over the global mapping.
    enum_overrides: EnumOverrides = attr.ib(factory=EnumOverrides.empty)

    # The default facility id for the region e.g. 01CNT02502506100
    facility_id: Optional[str] = attr.ib(default=None)
