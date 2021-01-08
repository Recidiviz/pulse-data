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

from recidiviz.common.attr_mixins import DefaultableAttr
from recidiviz.common.constants.enum_overrides import EnumOverrides


@enum.unique
class SystemLevel(enum.Enum):
    COUNTY = 'COUNTY'
    STATE = 'STATE'


@attr.s(frozen=True)
class IngestMetadata(DefaultableAttr):
    """Metadata used to construct entity objects from ingest_info objects."""

    # The region code for the region that this ingest_info was ingested from.
    # e.g. us_nd or us_ca or us_va_prince_william or us_ny_westchester
    region: str = attr.ib()

    # The jurisdiction id for the region that this ingest_info was ingested
    # from.
    jurisdiction_id: str = attr.ib()

    # The time the given ingest work started. In the normal scraping pipeline,
    # for example, this is the scraper_start_time.
    ingest_time: datetime = attr.ib()

    # Region specific mapping which takes precedence over the global mapping.
    enum_overrides: EnumOverrides = attr.ib(factory=EnumOverrides.empty)

    # The system level from which data is being ingested, e.g. COUNTY or STATE
    system_level: SystemLevel = attr.ib(default=SystemLevel.COUNTY)

    # The default facility id for the region e.g. 01CNT02502506100
    facility_id: Optional[str] = attr.ib(default=None)
