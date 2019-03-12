# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
from datetime import datetime

import attr

from recidiviz.common.attr_mixins import DefaultableAttr
from recidiviz.common.constants.enum_overrides import EnumOverrides


@attr.s(frozen=True)
class IngestMetadata(DefaultableAttr):
    """Metadata used to construct entity objects from ingest_info objects."""

    # The region code for the region that this ingest_info was scraped from.
    region: str = attr.ib()

    # The jurisdiction id for the region that this ingest_info was scraped from.
    jurisdiction_id: str = attr.ib()

    # The last time this ingest_info was seen from its data source. In the
    # normal ingest pipeline, this is the scraper_start_time.
    last_seen_time: datetime = attr.ib()

    # Region specific mapping which takes precedence over the global mapping.
    enum_overrides: EnumOverrides = attr.ib(factory=EnumOverrides.empty)
