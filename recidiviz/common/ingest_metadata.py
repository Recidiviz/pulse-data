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
from typing import Dict, Any, Optional

import attr

from recidiviz.common.buildable_attr import BuildableAttr
from recidiviz.common.constants.mappable_enum import MappableEnum


def _normalize_keys(dictionary: Dict[str, Any]) -> Dict[str, Any]:
    return {k.upper(): v for k, v in dictionary.items()}


@attr.s(frozen=True)
class IngestMetadata(BuildableAttr):
    """Metadata used to construct entity objects from ingest_info objects."""

    # The region that this ingest_info was scraped from.
    region: str = attr.ib()

    # The last time this ingest_info was seen from its data source. In the
    # normal ingest pipeline, this is the scraper_start_time.
    last_seen_time: datetime = attr.ib()

    # Region specific mapping which takes precedence over the global mapping.
    enum_overrides: Dict[str, Optional[MappableEnum]] = attr.ib(
        factory=dict, converter=_normalize_keys)
