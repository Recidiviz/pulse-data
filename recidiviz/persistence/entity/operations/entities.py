# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Domain logic entities for operations database data.

Note: These classes mirror the SQL Alchemy ORM objects but are kept separate. This allows these persistence layer
objects additional flexibility that the SQL Alchemy ORM objects can't provide.
"""

import datetime
from typing import Optional

import attr

from recidiviz.common.attr_mixins import BuildableAttr, DefaultableAttr
from recidiviz.persistence.entity.base_entity import entity_graph_eq


@attr.s(eq=False)
class OperationsEntity:
    """Base class for all entity types."""
    # Consider Entity abstract and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is OperationsEntity:
            raise Exception('Abstract class cannot be instantiated')
        return super().__new__(cls)

    def __eq__(self, other):
        return entity_graph_eq(self, other)


@attr.s(eq=False)
class DirectIngestFileMetadata(OperationsEntity, BuildableAttr, DefaultableAttr):
    """A Base class which defines all fields common each of the direct ingest file metadata tables."""
    file_id: int = attr.ib()

    region_code: str = attr.ib()
    file_tag: str = attr.ib()

    processed_time: Optional[datetime.datetime] = attr.ib()


@attr.s(eq=False)
class DirectIngestRawFileMetadata(DirectIngestFileMetadata):
    """Metadata about a raw file imported directly from a particular region."""
    discovery_time: datetime.datetime = attr.ib()
    normalized_file_name: str = attr.ib()
    datetimes_contained_upper_bound_inclusive: datetime.datetime = attr.ib()


@attr.s(eq=False)
class DirectIngestIngestFileMetadata(DirectIngestFileMetadata):
    """Metadata about a SQL-preprocessed, persistence-ready direct ingest files."""
    is_invalidated: bool = attr.ib()
    job_creation_time: datetime.datetime = attr.ib()
    datetimes_contained_lower_bound_exclusive: Optional[datetime.datetime] = attr.ib()
    datetimes_contained_upper_bound_inclusive: datetime.datetime = attr.ib()
    normalized_file_name: Optional[str] = attr.ib()
    export_time: Optional[datetime.datetime] = attr.ib()

    discovery_time: Optional[datetime.datetime] = attr.ib()
