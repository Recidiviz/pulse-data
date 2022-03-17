# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines an ingest file type enum."""
from enum import Enum


# TODO(#11424): We should be able to eliminate this enum entirely once we have eliminated
#  file-based materialization for ingest view results. The only type of file in the
#  GCS ingest buckets will be raw data files.
class GcsfsDirectIngestFileType(Enum):
    """Denotes the type of a file encountered by the BaseDirectIngestController. Files will have their type added to
    the normalized name and this type will be used to determine how to handle the file (import to BigQuery vs ingest
    directly to Postgres). When moved to storage, files with different file types will live in different subdirectories
    in a region's storage bucket."""

    # Raw data received directly from state
    RAW_DATA = "raw"

    # TODO(#11424): Usages of this enum value should be deleted as part of the work to
    #  migrate ingest view query materialization to BQ. Delete entirely once we've
    #  shipped BQ-based materialization for all states.
    # Ingest-ready file
    INGEST_VIEW = "ingest_view"

    @classmethod
    def from_string(cls, type_str: str) -> "GcsfsDirectIngestFileType":
        if type_str == GcsfsDirectIngestFileType.RAW_DATA.value:
            return GcsfsDirectIngestFileType.RAW_DATA
        if type_str == GcsfsDirectIngestFileType.INGEST_VIEW.value:
            return GcsfsDirectIngestFileType.INGEST_VIEW

        raise ValueError(f"Unknown direct ingest file type string: [{type_str}]")
