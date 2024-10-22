# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Types used in the cloud_storage module."""
import json

import attr

from recidiviz.common import attr_validators


@attr.define
class CsvChunkBoundary:
    """Information about a given csv chunk boundary.
    Attributes:
        start_inclusive (int): the first byte of the row-safe csv chunk (0 indexed)
        end_exclusive (int): the last byte (exclusive) of the row-safe csv chunk
        chunk_num (int): the 0-indexed chunk sequence number
    """

    start_inclusive: int = attr.ib(validator=attr_validators.is_int)
    end_exclusive: int = attr.ib(validator=attr_validators.is_int)
    chunk_num: int = attr.ib(validator=attr_validators.is_int)

    def get_chunk_size(self) -> int:
        """Return the size of the chunk in bytes."""
        return self.end_exclusive - self.start_inclusive

    def serialize(self) -> str:
        data = [self.start_inclusive, self.end_exclusive, self.chunk_num]
        return json.dumps(data)

    @staticmethod
    def deserialize(json_str: str) -> "CsvChunkBoundary":
        data = json.loads(json_str)
        return CsvChunkBoundary(
            start_inclusive=data[0], end_exclusive=data[1], chunk_num=data[2]
        )
