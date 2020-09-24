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
"""Defines types used for general cloud storage code."""
import abc
from typing import TypeVar, Generic, Iterator

# Type for a single row/chunk returned by the ingest contents iterator.
FileContentsRowType = TypeVar('IngestContentsRowType')


class FileContentsHandle(Generic[FileContentsRowType]):
    @abc.abstractmethod
    def get_contents_iterator(self) -> Iterator[FileContentsRowType]:
        """Should be overridden by subclasses to return an iterator over contents of the desired format.
        Will throw if the contents could not be read (i.e. if they no longer
        exist).
        """
