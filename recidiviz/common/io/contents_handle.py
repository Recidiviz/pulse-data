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
"""Defines an abstract interface that can be used to access contents of a data source.
"""
import abc
from typing import Generic, Iterator, TypeVar

# Type for a single row/chunk returned by the contents iterator.
ContentsRowType = TypeVar("ContentsRowType")


class ContentsHandle(Generic[ContentsRowType]):
    """Defines an abstract interface that can be used to access contents of a data
    source.
    """

    @abc.abstractmethod
    def get_contents_iterator(self) -> Iterator[ContentsRowType]:
        """Should be overridden by subclasses to return an iterator over contents of
        the desired format. Will throw if the contents could not be read (i.e. if they
        no longer exist).
        """
