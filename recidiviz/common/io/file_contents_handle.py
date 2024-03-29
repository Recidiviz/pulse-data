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
"""Defines an abstract interface that can be used to access file contents."""
import abc
from contextlib import contextmanager
from typing import Generic, Iterator, TypeVar

from recidiviz.common.io.contents_handle import ContentsHandle, ContentsRowType

IoType = TypeVar("IoType")


class FileContentsHandle(ContentsHandle, Generic[ContentsRowType, IoType]):
    """Defines an abstract interface that can be used to access file contents."""

    @abc.abstractmethod
    @contextmanager
    def open(self, mode: str = "r") -> Iterator[IoType]:
        """Should be overridden by subclasses to return a way to open a file stream."""
