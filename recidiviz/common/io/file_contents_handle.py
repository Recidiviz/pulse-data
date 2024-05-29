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
from typing import Generic, Iterator, Protocol, TypeVar, Union

from recidiviz.common.io.contents_handle import ContentsHandle

FileLineType_co = TypeVar("FileLineType_co", bound=Union[str, bytes], covariant=True)


class ReadLineIO(Protocol[FileLineType_co]):
    """Protocol for any class that has readline() implemented."""

    @abc.abstractmethod
    def readline(self, limit: int = -1) -> FileLineType_co:
        pass


IoType = TypeVar("IoType", bound=ReadLineIO)


class FileContentsHandle(ContentsHandle, Generic[FileLineType_co, IoType]):
    """Defines an abstract interface that can be used to access file contents."""

    @abc.abstractmethod
    @contextmanager
    def open(self, mode: str = "r") -> Iterator[IoType]:
        """Should be overridden by subclasses to return a way to open a file stream."""

    def get_contents_iterator(self) -> Iterator[FileLineType_co]:
        """Lazy function (generator) to read a file line by line."""
        with self.open() as f:
            while line := f.readline():
                yield line
