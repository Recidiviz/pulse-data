# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for on_exit"""

import sys
from unittest.mock import Mock

import pytest

from recidiviz.utils.context import on_exit


def test_on_exit_standard_return() -> None:
    # Arrange
    mock = Mock()

    def standard_return() -> int:
        with on_exit(mock, "foo", x=5):
            return 1

    # Act
    result = standard_return()

    # Assert
    assert result == 1
    mock.assert_called_once_with("foo", x=5)


def test_on_exit_throws() -> None:
    # Arrange
    mock = Mock()

    def throws() -> int:
        with on_exit(mock, "foo", x=5):
            raise ValueError("muahaha")

    # Act
    with pytest.raises(ValueError):
        throws()

    # Assert
    mock.assert_called_once_with("foo", x=5)


def test_on_exit_runnable_throws() -> None:
    # Arrange
    mock = Mock(side_effect=ValueError("muahaha"))

    def runnable_throws() -> int:
        with on_exit(mock, "foo", x=5):
            return 1

    # Act
    with pytest.raises(ValueError):
        runnable_throws()

    # Assert
    mock.assert_called_once_with("foo", x=5)


def test_on_exit_sysexit() -> None:
    # Arrange
    mock = Mock()

    def exits() -> int:
        with on_exit(mock, "foo", x=5):
            sys.exit(1)

    # Act
    with pytest.raises(SystemExit):
        exits()

    # Assert
    mock.assert_called_once_with("foo", x=5)


def test_on_exit_runnable_sysexit() -> None:
    # Arrange
    mock = Mock(side_effect=lambda m, x: sys.exit(1))

    def runnable_exits() -> int:
        with on_exit(mock, "foo", x=5):
            return 1

    # Act
    with pytest.raises(SystemExit):
        runnable_exits()

    # Assert
    mock.assert_called_once_with("foo", x=5)
