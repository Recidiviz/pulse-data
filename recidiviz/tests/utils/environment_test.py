# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for utils/environment.py."""
import sys
from http import HTTPStatus
from typing import Tuple

import pytest
from mock import Mock, patch

import recidiviz
from recidiviz.utils import environment


@patch("os.getenv")
def test_in_prod_false(mock_os: Mock) -> None:
    mock_os.return_value = "not production"
    assert not environment.in_gcp()


@patch("os.getenv")
def test_in_prod_true(mock_os: Mock) -> None:
    mock_os.return_value = "production"
    assert environment.in_gcp()


def test_local_only_is_local() -> None:
    track = "Emerald Rush"

    @environment.local_only
    def get() -> Tuple[str, HTTPStatus]:
        return (track, HTTPStatus.OK)

    response = get()
    assert response == (track, HTTPStatus.OK)


@patch("os.getenv")
def test_local_only_is_prod(mock_os: Mock) -> None:
    track = "Emerald Rush"
    mock_os.return_value = "production"

    @environment.local_only
    def get() -> Tuple[str, HTTPStatus]:
        return (track, HTTPStatus.OK)

    with pytest.raises(RuntimeError) as e:
        _ = get()
    assert str(e.value) == "Not available, see service logs."


def test_test_in_test() -> None:
    assert environment.in_test()


def test_test_in_ci() -> None:
    """This test will fail when run locally."""
    assert environment.in_ci()


def test_test_only_is_test() -> None:
    track = "Emerald Rush"

    @environment.test_only
    def get() -> str:
        return track

    assert get() == track


def test_test_only_not_test() -> None:
    track = "Emerald Rush"

    @environment.test_only
    def get() -> str:
        return track

    with patch.dict("recidiviz.__dict__"), patch.object(sys, "modules", {}):
        del recidiviz.__dict__["called_from_test"]
        assert not hasattr(recidiviz, "called_from_test")

        with pytest.raises(RuntimeError) as exception:
            get()
        assert str(exception.value) == "Function may only be called from tests"

    assert hasattr(recidiviz, "called_from_test")
