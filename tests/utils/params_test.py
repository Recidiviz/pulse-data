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
# =============================================================================

"""Tests for utils/params.py."""


from utils import params


PARAMS = [("region", "us_mo"), ("scrape_type", "background")]


def test_get_param():
    assert params.get_param("region", PARAMS) == "us_mo"


def test_get_param_default():
    assert params.get_param("foo", PARAMS, default="bar") == "bar"


def test_get_param_no_default():
    assert not params.get_param("foo", PARAMS)


def test_get_param_explicitly_none_default():
    assert not params.get_param("foo", PARAMS, default=None)
