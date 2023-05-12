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
"""Tests for utils/endpoint_helpers.py."""
import unittest
from typing import Optional
from unittest import mock
from unittest.mock import patch

from flask import Flask

from recidiviz.utils.endpoint_helpers import get_value_from_request

app = Flask(__name__)


class MyTestCase(unittest.TestCase):
    @patch("flask.Request.get_data")
    def test_get_value_from_request_gets_string(
        self, mock_get_data: mock.MagicMock
    ) -> None:
        with app.test_request_context():
            mock_get_data.return_value = '{"key": "value"}'
            result: Optional[str] = get_value_from_request("key")

            self.assertEqual("value", result)

    @patch("flask.Request.get_data")
    def test_get_value_from_request_gets_int(
        self, mock_get_data: mock.MagicMock
    ) -> None:
        with app.test_request_context():
            mock_get_data.return_value = '{"key": 1}'
            result: Optional[int] = get_value_from_request("key")

            self.assertEqual(1, result)

    @patch("flask.Request.get_data")
    def test_get_value_from_request_gets_none(
        self, mock_get_data: mock.MagicMock
    ) -> None:
        with app.test_request_context():
            mock_get_data.return_value = '{"key": "value"}'
            result = get_value_from_request("random_key")

            self.assertEqual(None, result)
