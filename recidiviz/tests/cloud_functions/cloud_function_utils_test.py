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
"""Tests for cloud_function_utils.py."""
from unittest import TestCase

from recidiviz.cloud_functions.cloud_function_utils import build_query_param_string


class CloudFunctionUtilsTest(TestCase):
    """Tests for cloud_function_utils.py."""

    def test_build_query_param_string(self) -> None:
        """Given valid request params, it returns a query param string."""
        request_params = {
            "batch_id": "20201120051030",
            "redirect_address": "dev@recidiviz.org",
            "cc_address": ["cc1@domain.org", "cc2@domain.org"],
        }

        accepted_query_params = ["batch_id", "redirect_address", "cc_address"]
        expected = (
            "?batch_id=20201120051030&redirect_address=dev%40recidiviz.org&cc_address=cc1%40domain.org&"
            "cc_address=cc2%40domain.org"
        )
        self.assertEqual(
            expected, build_query_param_string(request_params, accepted_query_params)
        )

    def test_build_query_param_string_invalid_request_params(self) -> None:
        """Given invalid request params, it raises a KeyError"""
        request_params = {"invalid_param": "mystery value"}
        accepted_query_params = ["batch_id", "redirect_address", "cc_address"]
        with self.assertRaises(KeyError):
            build_query_param_string(request_params, accepted_query_params)

    def test_build_query_param_string_empty_value(self) -> None:
        """Given valid request params with empty values, it does not include it in the query param string"""
        request_params = {
            "batch_id": "20201120051030",
            "redirect_address": None,
            "cc_address": ["cc1@domain.org", "cc2@domain.org"],
        }
        accepted_query_params = ["batch_id", "redirect_address", "cc_address"]
        expected = "?batch_id=20201120051030&cc_address=cc1%40domain.org&cc_address=cc2%40domain.org"
        self.assertEqual(
            expected, build_query_param_string(request_params, accepted_query_params)
        )
