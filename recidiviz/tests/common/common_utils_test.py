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
"""Tests for methods in common_utils.py."""
import unittest
from mock import MagicMock, call

from google.api_core import exceptions  # pylint: disable=no-name-in-module

from recidiviz.common.common_utils import create_generated_id, \
    is_generated_id, retry_grpc, create_synthetic_id, get_external_id

GO_AWAY_ERROR = exceptions.InternalServerError('500 GOAWAY received')
DEADLINE_EXCEEDED_ERROR = exceptions.InternalServerError('504 Deadline '
                                                         'Exceeded')
OTHER_ERROR = exceptions.InternalServerError('500 received')


class CommonUtilsTest(unittest.TestCase):
    """Tests for common_utils.py."""

    def test_create_generated_id(self):
        generated_id = create_generated_id()
        self.assertTrue(generated_id.endswith("_GENERATE"))

    def test_is_generated_id(self):
        id_str = "id_str_GENERATE"
        self.assertTrue(is_generated_id(id_str))

    def test_is_not_generated_id(self):
        id_str = "id_str"
        self.assertFalse(is_generated_id(id_str))

    def test_create_synthetic_id(self):
        id_str = "id_str"
        id_type = "id_type"
        self.assertEqual("id_type:id_str", create_synthetic_id(
            external_id=id_str, id_type=id_type))

    def test_get_external_id(self):
        synthetic_id = "id_type:11:111"
        self.assertEqual("11:111", get_external_id(synthetic_id=synthetic_id))

    def test_retry_grpc_no_raise(self):
        fn = MagicMock()
        # Two GOAWAY errors, 1 DEADLINE_EXCEEDED, then works
        fn.side_effect = [GO_AWAY_ERROR] * 2 + [DEADLINE_EXCEEDED_ERROR] + [3]

        result = retry_grpc(3, fn, 1, b=2)

        self.assertEqual(result, 3)
        fn.assert_has_calls([call(1, b=2)] * 3)

    def test_retry_grpc_raises(self):
        fn = MagicMock()
        # Always a GOAWAY error
        fn.side_effect = GO_AWAY_ERROR

        with self.assertRaises(exceptions.InternalServerError):
            retry_grpc(3, fn, 1, b=2)

        fn.assert_has_calls([call(1, b=2)] * 4)

    def test_retry_grpc_raises_no_goaway(self):
        fn = MagicMock()
        # Always a different error
        fn.side_effect = OTHER_ERROR

        with self.assertRaises(exceptions.InternalServerError):
            retry_grpc(3, fn, 1, b=2)

        fn.assert_has_calls([call(1, b=2)])

    def test_retry_grpc_raises_deadline_exceeded(self):
        fn = MagicMock()
        # Always a DEADLINE_EXCEEDED error
        fn.side_effect = DEADLINE_EXCEEDED_ERROR

        with self.assertRaises(exceptions.InternalServerError):
            retry_grpc(3, fn, 1, b=2)

        fn.assert_has_calls([call(1, b=2)] * 4)
