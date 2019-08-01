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
"""Tests for GcsfsDirectIngestController."""
import datetime
import json
import unittest

from recidiviz.common.serialization import attr_to_json_dict, \
    datetime_to_serializable, serializable_to_datetime, attr_from_json_dict
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsIngestArgs


class TestGcsfsDirectIngestController(unittest.TestCase):
    """Tests for GcsfsDirectIngestController."""

    def test_serialize_gcsfs_ingest_args(self):
        now = datetime.datetime.now()

        str_now = datetime_to_serializable(now)
        print(str_now)
        now_converted = serializable_to_datetime(str_now)
        print(now_converted)

        self.assertTrue(now, now_converted)

        args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path='foo/bar.csv',
            storage_bucket='recidiviz-storage'
        )

        args_dict = attr_to_json_dict(args)
        print(f'type = {type(args_dict)}')
        print(args_dict)

        serialized = json.dumps(args_dict).encode()
        print(f'type = {type(serialized)}')
        print(serialized)

        args_dict = json.loads(serialized)
        print(f'type = {type(args_dict)}')
        print(args_dict)

        result_args = attr_from_json_dict(args_dict)
        print(f'type = {type(result_args)}')
        print(result_args)

        self.assertEqual(args, result_args)
