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
from typing import List

from mock import patch, Mock

from recidiviz import IngestInfo
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.serialization import attr_to_json_dict, \
    datetime_to_serializable, serializable_to_datetime, attr_from_json_dict
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs
from recidiviz.ingest.models.ingest_info import StatePerson, Person
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    build_controller_for_tests, add_paths_with_tags_and_process
from recidiviz.tests.utils.fake_region import TEST_STATE_REGION, \
    TEST_COUNTY_REGION


class StateTestGcsfsDirectIngestController(GcsfsDirectIngestController):
    def __init__(self,
                 ingest_directory_path: str,
                 storage_directory_path: str):
        super().__init__(TEST_STATE_REGION.region_code,
                         SystemLevel.STATE,
                         ingest_directory_path,
                         storage_directory_path)

    def _get_file_tag_rank_list(self) -> List[str]:
        return ['tagA', 'tagB', 'tagC']

    def _parse(self, args: GcsfsIngestArgs, contents: str) -> IngestInfo:
        return IngestInfo(state_people=[
            StatePerson(full_name="MARTHA STEWART")
        ])


class CountyTestGcsfsDirectIngestController(GcsfsDirectIngestController):
    def __init__(self,
                 ingest_directory_path: str,
                 storage_directory_path: str):
        super().__init__(TEST_COUNTY_REGION.region_code,
                         SystemLevel.COUNTY,
                         ingest_directory_path,
                         storage_directory_path)

    def _get_file_tag_rank_list(self) -> List[str]:
        return ['tagA', 'tagB']

    def _parse(self, args: GcsfsIngestArgs, contents: str) -> IngestInfo:
        return IngestInfo(people=[
            Person(full_name="AL CAPONE")
        ])


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
class TestGcsfsDirectIngestController(unittest.TestCase):
    """Tests for GcsfsDirectIngestController."""

    FIXTURE_PATH_PREFIX = 'direct/controllers'

    def run_file_order_test_for_controller_cls(self, controller_cls):
        """Writes all expected files to the mock fs, then kicks the controller
        and ensures that all jobs are run to completion in the proper order."""

        controller = build_controller_for_tests(controller_cls,
                                                self.FIXTURE_PATH_PREFIX)

        # pylint:disable=protected-access
        file_tags = list(
            reversed(sorted(controller._get_file_tag_rank_list())))

        add_paths_with_tags_and_process(self, controller, file_tags)

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_STATE_REGION))
    def test_state_runs_files_in_order(self):
        self.run_file_order_test_for_controller_cls(
            StateTestGcsfsDirectIngestController)

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_COUNTY_REGION))
    def test_county_runs_files_in_order(self):
        self.run_file_order_test_for_controller_cls(
            CountyTestGcsfsDirectIngestController)

    @patch("recidiviz.utils.regions.get_region",
           Mock(return_value=TEST_STATE_REGION))
    def test_state_unexpected_tag(self):
        controller = build_controller_for_tests(
            StateTestGcsfsDirectIngestController,
            self.FIXTURE_PATH_PREFIX)

        file_tags = ['tagA', 'unexpected_tag', 'tagB', 'tagC']
        unexpected_tags = ['unexpected_tag']

        add_paths_with_tags_and_process(
            self, controller, file_tags, unexpected_tags)

    def test_serialize_gcsfs_ingest_args(self):
        now = datetime.datetime.now()

        str_now = datetime_to_serializable(now)
        now_converted = serializable_to_datetime(str_now)

        self.assertTrue(now, now_converted)

        args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path='foo/bar.csv',
        )

        args_dict = attr_to_json_dict(args)
        serialized = json.dumps(args_dict).encode()
        args_dict = json.loads(serialized)
        result_args = attr_from_json_dict(args_dict)
        self.assertEqual(args, result_args)
