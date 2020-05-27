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
"""Class with basic functionality for tests of all region-specific
GcsfsDirectIngestControllers.
"""

import abc
import os
import unittest
from typing import Type

from freezegun import freeze_time
from mock import patch, Mock
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz import IngestInfo
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    build_gcsfs_controller_for_tests, ingest_args_for_fixture_file
from recidiviz.tests.ingest.direct.fake_direct_ingest_gcs_file_system import FakeDirectIngestGCSFileSystem
from recidiviz.tests.utils import fakes
from recidiviz.tests.utils.test_utils import print_visible_header_label


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
@freeze_time('2019-09-27')
class BaseDirectIngestControllerTests(unittest.TestCase):
    """Class with basic functionality for tests of all region-specific
    GcsfsDirectIngestControllers.
    """

    @classmethod
    @abc.abstractmethod
    def region_code(cls) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        pass

    @classmethod
    @abc.abstractmethod
    def schema_base(cls) -> DeclarativeMeta:
        pass

    def setUp(self) -> None:
        self.maxDiff = 250000
        fakes.use_in_memory_sqlite_database(self.schema_base())

        self.controller = build_gcsfs_controller_for_tests(
            self.controller_cls(),
            self.fixture_path_prefix(),
            run_async=False,
            max_delay_sec_between_files=0)

        # Set entity matching error threshold to a diminishingly small number
        # for tests. We cannot set it to 0 because we throw when errors *equal*
        # the error threshold.
        self.entity_matching_error_threshold_patcher = patch(
            'recidiviz.persistence.persistence.ERROR_THRESHOLD',
            pow(1, -10))
        self.entity_matching_error_threshold_patcher.start()

    def tearDown(self) -> None:
        self.entity_matching_error_threshold_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

    @classmethod
    def fixture_path_prefix(cls):
        return os.path.join('direct', 'regions', cls.region_code().lower())

    def run_parse_file_test(self,
                            expected: IngestInfo,
                            fixture_file_name: str) -> IngestInfo:
        """Runs a test that reads and parses a given fixture file. Returns the
        parsed IngestInfo object for tests to run further validations."""
        args = ingest_args_for_fixture_file(self.controller,
                                            f'{fixture_file_name}.csv')

        if not isinstance(self.controller.fs, FakeDirectIngestGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeDirectIngestGCSFileSystem. Found instead "
                             f"type [{type(self.controller.fs)}]")
        self.controller.fs.test_add_path(args.file_path)

        # pylint:disable=protected-access
        fixture_contents_handle = self.controller._get_contents_handle(args)

        final_info = self.controller._parse(args, fixture_contents_handle)

        print_visible_header_label('FINAL')
        print(final_info)

        print_visible_header_label('EXPECTED')
        print(expected)

        self.assertEqual(expected, final_info)

        return final_info
