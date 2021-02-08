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
import datetime
import os
import unittest
from typing import Type, Optional

import pytest
from freezegun import freeze_time
from mock import patch
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz import IngestInfo
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsIngestViewExportArgs
from recidiviz.persistence.database.base_schema import OperationsBase
from recidiviz.persistence.database.schema.operations import schema as operations_schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    build_gcsfs_controller_for_tests, ingest_args_for_fixture_file
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import fixture_util
from recidiviz.tests.utils.test_utils import print_visible_header_label
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
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

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.maxDiff = 250000

        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = 'recidiviz-staging'

        local_postgres_helpers.use_on_disk_postgresql_database(self.schema_base())
        local_postgres_helpers.use_on_disk_postgresql_database(OperationsBase)

        self.controller = build_gcsfs_controller_for_tests(
            self.controller_cls(),
            self.fixture_path_prefix(),
            run_async=False,
            max_delay_sec_between_files=0,
            regions_module=regions
        )

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(OperationsBase)
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.schema_base())
        self.metadata_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(cls.temp_db_dir)

    @classmethod
    def fixture_path_prefix(cls) -> str:
        return os.path.join('direct', 'regions', cls.region_code().lower())

    def run_parse_file_test(self,
                            expected: IngestInfo,
                            fixture_file_name: str) -> IngestInfo:
        """Runs a test that reads and parses a given fixture file. Returns the
        parsed IngestInfo object for tests to run further validations."""
        args = ingest_args_for_fixture_file(self.controller,
                                            f'{fixture_file_name}.csv')

        if not isinstance(self.controller.fs.gcs_file_system, FakeGCSFileSystem):
            raise ValueError(f"Controller fs must have type "
                             f"FakeGCSFileSystem. Found instead "
                             f"type [{type(self.controller.fs.gcs_file_system)}]")

        if self.controller.region.are_ingest_view_exports_enabled_in_env():
            now = datetime.datetime.now()
            yesterday = now - datetime.timedelta(days=1)
            ingest_file_export_job_args = GcsfsIngestViewExportArgs(
                ingest_view_name=fixture_file_name,
                upper_bound_datetime_to_export=now,
                upper_bound_datetime_prev=yesterday,
            )

            self.controller.file_metadata_manager.register_ingest_file_export_job(ingest_file_export_job_args)
            self.controller.ingest_view_export_manager.export_view_for_args(ingest_file_export_job_args)
        else:
            fixture_util.add_direct_ingest_path(self.controller.fs.gcs_file_system, args.file_path)

        # pylint:disable=protected-access
        fixture_contents_handle = self.controller._get_contents_handle(args)

        if fixture_contents_handle is None:
            self.fail('fixture_contents_handle should not be None')
        final_info = self.controller._parse(args, fixture_contents_handle)

        print_visible_header_label('FINAL')
        print(final_info)

        print_visible_header_label('EXPECTED')
        print(expected)

        self.assertEqual(expected, final_info)

        return final_info

    @staticmethod
    def invalidate_ingest_view_metadata() -> None:
        session = SessionFactory.for_schema_base(OperationsBase)
        try:
            session.query(operations_schema.DirectIngestIngestFileMetadata) \
                .update({operations_schema.DirectIngestIngestFileMetadata.is_invalidated: True})
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
