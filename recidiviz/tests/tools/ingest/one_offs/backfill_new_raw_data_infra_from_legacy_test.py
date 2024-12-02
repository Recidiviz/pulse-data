# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for BackfillNewRawDataInfraFromLegacy"""
import os
import unittest
from typing import Optional
from unittest.mock import patch

import pytest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager_v2 import (
    DirectIngestRawFileMetadataManagerV2,
)
from recidiviz.ingest.direct.metadata.legacy_direct_ingest_raw_file_metadata_manager import (
    LegacyDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.ingest.one_offs.backfill_new_raw_data_infra_from_legacy import (
    BackfillNewRawDataInfraFromLegacy,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.utils.fixture_helpers import reset_fixtures

a = "recidiviz/tools/admin_panel/fixtures/operations_db/direct_ingest_raw_file_metadata.csv"


@pytest.mark.uses_db
class BackfillNewRawDataInfraFromLegacyTest(unittest.TestCase):
    """Tests for BackfillNewRawDataInfraFromLegacy."""

    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)
        self.is_enabled_patcher = patch(
            "recidiviz.tools.ingest.one_offs.backfill_new_raw_data_infra_from_legacy.is_raw_data_import_dag_enabled",
            return_value=True,
        )
        self.is_enabled_patcher.start()
        self.prompt_for_confirmation_patcher = patch(
            "recidiviz.tools.ingest.one_offs.backfill_new_raw_data_infra_from_legacy.prompt_for_confirmation",
            return_value=None,
        )
        self.prompt_for_confirmation_patcher.start()

    def tearDown(self) -> None:
        self.prompt_for_confirmation_patcher.stop()
        self.is_enabled_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_valid(self) -> None:
        reset_fixtures(
            engine=SQLAlchemyEngineManager.get_engine_for_database(
                database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
            ),
            tables=[schema.DirectIngestRawFileMetadata],
            fixture_directory=os.path.join(
                os.path.dirname(__file__),
                "../../../../..",
                "recidiviz/tools/admin_panel/fixtures/operations_db",
            ),
            csv_headers=True,
        )

        manager = BackfillNewRawDataInfraFromLegacy(
            state_code=StateCode.US_CO,
            raw_data_instance=DirectIngestInstance.PRIMARY,
            project_id="recidiviz-testing",
            with_proxy=False,  # let's us connect to local db
        )

        manager.backfill(dry_run=False)

        v2 = DirectIngestRawFileMetadataManagerV2(
            StateCode.US_CO.value, DirectIngestInstance.PRIMARY
        )
        legacy = LegacyDirectIngestRawFileMetadataManager(
            StateCode.US_CO.value, DirectIngestInstance.PRIMARY
        )

        legacy_non_invalidated = legacy.get_non_invalidated_files()

        new_non_invalidated = v2.get_non_invalidated_raw_big_query_files()

        assert len(legacy_non_invalidated) == len(new_non_invalidated)

        sorted_legacy_non_invalidated = sorted(
            legacy_non_invalidated, key=lambda x: x.file_id
        )

        sorted_new_non_invalidated = sorted(
            new_non_invalidated, key=lambda x: x.file_id
        )

        for i in range(len(legacy_non_invalidated)):
            legacy_metadata = sorted_legacy_non_invalidated[i]
            new_metadata = sorted_new_non_invalidated[i]

            assert legacy_metadata.file_id == new_metadata.file_id
            assert legacy_metadata.region_code == new_metadata.region_code
            assert legacy_metadata.raw_data_instance == new_metadata.raw_data_instance
            assert legacy_metadata.is_invalidated == new_metadata.is_invalidated
            assert legacy_metadata.file_tag == new_metadata.file_tag
            assert (
                legacy_metadata.file_processed_time == new_metadata.file_processed_time
            )

            gcs_files = v2.get_raw_gcs_file_metadata_by_file_id(legacy_metadata.file_id)

            assert len(gcs_files) == 1
            gcs_file = gcs_files[0]

            assert legacy_metadata.file_id == gcs_file.file_id
            assert legacy_metadata.region_code == gcs_file.region_code
            assert legacy_metadata.raw_data_instance == gcs_file.raw_data_instance
            assert legacy_metadata.is_invalidated == gcs_file.is_invalidated
            assert legacy_metadata.file_tag == gcs_file.file_tag
            assert legacy_metadata.file_discovery_time == gcs_file.file_discovery_time
            assert legacy_metadata.normalized_file_name == gcs_file.normalized_file_name

    def test_conflicts(self) -> None:
        reset_fixtures(
            engine=SQLAlchemyEngineManager.get_engine_for_database(
                database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
            ),
            tables=[
                schema.DirectIngestRawFileMetadata,
                schema.DirectIngestRawBigQueryFileMetadata,
            ],
            fixture_directory=os.path.join(
                os.path.dirname(__file__),
                "../../../../..",
                "recidiviz/tools/admin_panel/fixtures/operations_db",
            ),
            csv_headers=True,
        )

        manager = BackfillNewRawDataInfraFromLegacy(
            state_code=StateCode.US_CO,
            raw_data_instance=DirectIngestInstance.PRIMARY,
            project_id="recidiviz-testing",
            with_proxy=False,  # let's us connect to local db
        )

        with self.assertRaisesRegex(ValueError, r"Found \[\d+\] conflicting ids"):
            manager.backfill(dry_run=False)
