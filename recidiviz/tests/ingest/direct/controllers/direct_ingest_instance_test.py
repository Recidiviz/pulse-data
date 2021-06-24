# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for the DirectIngestInstance."""
import unittest
from unittest.mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    gcsfs_direct_ingest_bucket_for_region,
)
from recidiviz.ingest.direct.errors import DirectIngestInstanceError
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SQLAlchemyStateDatabaseVersion,
)


class TestDirectIngestInstance(unittest.TestCase):
    """Tests for the DirectIngestInstance."""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-staging"

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def test_state_get_database_version_state(self) -> None:
        expected_versions = {
            DirectIngestInstance.PRIMARY: SQLAlchemyStateDatabaseVersion.LEGACY,
            DirectIngestInstance.SECONDARY: SQLAlchemyStateDatabaseVersion.SECONDARY,
        }

        versions = {
            instance: instance.database_version(
                system_level=SystemLevel.STATE, state_code=StateCode.US_XX
            )
            for instance in DirectIngestInstance
        }

        self.assertEqual(expected_versions, versions)

    def test_state_get_database_version_county(self) -> None:
        self.assertEqual(
            SQLAlchemyStateDatabaseVersion.LEGACY,
            DirectIngestInstance.PRIMARY.database_version(
                system_level=SystemLevel.COUNTY, state_code=None
            ),
        )

        with self.assertRaises(DirectIngestInstanceError) as e:
            _ = DirectIngestInstance.SECONDARY.database_version(
                system_level=SystemLevel.COUNTY, state_code=None
            )
        self.assertEqual(
            str(e.exception),
            "Direct ingest for [SystemLevel.COUNTY] only has single, primary ingest "
            "instance. Ingest instance [DirectIngestInstance.SECONDARY] not valid.",
        )

    def test_from_state_ingest_bucket(self) -> None:
        ingest_bucket_path = gcsfs_direct_ingest_bucket_for_region(
            region_code="us_xx",
            system_level=SystemLevel.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
            project_id="recidiviz-456",
        )

        self.assertEqual(
            DirectIngestInstance.PRIMARY,
            DirectIngestInstance.for_ingest_bucket(ingest_bucket_path),
        )

        ingest_bucket_path = gcsfs_direct_ingest_bucket_for_region(
            region_code="us_xx",
            system_level=SystemLevel.STATE,
            ingest_instance=DirectIngestInstance.SECONDARY,
            project_id="recidiviz-456",
        )

        self.assertEqual(
            DirectIngestInstance.SECONDARY,
            DirectIngestInstance.for_ingest_bucket(ingest_bucket_path),
        )

    def test_from_county_ingest_bucket(self) -> None:
        ingest_bucket_path = gcsfs_direct_ingest_bucket_for_region(
            region_code="us_xx_yyyyy",
            system_level=SystemLevel.COUNTY,
            ingest_instance=DirectIngestInstance.PRIMARY,
            project_id="recidiviz-456",
        )

        self.assertEqual(
            DirectIngestInstance.PRIMARY,
            DirectIngestInstance.for_ingest_bucket(ingest_bucket_path),
        )

    def test_check_is_valid_system_level(self) -> None:
        for system_level in SystemLevel:
            # Shouldn't crash for any system level
            DirectIngestInstance.PRIMARY.check_is_valid_system_level(system_level)

        DirectIngestInstance.SECONDARY.check_is_valid_system_level(SystemLevel.STATE)
        with self.assertRaises(DirectIngestInstanceError) as e:
            DirectIngestInstance.SECONDARY.check_is_valid_system_level(
                system_level=SystemLevel.COUNTY
            )
        self.assertEqual(
            str(e.exception),
            "Direct ingest for [SystemLevel.COUNTY] only has single, primary ingest "
            "instance. Ingest instance [DirectIngestInstance.SECONDARY] not valid.",
        )
