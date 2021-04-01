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

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    gcsfs_direct_ingest_bucket_for_region,
)
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SQLAlchemyStateDatabaseVersion,
)


class TestDirectIngestInstance(unittest.TestCase):
    """Tests for the DirectIngestInstance."""

    def test_state_get_database_version_state(self) -> None:
        expected_versions = {
            DirectIngestInstance.PRIMARY: SQLAlchemyStateDatabaseVersion.LEGACY,
            DirectIngestInstance.SECONDARY: SQLAlchemyStateDatabaseVersion.SECONDARY,
        }

        versions = {
            instance: instance.database_version(system_level=SystemLevel.STATE)
            for instance in DirectIngestInstance
        }

        self.assertEqual(expected_versions, versions)

    def test_state_get_database_version_county(self) -> None:
        self.assertEqual(
            SQLAlchemyStateDatabaseVersion.LEGACY,
            DirectIngestInstance.PRIMARY.database_version(
                system_level=SystemLevel.COUNTY
            ),
        )

        with self.assertRaises(ValueError) as e:
            _ = DirectIngestInstance.SECONDARY.database_version(
                system_level=SystemLevel.COUNTY
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

        # TODO(#6077): Add test for mapping secondary bucket to ingest instance once
        #  that is supported.

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
