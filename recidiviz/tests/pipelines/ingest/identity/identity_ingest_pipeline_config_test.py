# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for IdentityIngestPipelineConfig."""
import os
import tempfile
import unittest

from recidiviz.common.constants.identity import PersonType
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    _DEFAULT_MAX_IDS_PER_TYPE,
    IdentityIngestPipelineConfig,
    IdentityIngestPipelineTenantConfig,
)


class IdentityIngestPipelineTenantConfigTest(unittest.TestCase):
    """Tests for IdentityIngestPipelineTenantConfig."""

    def test_get_max_ids_for_type_uses_override(self) -> None:
        config = IdentityIngestPipelineTenantConfig(
            max_ids_per_type_overrides={"US_OZ_INMATE_NUM": 50}
        )
        self.assertEqual(config.get_max_ids_for_type("US_OZ_INMATE_NUM"), 50)

    def test_get_max_ids_for_type_unlisted_type_returns_default(self) -> None:
        config = IdentityIngestPipelineTenantConfig(
            max_ids_per_type_overrides={"US_OZ_INMATE_NUM": 50}
        )
        self.assertEqual(
            config.get_max_ids_for_type("US_OZ_SID"), _DEFAULT_MAX_IDS_PER_TYPE
        )

    def test_get_max_ids_for_type_empty_overrides_returns_default(self) -> None:
        config = IdentityIngestPipelineTenantConfig()
        self.assertEqual(
            config.get_max_ids_for_type("US_OZ_INMATE_NUM"), _DEFAULT_MAX_IDS_PER_TYPE
        )


class IdentityIngestPipelineConfigTest(unittest.TestCase):
    """Tests for IdentityIngestPipelineConfig."""

    def setUp(self) -> None:
        self.config = IdentityIngestPipelineConfig.load_clustering_config()

    def test_load_clustering_config_default_config_has_no_overrides(self) -> None:
        self.assertEqual(self.config.default_config.max_ids_per_type_overrides, {})

    def test_load_clustering_config_loads_overrides_from_state_config(self) -> None:
        with tempfile.TemporaryDirectory() as regions_dir:
            state_dir = os.path.join(regions_dir, "us_xx")
            os.makedirs(state_dir)
            with open(
                os.path.join(state_dir, "identity_config.yaml"), "w", encoding="utf-8"
            ) as f:
                f.write(
                    "jii:\n"
                    "  max_ids_per_type_overrides:\n"
                    "    US_XX_INMATE_NUM: 50\n"
                    "    US_XX_SID: 3\n"
                )
            config = IdentityIngestPipelineConfig.load_clustering_config(
                regions_dir=regions_dir
            )

        jii_config = config.get_tenant_clustering_config("US_XX", PersonType.JII)
        self.assertEqual(jii_config.get_max_ids_for_type("US_XX_INMATE_NUM"), 50)
        self.assertEqual(jii_config.get_max_ids_for_type("US_XX_SID"), 3)
        self.assertEqual(
            jii_config.get_max_ids_for_type("US_XX_OTHER"), _DEFAULT_MAX_IDS_PER_TYPE
        )
        # staff is not configured in the temp YAML, so it should fall back to default
        staff_config = config.get_tenant_clustering_config("US_XX", PersonType.STAFF)
        self.assertEqual(staff_config, config.default_config)

    def test_get_tenant_clustering_config_us_oz_jii(self) -> None:
        config = self.config.get_tenant_clustering_config("US_OZ", PersonType.JII)
        self.assertEqual(config.max_ids_per_type_overrides, {})

    def test_get_tenant_clustering_config_us_oz_staff(self) -> None:
        config = self.config.get_tenant_clustering_config("US_OZ", PersonType.STAFF)
        self.assertEqual(config.max_ids_per_type_overrides, {})

    def test_get_tenant_clustering_config_returns_default_for_unconfigured_tenant(
        self,
    ) -> None:
        config = self.config.get_tenant_clustering_config("US_XX", PersonType.JII)
        self.assertEqual(config, self.config.default_config)

    def test_get_tenant_clustering_config_returns_default_for_unconfigured_person_type(
        self,
    ) -> None:
        config = self.config.get_tenant_clustering_config(
            "US_OZ", PersonType.RECIDIVIZ_EMPLOYEE
        )
        self.assertEqual(config, self.config.default_config)

    def test_get_tenant_clustering_config_tenant_is_case_insensitive(self) -> None:
        upper = self.config.get_tenant_clustering_config("US_OZ", PersonType.JII)
        lower = self.config.get_tenant_clustering_config("us_oz", PersonType.JII)
        self.assertEqual(upper, lower)
