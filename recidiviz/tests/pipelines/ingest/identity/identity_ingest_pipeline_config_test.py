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
from unittest.mock import patch

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    _DEFAULT_MAX_IDS_PER_TYPE,
    ConflictCheckedAttribute,
    ConflictCheckedAttributeOverrides,
    IdentityIngestPipelineConfig,
    IdentityIngestPipelineTenantConfig,
    OptionalConflictCheckedAttribute,
    ResolutionStrategy,
    ResolutionStrategyOverrides,
)
from recidiviz.tests.ingest import fixtures
from recidiviz.utils.yaml_dict import YAMLDict


def _load_config_from_yaml(yaml_text: str) -> IdentityIngestPipelineConfig:
    """Loads an identity config from a single US_XX identity_config.yaml with
    the given contents."""
    with tempfile.TemporaryDirectory() as regions_dir, patch(
        "recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config."
        "get_direct_ingest_states_existing_in_env",
        return_value=[StateCode.US_XX],
    ):
        state_dir = os.path.join(regions_dir, "us_xx")
        os.makedirs(state_dir)
        with open(
            os.path.join(state_dir, "identity_config.yaml"), "w", encoding="utf-8"
        ) as f:
            f.write(yaml_text)
        return IdentityIngestPipelineConfig.load_clustering_config(
            regions_dir=regions_dir
        )


def _tenant_config_from_fixture(
    fixture_filename: str,
) -> IdentityIngestPipelineTenantConfig:
    """Parses a person-type config block from the given fixture YAML file."""
    return IdentityIngestPipelineTenantConfig.from_yaml_dict(
        YAMLDict.from_path(fixtures.as_filepath(fixture_filename))
    )


class ConflictCheckedAttributeEnumTest(unittest.TestCase):
    """Tests for the conflict-checked attribute enums."""

    def test_optional_attributes_are_a_subset_of_conflict_checked(self) -> None:
        self.assertLessEqual(
            {a.value for a in OptionalConflictCheckedAttribute},
            {a.value for a in ConflictCheckedAttribute},
        )


class IdentityIngestPipelineTenantConfigTest(unittest.TestCase):
    """Tests for IdentityIngestPipelineTenantConfig construction and effective
    config."""

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

    def test_default_conflict_checked_attributes_config(self) -> None:
        config = IdentityIngestPipelineTenantConfig()
        self.assertTrue(
            config.conflict_checked_attributes_config[
                OptionalConflictCheckedAttribute.SEX
            ]
        )
        self.assertFalse(
            config.conflict_checked_attributes_config[
                OptionalConflictCheckedAttribute.GENDER
            ]
        )
        self.assertFalse(
            config.conflict_checked_attributes_config[
                OptionalConflictCheckedAttribute.ETHNICITY
            ]
        )

    def test_default_resolution_strategy_config(self) -> None:
        config = IdentityIngestPipelineTenantConfig()
        self.assertEqual(
            config.resolution_strategy_config[ConflictCheckedAttribute.SURNAME],
            ResolutionStrategy.KEEP_LATEST,
        )
        self.assertEqual(
            config.resolution_strategy_config[ConflictCheckedAttribute.BIRTHDATE],
            ResolutionStrategy.KEEP_LATEST,
        )
        self.assertEqual(
            config.resolution_strategy_config[ConflictCheckedAttribute.SEX],
            ResolutionStrategy.SET_NULL,
        )

    def test_overrides_overlay_defaults_in_effective_config(self) -> None:
        config = IdentityIngestPipelineTenantConfig(
            conflict_check_overrides=ConflictCheckedAttributeOverrides(
                overrides={OptionalConflictCheckedAttribute.SEX: False}
            ),
            resolution_strategy_overrides=ResolutionStrategyOverrides(
                overrides={
                    ConflictCheckedAttribute.GENDER: ResolutionStrategy.KEEP_LATEST
                }
            ),
        )
        # Overridden attributes take the override.
        self.assertFalse(
            config.conflict_checked_attributes_config[
                OptionalConflictCheckedAttribute.SEX
            ]
        )
        self.assertEqual(
            config.resolution_strategy_config[ConflictCheckedAttribute.GENDER],
            ResolutionStrategy.KEEP_LATEST,
        )
        # Unoverridden attributes keep their defaults.
        self.assertFalse(
            config.conflict_checked_attributes_config[
                OptionalConflictCheckedAttribute.GENDER
            ]
        )
        self.assertEqual(
            config.resolution_strategy_config[ConflictCheckedAttribute.SURNAME],
            ResolutionStrategy.KEEP_LATEST,
        )


class IdentityIngestPipelineTenantConfigFromYamlTest(unittest.TestCase):
    """Tests for parsing a person-type config block from YAML."""

    def test_parses_full_config(self) -> None:
        self.assertEqual(
            IdentityIngestPipelineTenantConfig(
                max_ids_per_type_overrides={"US_XX_INMATE_NUM": 50, "US_XX_SID": 3},
                conflict_check_overrides=ConflictCheckedAttributeOverrides(
                    overrides={
                        OptionalConflictCheckedAttribute.SEX: False,
                        OptionalConflictCheckedAttribute.GENDER: True,
                    }
                ),
                resolution_strategy_overrides=ResolutionStrategyOverrides(
                    overrides={
                        ConflictCheckedAttribute.BIRTHDATE: ResolutionStrategy.SET_NULL,
                        ConflictCheckedAttribute.GENDER: ResolutionStrategy.KEEP_LATEST,
                    }
                ),
            ),
            _tenant_config_from_fixture("identity_tenant_config_full.yaml"),
        )

    def test_full_config_effective_values_overlay_defaults(self) -> None:
        config = _tenant_config_from_fixture("identity_tenant_config_full.yaml")
        self.assertFalse(
            config.conflict_checked_attributes_config[
                OptionalConflictCheckedAttribute.SEX
            ]
        )
        self.assertTrue(
            config.conflict_checked_attributes_config[
                OptionalConflictCheckedAttribute.GENDER
            ]
        )
        # ethnicity is not in the fixture, so it keeps its default.
        self.assertFalse(
            config.conflict_checked_attributes_config[
                OptionalConflictCheckedAttribute.ETHNICITY
            ]
        )
        self.assertEqual(
            config.resolution_strategy_config[ConflictCheckedAttribute.BIRTHDATE],
            ResolutionStrategy.SET_NULL,
        )
        # surname is not in the fixture, so it keeps its default.
        self.assertEqual(
            config.resolution_strategy_config[ConflictCheckedAttribute.SURNAME],
            ResolutionStrategy.KEEP_LATEST,
        )

    def test_conflict_check_unconfigurable_attribute_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Unexpected conflict_check_overrides attribute \[surname\]"
        ):
            _tenant_config_from_fixture(
                "identity_tenant_config_conflict_check_unconfigurable.yaml"
            )

    def test_conflict_check_non_bool_value_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"The field \[sex\] must be of type"):
            _tenant_config_from_fixture(
                "identity_tenant_config_conflict_check_non_bool.yaml"
            )

    def test_resolution_unconfigurable_attribute_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Unexpected resolution_strategy_overrides attribute \[preferred_name\]",
        ):
            _tenant_config_from_fixture(
                "identity_tenant_config_resolution_unconfigurable.yaml"
            )

    def test_resolution_invalid_strategy_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Invalid resolution \[bogus\] for \[sex\]"
        ):
            _tenant_config_from_fixture(
                "identity_tenant_config_resolution_invalid_strategy.yaml"
            )

    def test_resolution_dangling_null_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"The field \[birthdate\] must be of type"
        ):
            _tenant_config_from_fixture(
                "identity_tenant_config_resolution_dangling_null.yaml"
            )


class IdentityIngestPipelineConfigTest(unittest.TestCase):
    """Tests for IdentityIngestPipelineConfig."""

    def setUp(self) -> None:
        self.config = IdentityIngestPipelineConfig.load_clustering_config()

    def test_load_clustering_config_default_config_has_no_overrides(self) -> None:
        self.assertEqual(self.config.default_config.max_ids_per_type_overrides, {})

    def test_load_clustering_config_loads_overrides_from_state_config(self) -> None:
        config = _load_config_from_yaml(
            "jii:\n"
            "  max_ids_per_type_overrides:\n"
            "    US_XX_INMATE_NUM: 50\n"
            "    US_XX_SID: 3\n"
            "  conflict_check_overrides:\n"
            "    sex: false\n"
        )
        jii_config = config.get_tenant_clustering_config(Tenant.US_XX, PersonType.JII)
        self.assertEqual(jii_config.get_max_ids_for_type("US_XX_INMATE_NUM"), 50)
        self.assertEqual(jii_config.get_max_ids_for_type("US_XX_SID"), 3)
        self.assertEqual(
            jii_config.get_max_ids_for_type("US_XX_OTHER"), _DEFAULT_MAX_IDS_PER_TYPE
        )
        self.assertFalse(
            jii_config.conflict_checked_attributes_config[
                OptionalConflictCheckedAttribute.SEX
            ]
        )
        # staff is not configured in the temp YAML, so it falls back to default.
        staff_config = config.get_tenant_clustering_config(
            Tenant.US_XX, PersonType.STAFF
        )
        self.assertEqual(staff_config, config.default_config)

    def test_load_clustering_config_unexpected_person_type_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Unable to parse identity config for \[US_XX\] \[JII\]: Found "
            r"unexpected config values",
        ):
            _load_config_from_yaml("jii:\n  not_a_real_key: 5\n")

    def test_load_clustering_config_unexpected_top_level_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found unexpected top-level config values for identity config "
            r"\[US_XX\]",
        ):
            _load_config_from_yaml(
                "jii:\n  max_ids_per_type_overrides: {}\nnot_a_person_type: {}\n"
            )

    def test_get_tenant_clustering_config_us_oz_jii(self) -> None:
        config = self.config.get_tenant_clustering_config(Tenant.US_OZ, PersonType.JII)
        self.assertEqual(config.max_ids_per_type_overrides, {})

    def test_get_tenant_clustering_config_returns_default_for_unconfigured_tenant(
        self,
    ) -> None:
        config = self.config.get_tenant_clustering_config(Tenant.US_XX, PersonType.JII)
        self.assertEqual(config, self.config.default_config)

    def test_get_tenant_clustering_config_returns_default_for_unconfigured_person_type(
        self,
    ) -> None:
        config = self.config.get_tenant_clustering_config(
            Tenant.US_OZ, PersonType.RECIDIVIZ_EMPLOYEE
        )
        self.assertEqual(config, self.config.default_config)
