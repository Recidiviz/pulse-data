# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests the classes in the metric_export_config file."""
import unittest
from collections import defaultdict
from unittest import mock
from unittest.mock import Mock

from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.export.products.product_configs import (
    PRODUCTS_CONFIG_PATH,
    BadProductExportSpecificationError,
    ProductConfig,
    ProductConfigs,
    ProductExportConfig,
    ProductStateConfig,
)
from recidiviz.pipelines.config_paths import PIPELINE_CONFIG_YAML_PATH
from recidiviz.tests.ingest import fixtures
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    DATA_PLATFORM_GCP_PROJECTS,
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.yaml_dict import YAMLDict


class TestProductConfig(unittest.TestCase):
    """Tests the functionality of the ProductConfig class."""

    def test_product_config_valid(self) -> None:
        _ = ProductConfig(
            name="Test Product",
            description="Test Product description",
            exports=["EXPORT", "OTHER_EXPORT"],
            states=[
                ProductStateConfig(state_code="US_XX", environment="production"),
                ProductStateConfig(state_code="US_WW", environment="staging"),
            ],
            environment=None,
            is_state_agnostic=False,
        )

    def test_product_config_invalid_environment(self) -> None:
        with self.assertRaises(ValueError):
            _ = ProductConfig(
                name="Test Product",
                description="Test Product description",
                exports=["EXPORT", "OTHER_EXPORT"],
                states=[
                    ProductStateConfig(state_code="US_XX", environment="production"),
                    ProductStateConfig(state_code="US_WW", environment="staging"),
                ],
                environment="production",
                is_state_agnostic=False,
            )

    def test_product_config_invalid_is_state_agnostic(self) -> None:
        with self.assertRaises(ValueError):
            _ = ProductConfig(
                name="Test Product",
                description="Test Product description",
                exports=["EXPORT", "OTHER_EXPORT"],
                states=[
                    ProductStateConfig(state_code="US_XX", environment="production"),
                    ProductStateConfig(state_code="US_WW", environment="staging"),
                ],
                environment="production",
                is_state_agnostic=True,
            )

        with self.assertRaises(ValueError):
            _ = ProductConfig(
                name="Test Product",
                description="Test Product description",
                exports=["EXPORT", "OTHER_EXPORT"],
                states=None,
                environment="production",
                is_state_agnostic=False,
            )


class TestProductConfigs(unittest.TestCase):
    """Tests the functionality of the ProductConfigs class."""

    def test_from_file(self) -> None:
        product_configs = ProductConfigs.from_file(
            path=fixtures.as_filepath("fixture_products.yaml", "../fixtures")
        )

        expected_product_configs = [
            ProductConfig(
                name="Test Product",
                description="Test Product description",
                exports=["EXPORT", "OTHER_EXPORT"],
                states=[
                    ProductStateConfig(state_code="US_XX", environment="production"),
                    ProductStateConfig(state_code="US_YY", environment="production"),
                    ProductStateConfig(state_code="US_WW", environment="staging"),
                ],
                environment=None,
                is_state_agnostic=False,
            ),
            ProductConfig(
                name="Test State Agnostic Product",
                description="Test State Agnostic Product description",
                exports=["MOCK_EXPORT_NAME"],
                states=None,
                environment="staging",
                is_state_agnostic=True,
            ),
            ProductConfig(
                name="Test Product Without Exports",
                description="Test Product Without Exports description",
                exports=[],
                states=[ProductStateConfig(state_code="US_XX", environment="staging")],
                environment=None,
                is_state_agnostic=False,
            ),
        ]

        self.assertEqual(expected_product_configs, product_configs.products)
        self.assertEqual(expected_product_configs, product_configs.products)

    def test_get_all_product_export_configs(self) -> None:
        product_configs = ProductConfigs.from_file(
            path=fixtures.as_filepath("fixture_products.yaml", "../fixtures")
        )
        export_configs = product_configs.get_all_export_configs()
        expected = [
            ProductExportConfig(export_job_name="EXPORT", state_code="US_XX"),
            ProductExportConfig(export_job_name="EXPORT", state_code="US_YY"),
            ProductExportConfig(export_job_name="EXPORT", state_code="US_WW"),
            ProductExportConfig(export_job_name="OTHER_EXPORT", state_code="US_XX"),
            ProductExportConfig(export_job_name="OTHER_EXPORT", state_code="US_YY"),
            ProductExportConfig(export_job_name="OTHER_EXPORT", state_code="US_WW"),
            ProductExportConfig(export_job_name="MOCK_EXPORT_NAME", state_code=None),
        ]
        self.assertEqual(expected, export_configs)

    def test_get_export_config_valid(self) -> None:
        product_configs = ProductConfigs.from_file(
            path=fixtures.as_filepath("fixture_products.yaml", "../fixtures")
        )
        _export_config = product_configs.get_export_config(
            export_job_name="EXPORT",
            state_code="US_XX",
        )

    def test_get_product_agnostic_export_configs(self) -> None:
        product_configs = ProductConfigs.from_file(
            path=fixtures.as_filepath("fixture_products.yaml", "../fixtures")
        )
        export_configs = product_configs.get_product_agnostic_export_configs()
        expected = [
            ProductExportConfig(export_job_name="MOCK_EXPORT_NAME", state_code=None),
        ]
        self.assertEqual(expected, export_configs)

    def test_get_export_config_missing_state_code(self) -> None:
        product_configs = ProductConfigs.from_file(
            path=fixtures.as_filepath("fixture_products.yaml", "../fixtures")
        )
        with self.assertRaisesRegex(
            BadProductExportSpecificationError,
            "Missing required state_code parameter for export_job_name EXPORT",
        ):
            product_configs.get_export_config(
                export_job_name="EXPORT",
            )

    def test_get_export_config_too_many_exports(self) -> None:
        product_configs = ProductConfigs.from_file(
            path=fixtures.as_filepath("fixture_products.yaml", "../fixtures")
        )
        product_configs.products.append(product_configs.products[0])
        with self.assertRaisesRegex(
            BadProductExportSpecificationError,
            "Wrong number of products returned for export for export_job_name EXPORT",
        ):
            product_configs.get_export_config(
                export_job_name="EXPORT",
            )

    @mock.patch("recidiviz.utils.metadata.project_id")
    def test_is_export_launched_in_env_is_launched(self, mock_project_id: Mock) -> None:
        mock_project_id.return_value = GCP_PROJECT_PRODUCTION
        product_configs = ProductConfigs.from_file(
            path=fixtures.as_filepath("fixture_products.yaml", "../fixtures")
        )
        export_config = product_configs.is_export_launched_in_env(
            export_job_name="EXPORT",
            state_code="US_XX",
        )
        self.assertTrue(export_config)

    @mock.patch("recidiviz.utils.metadata.project_id")
    def test_is_export_launched_in_env_not_launched(
        self,
        mock_project_id: Mock,
    ) -> None:
        mock_project_id.return_value = GCP_PROJECT_PRODUCTION
        product_configs = ProductConfigs.from_file(
            path=fixtures.as_filepath("fixture_products.yaml", "../fixtures")
        )
        export_config = product_configs.is_export_launched_in_env(
            export_job_name="EXPORT",
            state_code="US_WW",
        )
        self.assertFalse(export_config)

    def test_get_states_with_export_enabled_in_any_env(self) -> None:
        product_configs = ProductConfigs.from_file(
            path=fixtures.as_filepath("fixture_products.yaml", "../fixtures")
        )
        states = product_configs.get_states_with_export_enabled_in_any_env("EXPORT")
        expected = {"US_WW", "US_YY", "US_XX"}
        self.assertEqual(expected, states)


class TestRealProductConfigs(unittest.TestCase):
    """Tests for the products.yaml configuration."""

    def _get_enabled_metric_pipelines_by_state(self) -> dict[str, set[str]]:
        pipeline_configs = YAMLDict.from_path(PIPELINE_CONFIG_YAML_PATH).pop_dicts(
            "metric_pipelines"
        )

        pipeline_params_by_state: dict[str, set[str]] = defaultdict(set)
        for pipeline_config in pipeline_configs:
            if (
                metadata.project_id() == GCP_PROJECT_STAGING
                or not pipeline_config.peek_optional("staging_only", bool)
            ):
                state_code = pipeline_config.peek("state_code", str)
                pipeline_params_by_state[state_code].add(
                    pipeline_config.peek("pipeline", str)
                )
        return pipeline_params_by_state

    def test_exports_enabled_only_for_states_with_metric_pipelines_enabled(
        self,
    ) -> None:
        """Ensures that all minimum required metric pipelines are running before any
        product metric export is enabled for a given state.
        """

        # TODO(#42928): MA JII pilot uses highly constrained raw data. Once we can ingest
        # incarceration periods we can enable required pipelines and remove this exemption
        state_code_exemptions = [StateCode.US_MA]

        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            with local_project_id_override(project_id):
                product_configs = ProductConfigs.from_file(path=PRODUCTS_CONFIG_PATH)
                enabled_metric_pipelines_by_state = (
                    self._get_enabled_metric_pipelines_by_state()
                )

                for state_code in StateCode:
                    if state_code in state_code_exemptions:
                        continue

                    state_code_export_configs = [
                        c
                        for c in product_configs.get_export_configs_for_job_filter(
                            state_code.value
                        )
                        if product_configs.is_export_launched_in_env(**c)
                    ]

                    if not state_code_export_configs:
                        continue

                    enabled_metric_pipelines = enabled_metric_pipelines_by_state[
                        state_code.value
                    ]

                    required_metric_pipelines = {"population_span_metrics"}

                    if missing := (
                        required_metric_pipelines - enabled_metric_pipelines
                    ):
                        raise ValueError(
                            f"Cannot enable product exports for state "
                            f"[{state_code.value}] until all required metric pipelines "
                            f"have been enabled for this state in "
                            f"calculation_pipeline_templates.yaml. Missing required "
                            f"pipelines: {missing}"
                        )
