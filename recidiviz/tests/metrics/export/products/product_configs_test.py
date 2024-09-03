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

# TODO(#11034): Add a test to make sure products launched in production have the required calc metrics enabled
# in production.
from unittest import mock
from unittest.mock import Mock

from recidiviz.metrics.export.products.product_configs import (
    BadProductExportSpecificationError,
    ProductConfig,
    ProductConfigs,
    ProductExportConfig,
    ProductStateConfig,
)
from recidiviz.tests.ingest import fixtures
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION


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
