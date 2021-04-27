# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for CalculationDocumentationGenerator."""
import os
import tempfile
import unittest
from filecmp import dircmp

from mock import patch, MagicMock

import recidiviz
from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import DagKey
from recidiviz.calculator.calculation_documentation_generator import (
    CalculationDocumentationGenerator,
    CALC_DOCS_PATH,
)
from recidiviz.calculator.pipeline.incarceration.metrics import (
    IncarcerationAdmissionMetric,
)
from recidiviz.calculator.pipeline.program.metrics import ProgramReferralMetric
from recidiviz.calculator.pipeline.recidivism.metrics import (
    ReincarcerationRecidivismCountMetric,
)
from recidiviz.calculator.pipeline.supervision.metrics import (
    SupervisionCaseComplianceMetric,
    SupervisionRevocationMetric,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.export.export_config import ProductConfig, PRODUCTS_CONFIG_PATH
from recidiviz.tests.metrics.export import fixtures
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FIXTURE_PRODUCTS_DIR = os.path.relpath(
    os.path.dirname(fixtures.__file__), os.path.dirname(recidiviz.__file__)
)


class CalculationDocumentationGeneratorTest(unittest.TestCase):
    """Tests for DirectIngestDocumentationGenerator."""

    def setUp(self) -> None:
        self.mock_config_path = os.path.join(
            os.path.dirname(fixtures.__file__), "fixture_products.yaml"
        )
        self.product_configs = ProductConfig.product_configs_from_file(
            self.mock_config_path
        )
        with local_project_id_override(GCP_PROJECT_STAGING), patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True
            self.docs_generator = CalculationDocumentationGenerator(
                self.product_configs
            )

    @patch(
        "recidiviz.calculator.calculation_documentation_generator."
        "CalculationDocumentationGenerator._get_dataflow_pipeline_enabled_states",
    )
    @patch(
        "recidiviz.calculator.calculation_documentation_generator."
        "CalculationDocumentationGenerator._get_product_enabled_states"
    )
    @patch(
        "recidiviz.calculator.calculation_documentation_generator.PRODUCTS_CONFIG_PATH",
        os.path.join(os.path.dirname(fixtures.__file__), "fixture_products.yaml"),
    )
    @patch(
        "recidiviz.calculator.calculation_documentation_generator.DATAFLOW_METRICS_TO_TABLES",
        {
            # IncarcerationMetrics
            IncarcerationAdmissionMetric: "test_incarceration_admission_metrics",
            # ProgramMetrics
            ProgramReferralMetric: "test_program_referral_metrics",
            # ReincarcerationRecidivismMetrics
            ReincarcerationRecidivismCountMetric: "test_recidivism_count_metrics",
            # SupervisionMetrics
            SupervisionCaseComplianceMetric: "test_supervision_case_compliance_metrics",
            SupervisionRevocationMetric: "test_supervision_revocation_metrics",
        },
    )
    def test_generate_summary_strings(
        self,
        mock_product_states: MagicMock,
        mock_pipeline_states: MagicMock,
    ) -> None:
        mock_pipeline_states.return_value = {StateCode("US_MO")}
        mock_product_states.return_value = {StateCode("US_MO"), StateCode("US_PA")}

        summary_strings = self.docs_generator.generate_summary_strings()

        expected_header_str = "## Calculation Catalog\n\n"
        expected_states_str = """- States (links to come)
  - Missouri
  - Pennsylvania"""
        expected_products_str = """\n- Products
  - [Test Product](calculation/products/test_product/test_product_summary.md)
  - [Test Product Without Exports](calculation/products/test_product_without_exports/test_product_without_exports_summary.md)
  - [Test State Agnostic Product](calculation/products/test_state_agnostic_product/test_state_agnostic_product_summary.md)"""
        expected_metrics_str = """\n- Dataflow Metrics (links to come)
  - INCARCERATION
    - IncarcerationAdmissionMetric
  - PROGRAM
    - ProgramReferralMetric
  - RECIDIVISM
    - ReincarcerationRecidivismCountMetric
  - SUPERVISION
    - SupervisionCaseComplianceMetric
    - SupervisionRevocationMetric\n"""

        expected_summary_strings = [
            expected_header_str,
            expected_states_str,
            expected_products_str,
            expected_metrics_str,
        ]

        self.assertIsNotNone(summary_strings)
        self.assertEqual(expected_summary_strings, summary_strings)

    @patch(
        "recidiviz.calculator.calculation_documentation_generator."
        "CalculationDocumentationGenerator._get_all_parent_keys_for_product"
    )
    @patch(
        "recidiviz.calculator.calculation_documentation_generator."
        "CalculationDocumentationGenerator._get_states_to_metrics"
    )
    @patch(
        "recidiviz.calculator.calculation_documentation_generator."
        "CalculationDocumentationGenerator._get_dataflow_pipeline_enabled_states",
    )
    def test_get_product_information(
        self,
        mock_pipeline_states: MagicMock,
        mock_states_to_metrics: MagicMock,
        mock_view_keys: MagicMock,
    ) -> None:
        mock_pipeline_states.return_value = {StateCode("US_MO"), StateCode("US_PA")}
        mock_states_to_metrics.return_value = {
            "US_PA": ["PROGRAM_REFERRAL"],
            "US_MO": ["INCARCERATION_ADMISSION"],
        }

        mock_view_keys.return_value = {
            DagKey(
                view_address=BigQueryAddress(dataset_id="dataset_1", table_id="table_1")
            ),
            DagKey(
                view_address=BigQueryAddress(dataset_id="dataset_1", table_id="table_2")
            ),
            DagKey(
                view_address=BigQueryAddress(dataset_id="dataset_1", table_id="table_3")
            ),
            DagKey(
                view_address=BigQueryAddress(dataset_id="state", table_id="table_1")
            ),
            DagKey(
                view_address=BigQueryAddress(dataset_id="state", table_id="table_2")
            ),
            DagKey(
                view_address=BigQueryAddress(
                    dataset_id="dataflow_metrics",
                    table_id="incarceration_admission_metrics",
                )
            ),
        }

        for product in self.product_configs:
            if product.name == "Test Product":
                documentation = self.docs_generator._get_product_information(  # pylint: disable=W0212
                    product
                )
                expected_docs = """#TEST PRODUCT
##SHIPPED STATES
  - Test State

## STATES IN DEVELOPMENT
  - Test State

##VIEWS

####dataset_1
  - table_1
  - table_2
  - table_3

##SOURCE TABLES
_Reference views that are used by other views. Some need to be updated manually._

####state
_Ingested state data. This dataset is a copy of the state postgres database._
  - table_1
  - table_2

##METRICS
_All metrics required to support this product and whether or not each state regularly calculates the metric._

** DISCLAIMER **
The presence of all required metrics for a state does not guarantee that this product is ready to launch in that state.

|       **Metric**        | **US_MO** | **US_PA** |
|-------------------------|-----------|-----------|
| INCARCERATION_ADMISSION | X         |           |
"""
                self.assertEqual(expected_docs, documentation)

    # TODO(#7125): Improve calc docs generation speed
    @unittest.skipIf(
        os.environ.get("TRAVIS") == "true", "docs/ does not exist in Travis"
    )
    def test_generate_products_markdowns(self) -> None:
        products = ProductConfig.product_configs_from_file(PRODUCTS_CONFIG_PATH)
        with local_project_id_override(GCP_PROJECT_STAGING), patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True
            docs_generator = CalculationDocumentationGenerator(products=products)

        with tempfile.TemporaryDirectory() as tmpdirname:
            docs_generator.generate_products_markdowns(tmpdirname)
            dir_comparison = dircmp(
                tmpdirname,
                os.path.join(os.path.dirname(recidiviz.__file__), "..", CALC_DOCS_PATH),
            )
            self.assertEqual(len(dir_comparison.diff_files), 0)
