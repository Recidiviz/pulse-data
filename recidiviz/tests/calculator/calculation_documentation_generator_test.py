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
import unittest

from mock import patch, MagicMock

import recidiviz
from recidiviz.calculator.calculation_documentation_generator import (
    CalculationDocumentationGenerator,
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
from recidiviz.tests.metrics.export import fixtures

FIXTURE_PRODUCTS_DIR = os.path.relpath(
    os.path.dirname(fixtures.__file__), os.path.dirname(recidiviz.__file__)
)


class CalculationDocumentationGeneratorTest(unittest.TestCase):
    """Tests for DirectIngestDocumentationGenerator."""

    def setUp(self) -> None:
        self.docs_generator = CalculationDocumentationGenerator()

    @patch(
        "recidiviz.calculator.calculation_documentation_generator."
        "CalculationDocumentationGenerator._get_dataflow_pipeline_enabled_states"
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
        expected_states_str = """- #### STATES (links to come)
  - Missouri
  - Pennsylvania\n"""
        expected_products_str = """\n- #### PRODUCTS (links to come)
  - Test Product
  - Test Product Without Exports
  - Test State Agnostic Product\n"""
        expected_metrics_str = """\n- #### DATAFLOW METRICS (links to come)
  - ##### INCARCERATION
    - IncarcerationAdmissionMetric
  - ##### PROGRAM
    - ProgramReferralMetric
  - ##### RECIDIVISM
    - ReincarcerationRecidivismCountMetric
  - ##### SUPERVISION
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
