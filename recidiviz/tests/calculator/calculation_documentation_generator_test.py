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
from recidiviz.big_query.big_query_view import BigQueryAddress, BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import DagKey
from recidiviz.calculator.calculation_documentation_generator import (
    CalculationDocumentationGenerator,
    CALC_DOCS_PATH,
    PipelineMetricInfo,
    StateMetricInfo,
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
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCPEnvironment
from recidiviz.utils.metadata import local_project_id_override

FIXTURE_PRODUCTS_DIR = os.path.relpath(
    os.path.dirname(fixtures.__file__), os.path.dirname(recidiviz.__file__)
)


class CalculationDocumentationGeneratorTest(unittest.TestCase):
    """Tests for DirectIngestDocumentationGenerator."""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"
        self.mock_config_path = os.path.join(
            os.path.dirname(fixtures.__file__), "fixture_products.yaml"
        )
        self.product_configs = ProductConfig.product_configs_from_file(
            self.mock_config_path
        )
        self.mock_product_states = {StateCode("US_MO")}
        self.mock_pipeline_states = {StateCode("US_MO"), StateCode("US_PA")}
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.state.source_table`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.state.source_table_2`",
        )
        view_3 = BigQueryView(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
                            SELECT * FROM `{project_id}.dataset_1.table_1`
                            JOIN `{project_id}.dataset_2.table_2`
                            USING (col)""",
        )
        view_4 = BigQueryView(
            dataset_id="dataset_4",
            view_id="table_4",
            description="table_4 description",
            view_query_template="""
                            SELECT * FROM `{project_id}.dataset_3.table_3`""",
        )
        view_5 = BigQueryView(
            dataset_id="dataset_5",
            view_id="table_5",
            description="table_5 description",
            view_query_template="""
                            SELECT * FROM `{project_id}.dataset_3.table_3`""",
        )
        with local_project_id_override(GCP_PROJECT_STAGING), patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists, patch.object(
            CalculationDocumentationGenerator,
            "_get_all_export_config_view_builder_addresses",
        ) as mock_top_level_view_addresses, patch.object(
            CalculationDocumentationGenerator,
            "_get_all_views_to_document",
        ) as mock_all_views_to_document, patch(
            "recidiviz.calculator.calculation_documentation_generator._build_views_to_update"
        ) as mock_views:
            mock_views.return_value = [view_1, view_2, view_3, view_4, view_5]
            mock_top_level_view_addresses.return_value = {view_1.address}
            mock_all_views_to_document.return_value = {view_1.address}
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True
            self.docs_generator = CalculationDocumentationGenerator(
                self.product_configs, CALC_DOCS_PATH
            )
        self.docs_generator.metric_calculations_by_state = {
            "Pennsylvania": [
                PipelineMetricInfo(
                    name="PROGRAM_REFERRAL", month_count=36, frequency="daily"
                )
            ],
            "Missouri": [
                PipelineMetricInfo(
                    name="INCARCERATION_ADMISSION", month_count=24, frequency="daily"
                ),
                PipelineMetricInfo(
                    name="INCARCERATION_ADMISSION",
                    month_count=240,
                    frequency="triggered by code changes",
                ),
            ],
        }

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

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
        mock_pipeline_states.return_value = self.mock_pipeline_states
        mock_product_states.return_value = self.mock_product_states

        self.docs_generator.metrics_by_generic_types = {
            "Incarceration": [IncarcerationAdmissionMetric],
            "Program": [ProgramReferralMetric],
            "Recidivism": [ReincarcerationRecidivismCountMetric],
            "Supervision": [
                SupervisionCaseComplianceMetric,
                SupervisionRevocationMetric,
            ],
        }

        summary_strings = self.docs_generator.generate_summary_strings()

        expected_header_str = "## Calculation Catalog\n\n"
        expected_states_str = """- States
  - [Missouri](calculation/states/missouri.md)
  - [Pennsylvania](calculation/states/pennsylvania.md)"""
        expected_products_str = """\n- Products
  - [Test Product](calculation/products/test_product/test_product_summary.md)
  - [Test Product Without Exports](calculation/products/test_product_without_exports/test_product_without_exports_summary.md)
  - [Test State Agnostic Product](calculation/products/test_state_agnostic_product/test_state_agnostic_product_summary.md)"""
        expected_views_str = """\n- Views
  - dataset_1
    - [table_1](calculation/views/dataset_1/table_1.md)
"""
        expected_metrics_str = """\n- Dataflow Metrics
  - INCARCERATION
    - [IncarcerationAdmissionMetric](calculation/metrics/incarceration/test_incarceration_admission_metrics.md)
  - PROGRAM
    - [ProgramReferralMetric](calculation/metrics/program/test_program_referral_metrics.md)
  - RECIDIVISM
    - [ReincarcerationRecidivismCountMetric](calculation/metrics/recidivism/test_recidivism_count_metrics.md)
  - SUPERVISION
    - [SupervisionCaseComplianceMetric](calculation/metrics/supervision/test_supervision_case_compliance_metrics.md)
    - [SupervisionRevocationMetric](calculation/metrics/supervision/test_supervision_revocation_metrics.md)\n"""

        expected_summary_strings = [
            expected_header_str,
            expected_states_str,
            expected_products_str,
            expected_views_str,
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
        "CalculationDocumentationGenerator._get_dataflow_pipeline_enabled_states",
    )
    def test_get_product_information(
        self,
        mock_pipeline_states: MagicMock,
        mock_view_keys: MagicMock,
    ) -> None:
        mock_pipeline_states.return_value = self.mock_pipeline_states

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
  - [Test State](../../states/test_state.md)

## STATES IN DEVELOPMENT
  - [Test State](../../states/test_state.md)

##VIEWS

####dataset_1
  - [table_1](../../views/dataset_1/table_1.md) <br/>
  - [table_2](../../views/dataset_1/table_2.md) <br/>
  - [table_3](../../views/dataset_1/table_3.md) <br/>

##SOURCE TABLES
_Reference views that are used by other views. Some need to be updated manually._

####state
_Ingested state data. This dataset is a copy of the state postgres database._
  - table_1 ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=table_1)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=table_1)) <br/>
  - table_2 ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=table_2)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=table_2)) <br/>

##METRICS
_All metrics required to support this product and whether or not each state regularly calculates the metric._

** DISCLAIMER **
The presence of all required metrics for a state does not guarantee that this product is ready to launch in that state.

|                                       **Metric**                                        |**US_MO**|**US_PA**|
|-----------------------------------------------------------------------------------------|---------|---------|
|[INCARCERATION_ADMISSION](../../metrics/incarceration/incarceration_admission_metrics.md)|X        |         |
"""
                self.assertEqual(expected_docs, documentation)

    @patch(
        "recidiviz.calculator.calculation_documentation_generator."
        "CalculationDocumentationGenerator._get_sorted_state_metric_info",
    )
    def test_get_state_information(self, mock_metric_calculations: MagicMock) -> None:
        mock_metric_calculations.return_value = {
            "Missouri": [
                PipelineMetricInfo(
                    name="INCARCERATION_ADMISSION", month_count=24, frequency="daily"
                ),
                PipelineMetricInfo(
                    name="INCARCERATION_ADMISSION",
                    month_count=240,
                    frequency="triggered by code changes",
                ),
            ]
        }
        self.docs_generator.products_by_state = {
            state_code: {
                GCPEnvironment.STAGING: ["Test Product Without Exports"],
                GCPEnvironment.PRODUCTION: ["Test Product"],
            }
            for state_code in self.mock_product_states
        }
        for state_code in self.mock_product_states:
            expected_docs = """#Missouri

##Shipped Products

  - [Test Product](../products/test_product/test_product_summary.md)

##Products in Development

  - [Test Product Without Exports](../products/test_product_without_exports/test_product_without_exports_summary.md)

##Regularly Calculated Metrics

|                                      **Metric**                                      |**Number of Months Calculated**|**Calculation Frequency**|
|--------------------------------------------------------------------------------------|------------------------------:|-------------------------|
|[INCARCERATION_ADMISSION](../metrics/incarceration/incarceration_admission_metrics.md)|                             24|daily                    |
|[INCARCERATION_ADMISSION](../metrics/incarceration/incarceration_admission_metrics.md)|                            240|triggered by code changes|
"""
            documentation = (
                self.docs_generator._get_state_information(  # pylint: disable=W0212
                    state_code, str(state_code.get_state())
                )
            )

            self.assertEqual(expected_docs, documentation)

    def test_get_view_information(self) -> None:
        dag_key = DagKey(
            view_address=BigQueryAddress(dataset_id="dataset_1", table_id="table_1")
        )

        docs = self.docs_generator._get_view_information(  # pylint: disable=W0212
            dag_key
        )
        expected_documentation_string = """##dataset_1.table_1
_table_1 description_

####View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataset_1&t=table_1)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataset_1&t=table_1)
<br/>

####Dependency Trees

#####Parentage
[dataset_1.table_1](../dataset_1/table_1.md) <br/>
|--state.source_table ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=source_table)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=source_table)) <br/>


#####Descendants
[dataset_1.table_1](../dataset_1/table_1.md) <br/>
|--[dataset_3.table_3](../dataset_3/table_3.md) <br/>
|----[dataset_4.table_4](../dataset_4/table_4.md) <br/>
|----[dataset_5.table_5](../dataset_5/table_5.md) <br/>

"""
        self.assertEqual(expected_documentation_string, docs)

    def test_get_metric_information(self) -> None:
        self.docs_generator.state_metric_calculations_by_metric = {
            "INCARCERATION_ADMISSION": [
                StateMetricInfo(name="Missouri", month_count=24, frequency="daily"),
                StateMetricInfo(
                    name="Missouri",
                    month_count=240,
                    frequency="triggered by code changes",
                ),
            ]
        }

        docs = self.docs_generator._get_metric_information(  # pylint: disable=W0212
            IncarcerationAdmissionMetric
        )
        expected_documentation_string = """##IncarcerationAdmissionMetric

####Metric attributes in Big Query

* [**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dataflow_metrics&t=incarceration_admission_metrics)
<br/>
* [**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dataflow_metrics&t=incarceration_admission_metrics)
<br/>

####Calculation Cadences

|**State**|**Number of Months Calculated**|**Calculation Frequency**|
|---------|------------------------------:|-------------------------|
|Missouri |                             24|daily                    |
|Missouri |                            240|triggered by code changes|


####Dependent Views

If you are interested in what views rely on this metric, please run the following script in your shell:

```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id dataflow_metrics_materialized --view_id most_recent_incarceration_admission_metrics --show_downstream_dependencies True```

"""
        self.assertEqual(expected_documentation_string, docs)

    # TODO(#7125): Improve calc docs generation speed
    @unittest.skipIf(
        os.environ.get("TRAVIS") == "true", "docs/ does not exist in Travis"
    )
    def test_generate_markdowns(self) -> None:
        products = ProductConfig.product_configs_from_file(PRODUCTS_CONFIG_PATH)

        with tempfile.TemporaryDirectory() as tmpdirname:
            with local_project_id_override(GCP_PROJECT_STAGING), patch.object(
                BigQueryTableChecker, "_table_has_column"
            ) as mock_table_has_column, patch.object(
                BigQueryTableChecker, "_table_exists"
            ) as mock_table_exists:
                mock_table_has_column.return_value = True
                mock_table_exists.return_value = True
                docs_generator = CalculationDocumentationGenerator(
                    products=products, root_calc_docs_dir=tmpdirname
                )
            docs_generator.generate_products_markdowns()
            docs_generator.generate_states_markdowns()
            dir_comparison = dircmp(
                tmpdirname,
                os.path.join(os.path.dirname(recidiviz.__file__), "..", CALC_DOCS_PATH),
            )
            self.assertEqual(len(dir_comparison.diff_files), 0)
