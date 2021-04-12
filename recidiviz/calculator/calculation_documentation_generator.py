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

"""Functionality for generating documentation about our calculations."""
import logging
import sys
from collections import defaultdict
from typing import List, Dict, Set

from recidiviz.calculator.dataflow_config import (
    PRODUCTION_TEMPLATES_PATH,
    DATAFLOW_METRICS_TO_TABLES,
)
from recidiviz.calculator.pipeline.incarceration.metrics import IncarcerationMetric
from recidiviz.calculator.pipeline.program.metrics import ProgramMetric
from recidiviz.calculator.pipeline.recidivism.metrics import (
    ReincarcerationRecidivismMetric,
)
from recidiviz.calculator.pipeline.supervision.metrics import (
    SupervisionMetric,
)

from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.export.export_config import PRODUCTS_CONFIG_PATH, ProductConfig
from recidiviz.tools.docs.summary_file_generator import update_summary_file
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.yaml_dict import YAMLDict

CALC_DOCS_PATH = "docs/calculation"


class CalculationDocumentationGenerator:
    """A class for generating documentation about our calculations."""

    @staticmethod
    def _get_dataflow_pipeline_enabled_states() -> Set[StateCode]:
        prod_templates_yaml = YAMLDict.from_path(PRODUCTION_TEMPLATES_PATH)
        daily_pipelines = prod_templates_yaml.pop_dicts("daily_pipelines")
        historical_pipelines = prod_templates_yaml.pop_dicts("historical_pipelines")
        states = {
            pipeline.pop("state_code", str).upper() for pipeline in daily_pipelines
        }.union(
            {
                pipeline.pop("state_code", str).upper()
                for pipeline in historical_pipelines
            }
        )

        for state_code in states:
            if not StateCode.is_state_code(state_code):
                raise ValueError(
                    f"Found invalid state code value [{state_code}]"
                    f" in pipeline template config."
                )

        return {StateCode(state_code) for state_code in states}

    @staticmethod
    def _get_product_enabled_states() -> Set[StateCode]:
        products = ProductConfig.product_configs_from_file(PRODUCTS_CONFIG_PATH)
        states: Set[str] = set()
        for product in products:
            states = states.union({state.state_code for state in product.states})

        for state_code in states:
            if not StateCode.is_state_code(state_code):
                raise ValueError(
                    f"Found invalid state code value [{state_code}]"
                    f" in product config."
                )
        return {StateCode(state_code) for state_code in states}

    def _get_calculation_states_summary_str(self) -> str:
        states = self._get_dataflow_pipeline_enabled_states().union(
            self._get_product_enabled_states()
        )

        state_names = [state_code.get_state() for state_code in states]
        header = "- #### STATES (links to come)\n"
        return (
            header
            + "  - "
            + "\n  - ".join(sorted([f"{state_name}" for state_name in state_names]))
            + "\n"
        )

    @staticmethod
    def _get_products_summary_str() -> str:
        header = "\n- #### PRODUCTS (links to come)\n"

        products = YAMLDict.from_path(PRODUCTS_CONFIG_PATH).pop("products", list)
        return (
            header
            + "  - "
            + "\n  - ".join(sorted([product["name"] for product in products]))
            + "\n"
        )

    @staticmethod
    def _get_metric_types() -> Dict[str, List[str]]:
        metrics_dict = defaultdict(list)

        for metric in DATAFLOW_METRICS_TO_TABLES:
            if issubclass(metric, SupervisionMetric):
                metrics_dict["Supervision"].append(f"{metric.__name__}")
            elif issubclass(metric, ReincarcerationRecidivismMetric):
                metrics_dict["Recidivism"].append(f"{metric.__name__}")
            elif issubclass(metric, ProgramMetric):
                metrics_dict["Program"].append(f"{metric.__name__}")
            elif issubclass(metric, IncarcerationMetric):
                metrics_dict["Incarceration"].append(f"{metric.__name__}")
            else:
                raise ValueError(
                    f"{metric.__name__} is not a subclass of an expected"
                    f" metric type.)"
                )

        return metrics_dict

    def _get_dataflow_metrics_summary_str(self) -> str:

        dataflow_str = "\n- #### DATAFLOW METRICS (links to come)\n"
        headers_to_metrics = self._get_metric_types()

        for header, class_list in headers_to_metrics.items():
            dataflow_str += f"  - ##### {header.upper()}\n"
            dataflow_str += "    - " + "\n    - ".join(class_list) + "\n"

        return dataflow_str

    def _build_summary_string(self) -> str:
        summary = self._get_calculation_states_summary_str()
        summary += self._get_dataflow_metrics_summary_str()

        return summary

    def generate_summary_strings(self) -> List[str]:
        logging.info("Generating calculation summary markdown")
        calculation_catalog_summary = ["## Calculation Catalog\n\n"]

        calculation_catalog_summary.extend([self._get_calculation_states_summary_str()])
        calculation_catalog_summary.extend([self._get_products_summary_str()])
        calculation_catalog_summary.extend([self._get_dataflow_metrics_summary_str()])

        return calculation_catalog_summary


def _create_ingest_catalog_calculation_summary(
    docs_generator: CalculationDocumentationGenerator,
) -> List[str]:
    return docs_generator.generate_summary_strings()


def generate_calculation_documentation() -> bool:
    # TODO(#6490): update this function to use the docs_generator
    return False


def main() -> int:
    with local_project_id_override(GCP_PROJECT_STAGING):
        docs_generator = CalculationDocumentationGenerator()
        modified = generate_calculation_documentation()
        if modified:
            update_summary_file(
                _create_ingest_catalog_calculation_summary(docs_generator),
                "## Calculation Catalog",
            )
        return 1 if modified else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(main())
