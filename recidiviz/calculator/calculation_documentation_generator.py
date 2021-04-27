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
import os
import sys
from collections import defaultdict
from typing import List, Dict, Set

from pytablewriter import MarkdownTableWriter

from recidiviz.big_query.big_query_view import BigQueryAddress
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    DagKey,
)
from recidiviz.calculator.dataflow_config import (
    PRODUCTION_TEMPLATES_PATH,
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
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
from recidiviz.metrics.export.export_config import (
    PRODUCTS_CONFIG_PATH,
    ProductConfig,
    VIEW_COLLECTION_EXPORT_INDEX,
    ProductStateConfig,
)
from recidiviz.tools.docs.summary_file_generator import update_summary_file
from recidiviz.utils.environment import (
    GCP_PROJECT_STAGING,
    GCPEnvironment,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.view_registry.datasets import (
    VIEW_SOURCE_TABLE_DATASETS,
    OTHER_SOURCE_TABLE_DATASETS,
    LATEST_VIEW_DATASETS,
    VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS,
)
from recidiviz.view_registry.deployed_views import DEPLOYED_VIEW_BUILDERS

CALC_DOCS_PATH = "docs/calculation"


class CalculationDocumentationGenerator:
    """A class for generating documentation about our calculations."""

    def __init__(self, products: List[ProductConfig]):
        self.products = products
        walker_views = [view_builder.build() for view_builder in DEPLOYED_VIEW_BUILDERS]
        self.dag_walker = BigQueryViewDagWalker(walker_views)
        self.prod_templates_yaml = YAMLDict.from_path(PRODUCTION_TEMPLATES_PATH)
        self.daily_pipelines = self.prod_templates_yaml.pop_dicts("daily_pipelines")
        self.historical_pipelines = self.prod_templates_yaml.pop_dicts(
            "historical_pipelines"
        )

    @staticmethod
    def bulleted_list(string_list: List[str], tabs: int = 1) -> str:
        # To avoid formatting issues for some view IDs we use an escape sequence
        escaped_underscore = r"\__"
        return "\n".join(
            [f"{'  '*tabs}- {s.replace('__', escaped_underscore)}" for s in string_list]
        )

    def _get_states_to_metrics(self) -> Dict[str, List[str]]:
        states_to_metrics = defaultdict(list)
        for pipeline in self.daily_pipelines:
            states_to_metrics[pipeline.peek("state_code", str)].extend(
                pipeline.peek("metric_types", str).split()
            )
        return states_to_metrics

    def _get_dataflow_pipeline_enabled_states(self) -> Set[StateCode]:
        states = {
            pipeline.peek("state_code", str).upper()
            for pipeline in self.daily_pipelines
        }.union(
            {
                pipeline.peek("state_code", str).upper()
                for pipeline in self.historical_pipelines
            }
        )

        for state_code in states:
            if not StateCode.is_state_code(state_code):
                raise ValueError(
                    f"Found invalid state code value [{state_code}]"
                    f" in pipeline template config."
                )

        return {StateCode(state_code) for state_code in states}

    def _get_product_enabled_states(self) -> Set[StateCode]:
        states: Set[str] = set()
        for product in self.products:
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
        header = "- States (links to come)\n"
        return header + self.bulleted_list(
            sorted([f"{state_name}" for state_name in state_names])
        )

    def _get_products_summary_str(self) -> str:
        header = "\n- Products\n"

        product_names = sorted([product.name for product in self.products])
        return header + self.bulleted_list(
            [
                f"[{product_name}](calculation/products/"
                f"{self._get_product_name_for_path(product_name)}/"
                f"{self._get_product_name_for_path(product_name)}_summary.md)"
                for product_name in product_names
            ]
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

        dataflow_str = "\n- Dataflow Metrics (links to come)\n"
        headers_to_metrics = self._get_metric_types()

        for header, class_list in headers_to_metrics.items():
            dataflow_str += f"  - {header.upper()}\n"
            dataflow_str += self.bulleted_list(class_list, 2) + "\n"

        return dataflow_str

    def generate_summary_strings(self) -> List[str]:
        logging.info("Generating calculation summary markdown")
        calculation_catalog_summary = ["## Calculation Catalog\n\n"]

        calculation_catalog_summary.extend([self._get_calculation_states_summary_str()])
        calculation_catalog_summary.extend([self._get_products_summary_str()])
        calculation_catalog_summary.extend([self._get_dataflow_metrics_summary_str()])

        return calculation_catalog_summary

    def states_list_for_env(
        self, states: List[ProductStateConfig], environment: GCPEnvironment
    ) -> str:
        if environment not in {GCPEnvironment.PRODUCTION, GCPEnvironment.STAGING}:
            raise ValueError(f"Unexpected environment: [{environment.value}]")
        states_list = [
            StateCode(state.state_code).get_state().name
            for state in states
            if state.environment == environment.value
        ]

        return self.bulleted_list(states_list) if states_list else "  N/A"

    def _get_shipped_states_str(self, product: ProductConfig) -> str:
        shipped_states_str = self.states_list_for_env(
            product.states, GCPEnvironment.PRODUCTION
        )
        development_states_str = self.states_list_for_env(
            product.states, GCPEnvironment.STAGING
        )

        return (
            "##SHIPPED STATES\n"
            + shipped_states_str
            + "\n\n## STATES IN DEVELOPMENT\n"
            + development_states_str
            + "\n\n"
        )

    def _get_dataset_headers_to_views(
        self, dag_keys: Set[DagKey], add_descriptions: bool = False
    ) -> str:
        datasets_to_views = defaultdict(list)
        for key in dag_keys:
            datasets_to_views[key.dataset_id].append(key.table_id)
        views_str = ""
        for dataset, views in sorted(datasets_to_views.items()):
            views_str += f"####{dataset}\n"
            views_str += (
                f"_{VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS[dataset]}_\n"
                if add_descriptions
                else ""
            )
            views_str += self.bulleted_list(sorted(views)) + "\n\n"

        return views_str

    def _get_views_str_for_product(self, view_keys: Set[DagKey]) -> str:
        views_header = "##VIEWS\n\n"
        if not view_keys:
            return views_header + "*This product does not use any BigQuery views.*\n\n"
        return views_header + self._get_dataset_headers_to_views(view_keys)

    def _get_source_tables_str_for_product(self, source_keys: Set[DagKey]) -> str:
        source_tables_header = (
            "##SOURCE TABLES\n"
            "_Reference views that are used by other views. Some need to be updated manually._\n\n"
        )
        if not source_keys:
            return (
                source_tables_header
                + "*This product does not reference any source tables.*\n\n"
            )
        return source_tables_header + self._get_dataset_headers_to_views(
            source_keys, add_descriptions=True
        )

    def _get_metrics_str_for_product(self, metric_keys: Set[DagKey]) -> str:
        """Builds the Metrics string for the product markdown file. Creates a table of
        necessary metric types and whether a state calculates those metrics"""
        metrics_header = (
            "##METRICS\n_All metrics required to support this product and"
            " whether or not each state regularly calculates the metric._"
            "\n\n** DISCLAIMER **\nThe presence of all required metrics"
            " for a state does not guarantee that this product is ready to"
            " launch in that state.\n\n"
        )

        if not metric_keys:
            return (
                metrics_header + "*This product does not rely on Dataflow metrics.*\n"
            )
        metric_types = [
            DATAFLOW_TABLES_TO_METRIC_TYPES[key.table_id].value for key in metric_keys
        ]

        state_codes = sorted(
            [
                state_code.value
                for state_code in self._get_dataflow_pipeline_enabled_states()
            ]
        )

        headers = ["**Metric**"] + [f"**{state_code}**" for state_code in state_codes]

        table_matrix = [
            [f"{metric_type}"]
            + [
                "X" if metric_type in self._get_states_to_metrics()[state_code] else ""
                for state_code in state_codes
            ]
            for metric_type in sorted(metric_types)
        ]

        writer = MarkdownTableWriter(
            headers=headers, value_matrix=table_matrix, margin=1
        )
        return metrics_header + writer.dumps()

    def _get_all_parent_keys_for_product(self, product: ProductConfig) -> Set[DagKey]:
        """Uses the BigQueryViewDagWalker to create a set of all parent dag nodes, then
        parses that set into views, metrics, and reference tables. Returns the file
        contents for a calc product markdown."""
        all_parent_keys: Set[DagKey] = set()

        for export in product.exports:
            collection_config = VIEW_COLLECTION_EXPORT_INDEX[export]
            view_builders = collection_config.view_builders_to_export

            # These views will be the starter nodes for the recursive call
            all_config_view_keys = {
                DagKey(
                    view_address=BigQueryAddress(
                        dataset_id=view_builder.dataset_id,
                        table_id=view_builder.view_id,
                    )
                )
                for view_builder in view_builders
            }

            start_nodes = [
                self.dag_walker.nodes_by_key[key] for key in all_config_view_keys
            ]
            parentage = all_config_view_keys
            for node in start_nodes:
                parentage = parentage.union(
                    self.dag_walker.find_full_parentage(
                        node, VIEW_SOURCE_TABLE_DATASETS | LATEST_VIEW_DATASETS
                    )
                )

            all_parent_keys = all_parent_keys.union(parentage)

        # Ignore materialized metric views as relevant metric info can be found in a
        # different dataset (DATAFLOW_METRICS_DATASET).
        all_parent_keys.difference_update(
            {
                key
                for key in all_parent_keys
                if key.dataset_id == DATAFLOW_METRICS_MATERIALIZED_DATASET
            }
        )
        return all_parent_keys

    def _get_product_information(self, product: ProductConfig) -> str:
        """Returns a string containing all relevant information for a given product
        including name, views used, source tables, and required metrics."""

        # TODO(#7063): Add product descriptions
        documentation = f"#{product.name.upper()}\n"

        documentation += self._get_shipped_states_str(product)

        all_parent_keys = self._get_all_parent_keys_for_product(product)

        source_keys = {
            key
            for key in all_parent_keys
            # Metric info will be included in the metric-specific section
            if key.dataset_id
            in OTHER_SOURCE_TABLE_DATASETS - {DATAFLOW_METRICS_DATASET}
        }

        metric_keys = {
            key for key in all_parent_keys if key.dataset_id == DATAFLOW_METRICS_DATASET
        }

        # Remove metric keys as they are surfaced in a metric-specific section. Remove
        # source table keys as they are surfaced in a reference-specific section
        view_keys = all_parent_keys - metric_keys - source_keys

        documentation += self._get_views_str_for_product(view_keys)
        documentation += self._get_source_tables_str_for_product(source_keys)
        documentation += self._get_metrics_str_for_product(metric_keys)

        return documentation

    @staticmethod
    def _get_product_name_for_path(product_name: str) -> str:
        return product_name.lower().replace(" ", "_")

    def generate_products_markdowns(self, root_dir_path: str) -> bool:
        """Generates markdown files if necessary for the docs/calculation/products
        directories"""
        anything_modified = False
        for product in self.products:
            # Generate documentation for each product
            documentation = self._get_product_information(product)

            # Write documentation to markdown files
            product_name_for_path = self._get_product_name_for_path(product.name)
            product_dir_path = os.path.join(
                root_dir_path, "products", product_name_for_path
            )
            os.makedirs(product_dir_path, exist_ok=True)

            product_markdown_path = os.path.join(
                product_dir_path,
                f"{product_name_for_path}_summary.md",
            )

            prior_documentation = None
            if os.path.exists(product_markdown_path):
                with open(product_markdown_path, "r") as product_md_file:
                    prior_documentation = product_md_file.read()

            if prior_documentation != documentation:
                with open(product_markdown_path, "w") as raw_data_md_file:
                    raw_data_md_file.write(documentation)
                    anything_modified = True

        return anything_modified


def _create_ingest_catalog_calculation_summary(
    docs_generator: CalculationDocumentationGenerator,
) -> List[str]:
    return docs_generator.generate_summary_strings()


def generate_calculation_documentation(
    docs_generator: CalculationDocumentationGenerator,
) -> bool:
    # TODO(#7125): Add calc doc generation to the pre-commit config
    modified = docs_generator.generate_products_markdowns(CALC_DOCS_PATH)
    return modified


def main() -> int:
    with local_project_id_override(GCP_PROJECT_STAGING):
        products = ProductConfig.product_configs_from_file(PRODUCTS_CONFIG_PATH)
        docs_generator = CalculationDocumentationGenerator(products)
        modified = generate_calculation_documentation(docs_generator)
        if modified:
            update_summary_file(
                _create_ingest_catalog_calculation_summary(docs_generator),
                "## Calculation Catalog",
            )
        return 1 if modified else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(main())
