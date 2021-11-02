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

"""A script which will be called using a pre-commit githook to generate our Calc Catalog documentation.

Can be run on-demand using:
    $ python -m recidiviz.calculator.calculation_documentation_generator
"""

import logging
import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Set, Type

import attr
from google.cloud import bigquery
from pytablewriter import MarkdownTableWriter

from recidiviz.big_query.big_query_view import BigQueryAddress, BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagNodeFamily,
    BigQueryViewDagWalker,
    DagKey,
)
from recidiviz.big_query.view_update_manager import build_views_to_update
from recidiviz.calculator.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
    PRODUCTION_TEMPLATES_PATH,
)
from recidiviz.calculator.pipeline.incarceration.metrics import IncarcerationMetric
from recidiviz.calculator.pipeline.program.metrics import ProgramMetric
from recidiviz.calculator.pipeline.recidivism.metrics import (
    ReincarcerationRecidivismMetric,
)
from recidiviz.calculator.pipeline.supervision.metrics import SupervisionMetric
from recidiviz.calculator.pipeline.utils.metric_utils import (
    PersonLevelMetric,
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.calculator.pipeline.violation.metrics import ViolationMetric
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    generate_metric_view_names,
)
from recidiviz.common import attr_validators
from recidiviz.common.attr_utils import get_enum_cls
from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.export.export_config import (
    PRODUCTS_CONFIG_PATH,
    VIEW_COLLECTION_EXPORT_INDEX,
    ProductConfig,
    ProductConfigs,
    ProductName,
)
from recidiviz.tools.docs.summary_file_generator import update_summary_file
from recidiviz.tools.docs.utils import persist_file_contents
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCPEnvironment
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.view_registry.datasets import (
    LATEST_VIEW_DATASETS,
    OTHER_SOURCE_TABLE_DATASETS,
    RAW_TABLE_DATASETS,
    VIEW_SOURCE_TABLE_DATASETS,
    VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS,
)
from recidiviz.view_registry.deployed_views import all_deployed_view_builders

ESCAPED_DOUBLE_UNDERSCORE = r"\__"
DATASETS_TO_SKIP_VIEW_DOCUMENTATION = LATEST_VIEW_DATASETS
CALC_DOCS_PATH = "docs/calculation"

MAX_DEPENDENCY_TREE_LENGTH = 250

DEPENDENCY_TREE_SCRIPT_TEMPLATE = """This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id {dataset_id} --view_id {table_id} --show_downstream_dependencies {descendants}```"""

BQ_LINK_TEMPLATE = """https://console.cloud.google.com/bigquery?pli=1&p={project}&page=table&project={project}&d={dataset_id}&t={table_id}"""

VIEW_DOCS_TEMPLATE = """##{view_dataset_id}.{view_table_id}
{description}

####View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**]({staging_link})
<br/>
[**Production**]({prod_link})
<br/>

####Dependency Trees

#####Parentage
{parent_tree}

#####Descendants
{child_tree}
"""

# TODO(#7563): Include more thorough documentation for each metric.
METRIC_DOCS_TEMPLATE = """##{metric_name}
{description}

####Metric attributes
Attributes specific to the `{metric_name}`:

{metric_attributes}

Attributes on all metrics:

{common_attributes}

####Metric tables in BigQuery

* [**Staging**]({staging_link})
<br/>
* [**Production**]({prod_link})
<br/>

####Calculation Cadences

{metrics_cadence_table}

####Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

{dependency_scripts_information_text}
"""

RAW_DATA_LINKS_TEMPLATE = (
    " ([Raw Data Doc](../../../ingest/{region}/raw_data/{raw_data_table}.md))"
    " ([BQ Staging]({staging_link}))"
    " ([BQ Prod]({prod_link}))"
)

SOURCE_TABLE_LINKS_TEMPLATE = " ([BQ Staging]({staging_link})) ([BQ Prod]({prod_link}))"

METRIC_LINKS_TEMPLATE = (
    " ([Metric Doc](../../metrics/{generic_type}/{metric_table}.md))"
)


@attr.s
class PipelineMetricInfo:
    """Stores info about calculation metric from the pipeline templates config."""

    # Metric name
    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # How many months the calculation includes
    month_count: Optional[int] = attr.ib(validator=attr_validators.is_opt_int)
    # Frequency of calculation
    frequency: str = attr.ib(validator=attr_validators.is_non_empty_str)


@attr.s
class StateMetricInfo:
    """Stores info about a state metric calculation from the pipeline templates config."""

    # State name
    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # How many months the calculation includes
    month_count: Optional[int] = attr.ib(validator=attr_validators.is_opt_int)
    # Frequency of calculation
    frequency: str = attr.ib(validator=attr_validators.is_non_empty_str)


class CalculationDocumentationGenerator:
    """A class for generating documentation about our calculations."""

    def __init__(
        self,
        products: List[ProductConfig],
        root_calc_docs_dir: str,
    ):
        self.root_calc_docs_dir = root_calc_docs_dir
        self.products = products

        self.states_by_product = self.get_states_by_product()

        # Reverses the states_by_product dictionary
        self.products_by_state: Dict[
            StateCode, Dict[GCPEnvironment, List[ProductName]]
        ] = defaultdict(lambda: defaultdict(list))

        for product_name, environments_to_states in self.states_by_product.items():
            for environment, states in environments_to_states.items():
                for state in states:
                    self.products_by_state[state][environment].append(product_name)
        self.dag_walker = BigQueryViewDagWalker(
            build_views_to_update(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                candidate_view_builders=all_deployed_view_builders(),
                dataset_overrides=None,
                override_should_build_predicate=True,
            )
        )
        self.prod_templates_yaml = YAMLDict.from_path(PRODUCTION_TEMPLATES_PATH)

        self.incremental_pipelines = self.prod_templates_yaml.pop_dicts(
            "incremental_pipelines"
        )
        self.historical_pipelines = self.prod_templates_yaml.pop_dicts(
            "historical_pipelines"
        )

        self.metric_calculations_by_state = self._get_state_metric_calculations(
            self.incremental_pipelines, "daily"
        )
        # combine with the historical pipelines
        for name, metric_info_list in self._get_state_metric_calculations(
            self.historical_pipelines, "triggered by code changes"
        ).items():
            self.metric_calculations_by_state[name].extend(metric_info_list)

        # Reverse the metric_calculations_by_state dictionary
        self.state_metric_calculations_by_metric: Dict[
            str, List[StateMetricInfo]
        ] = defaultdict(list)
        for state_name, metric_info_list in self.metric_calculations_by_state.items():
            for metric_info in metric_info_list:
                self.state_metric_calculations_by_metric[metric_info.name].append(
                    StateMetricInfo(
                        name=state_name,
                        month_count=metric_info.month_count,
                        frequency=metric_info.frequency,
                    )
                )

        self.metrics_by_generic_types = self._get_metrics_by_generic_types()

        self.generic_types_by_metric_name = {}
        for generic_type, metric_list in self.metrics_by_generic_types.items():
            for metric in metric_list:
                self.generic_types_by_metric_name[
                    DATAFLOW_METRICS_TO_TABLES[metric]
                ] = generic_type

        def _preprocess_views(
            v: BigQueryView, _parent_results: Dict[BigQueryView, None]
        ) -> None:
            dag_key = DagKey(view_address=v.address)
            node = self.dag_walker.nodes_by_key[dag_key]

            # Fills out full child/parent dependencies and tree representations for use
            # in various sections.
            self.dag_walker.populate_node_family_for_node(
                node=node,
                datasets_to_skip=RAW_TABLE_DATASETS,
                custom_node_formatter=self._dependency_tree_formatter_for_gitbook,
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS
                | LATEST_VIEW_DATASETS,
            )

        self.dag_walker.process_dag(_preprocess_views)
        self.all_views_to_document = self._get_all_views_to_document()

    def _get_all_export_config_view_builder_addresses(self) -> Set[BigQueryAddress]:
        all_export_view_builder_addresses: Set[BigQueryAddress] = set()
        for product in self.products:
            all_export_view_builder_addresses = all_export_view_builder_addresses.union(
                self._get_all_config_view_addresses_for_product(product)
            )
        return all_export_view_builder_addresses

    def get_states_by_product(
        self,
    ) -> Dict[ProductName, Dict[GCPEnvironment, List[StateCode]]]:
        """Returns the dict of products to states and environments."""
        states_by_product: Dict[
            ProductName, Dict[GCPEnvironment, List[StateCode]]
        ] = defaultdict(lambda: defaultdict(list))
        for product in self.products:
            if product.states is not None:
                for state in product.states:
                    environment = GCPEnvironment(state.environment)
                    state_code = StateCode(state.state_code)
                    states_by_product[product.name][environment].append(state_code)

        return states_by_product

    @staticmethod
    def bulleted_list(
        string_list: List[str], tabs: int = 1, escape_underscores: bool = True
    ) -> str:
        """Returns a string holding a bulleted list of the input string list."""
        return "\n".join(
            [
                f"{'  '*tabs}- {s.replace('__', ESCAPED_DOUBLE_UNDERSCORE) if escape_underscores else s}"
                for s in string_list
            ]
        )

    def _get_dataflow_pipeline_enabled_states(self) -> Set[StateCode]:
        """Returns the set of StateCodes for all states present in our production calc
        pipeline template."""
        states = {
            pipeline.peek("state_code", str).upper()
            for pipeline in self.incremental_pipelines
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
            if product.states is not None:
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

        state_names = [str(state_code.get_state()) for state_code in states]
        header = "- States\n"
        return header + self.bulleted_list(
            sorted(
                [
                    f"[{state_name}](calculation/states/{self._normalize_string_for_path(state_name)}.md)"
                    for state_name in state_names
                ]
            )
        )

    def _get_products_summary_str(self) -> str:
        header = "\n- Products\n"

        product_names = sorted([product.name for product in self.products])
        return header + self.bulleted_list(
            [
                f"[{product_name}](calculation/products/"
                f"{self._normalize_string_for_path(product_name)}/"
                f"{self._normalize_string_for_path(product_name)}_summary.md)"
                for product_name in product_names
            ]
        )

    def _get_views_summary_str(self) -> str:
        header = "\n- Views"
        bullets = ""
        for dataset_id, dag_key_list in self._get_keys_by_dataset(
            self.all_views_to_document
        ).items():
            bullets += f"\n  - {dataset_id}\n"
            bullets += self.bulleted_list(
                [
                    f"[{dag_key.table_id.replace('__', ESCAPED_DOUBLE_UNDERSCORE)}](calculation/views/{dataset_id}/{dag_key.table_id}.md)"
                    for dag_key in dag_key_list
                ],
                tabs=2,
                escape_underscores=False,
            )

        return header + bullets + "\n"

    @staticmethod
    def _get_metrics_by_generic_types() -> Dict[str, List[Type[RecidivizMetric]]]:
        metrics_dict: Dict[str, List[Type[RecidivizMetric]]] = defaultdict(list)

        for metric in DATAFLOW_METRICS_TO_TABLES:
            if issubclass(metric, SupervisionMetric):
                metrics_dict["Supervision"].append(metric)
            elif issubclass(metric, ReincarcerationRecidivismMetric):
                metrics_dict["Recidivism"].append(metric)
            elif issubclass(metric, ProgramMetric):
                metrics_dict["Program"].append(metric)
            elif issubclass(metric, IncarcerationMetric):
                metrics_dict["Incarceration"].append(metric)
            elif issubclass(metric, ViolationMetric):
                metrics_dict["Violation"].append(metric)
            else:
                raise ValueError(
                    f"{metric.__name__} is not a subclass of an expected"
                    f" metric type.)"
                )

        return metrics_dict

    def _get_dataflow_metrics_summary_str(self) -> str:

        dataflow_str = "\n- Dataflow Metrics\n"

        for header, class_list in self.metrics_by_generic_types.items():
            dataflow_str += f"  - {header.upper()}\n"

            dataflow_str += (
                self.bulleted_list(
                    [
                        f"[{metric.__name__}](calculation/metrics/{header.lower()}/{DATAFLOW_METRICS_TO_TABLES[metric]}.md)"
                        for metric in class_list
                    ],
                    2,
                )
                + "\n"
            )

        return dataflow_str

    def generate_summary_strings(self) -> List[str]:
        logging.info("Generating calculation summary markdown")
        calculation_catalog_summary = ["## Calculation Catalog\n\n"]

        calculation_catalog_summary.extend([self._get_calculation_states_summary_str()])
        calculation_catalog_summary.extend([self._get_products_summary_str()])
        calculation_catalog_summary.extend([self._get_views_summary_str()])
        calculation_catalog_summary.extend([self._get_dataflow_metrics_summary_str()])

        return calculation_catalog_summary

    def products_list_for_env(
        self, state_code: StateCode, environment: GCPEnvironment
    ) -> str:
        """Returns a bulleted list of products launched in the state in the given environment."""
        if environment not in {GCPEnvironment.PRODUCTION, GCPEnvironment.STAGING}:
            raise ValueError(f"Unexpected environment: [{environment.value}]")

        if (
            not state_code in self.products_by_state
            or environment not in self.products_by_state[state_code]
            or not self.products_by_state[state_code][environment]
        ):
            return "N/A"

        return self.bulleted_list(
            [
                f"[{product}](../products/{self._normalize_string_for_path(product)}/{self._normalize_string_for_path(product)}_summary.md)"
                for product in self.products_by_state[state_code][environment]
            ]
        )

    def states_list_for_env(
        self, product: ProductConfig, environment: GCPEnvironment
    ) -> str:
        """Returns a bulleted list of states where a product is launched in the given environment."""
        if environment not in {GCPEnvironment.PRODUCTION, GCPEnvironment.STAGING}:
            raise ValueError(f"Unexpected environment: [{environment.value}]")
        states_list = [
            f"[{str(state_code.get_state())}](../../states/{self._normalize_string_for_path(str(state_code.get_state()))}.md)"
            for state_code in self.states_by_product[product.name][environment]
        ]

        return self.bulleted_list(states_list) if states_list else "  N/A"

    def _get_shipped_states_str(self, product: ProductConfig) -> str:
        """Returns a string containing lists of shipped states and states in development
        for a given product."""

        shipped_states_str = self.states_list_for_env(
            product, GCPEnvironment.PRODUCTION
        )

        development_states_str = self.states_list_for_env(
            product, GCPEnvironment.STAGING
        )

        return (
            "##SHIPPED STATES\n"
            + shipped_states_str
            + "\n\n## STATES IN DEVELOPMENT\n"
            + development_states_str
            + "\n\n"
        )

    @staticmethod
    def _get_keys_by_dataset(dag_keys: Set[DagKey]) -> Dict[str, List[DagKey]]:
        """Given a set of DagKeys, returns a sorted dictionary of those keys, organized by dataset."""
        datasets_to_views = defaultdict(list)
        for key in sorted(
            dag_keys, key=lambda dag_key: (dag_key.dataset_id, dag_key.table_id)
        ):
            datasets_to_views[key.dataset_id].append(key)
        return datasets_to_views

    def _get_dataset_headers_to_views_str(
        self, dag_keys: Set[DagKey], source_tables_section: bool = False
    ) -> str:
        """Given a set of DagKeys, returns a str list of those views, organized by dataset."""
        datasets_to_keys = self._get_keys_by_dataset(dag_keys)
        views_str = ""
        for dataset, keys in datasets_to_keys.items():
            views_str += f"####{dataset}\n"
            views_str += (
                f"_{VIEW_SOURCE_TABLE_DATASETS_TO_DESCRIPTIONS[dataset]}_\n"
                if source_tables_section
                else ""
            )
            views_str += (
                self.bulleted_list(
                    [
                        self._dependency_tree_formatter_for_gitbook(
                            dag_key, products_section=True
                        )
                        for dag_key in keys
                    ],
                    escape_underscores=False,
                )
            ) + "\n\n"

        return views_str

    def _get_views_str_for_product(self, view_keys: Set[DagKey]) -> str:
        """Returns the string containing the VIEWS section of the product markdown."""
        views_header = "##VIEWS\n\n"
        if not view_keys:
            return views_header + "*This product does not use any BigQuery views.*\n\n"
        return views_header + self._get_dataset_headers_to_views_str(view_keys)

    def _get_source_tables_str_for_product(self, source_keys: Set[DagKey]) -> str:
        """Returns the string containing the SOURCE TABLES section of the product markdown."""
        source_tables_header = (
            "##SOURCE TABLES\n"
            "_Reference views that are used by other views. Some need to be updated manually._\n\n"
        )
        if not source_keys:
            return (
                source_tables_header
                + "*This product does not reference any source tables.*\n\n"
            )
        return source_tables_header + self._get_dataset_headers_to_views_str(
            source_keys, source_tables_section=True
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
        state_codes = sorted(
            self._get_dataflow_pipeline_enabled_states(), key=lambda code: code.value
        )

        headers = ["**Metric**"] + [
            f"**{state_code.value}**" for state_code in state_codes
        ]

        table_matrix = [
            [
                f"[{DATAFLOW_TABLES_TO_METRIC_TYPES[metric_key.table_id].value}](../../metrics/{self.generic_types_by_metric_name[metric_key.table_id].lower()}/{metric_key.table_id}.md)"
            ]
            + [
                "X"
                if DATAFLOW_TABLES_TO_METRIC_TYPES[metric_key.table_id].value
                in [
                    metric.name
                    for metric in self.metric_calculations_by_state[
                        str(state_code.get_state())
                    ]
                ]
                else ""
                for state_code in state_codes
            ]
            for metric_key in sorted(metric_keys, key=lambda dag_key: dag_key.table_id)
        ]

        writer = MarkdownTableWriter(
            headers=headers, value_matrix=table_matrix, margin=0
        )
        return metrics_header + writer.dumps()

    @staticmethod
    def _get_all_config_view_addresses_for_product(
        product: ProductConfig,
    ) -> Set[BigQueryAddress]:
        """Returns a set containing a BQ address for each view listed by each export
        necessary for the given product."""
        all_config_view_addresses: Set[BigQueryAddress] = set()
        for export in product.exports:
            collection_config = VIEW_COLLECTION_EXPORT_INDEX[export]
            view_builders = collection_config.view_builders_to_export

            all_config_view_addresses = all_config_view_addresses.union(
                {
                    BigQueryAddress(
                        dataset_id=view_builder.dataset_id,
                        table_id=view_builder.view_id,
                    )
                    for view_builder in view_builders
                }
            )

        return all_config_view_addresses

    def _get_all_parent_keys_for_product(self, product: ProductConfig) -> Set[DagKey]:
        """Returns a set containing a DagKey for every view that this product relies upon. """
        all_config_view_addresses = self._get_all_config_view_addresses_for_product(
            product
        )

        all_parent_keys: Set[DagKey] = set()
        for view_address in all_config_view_addresses:
            dag_key = DagKey(view_address=view_address)
            node = self.dag_walker.nodes_by_key[dag_key]
            # Add in the top level view
            all_parent_keys.add(dag_key)
            # Add in all ancestors
            all_parent_keys = all_parent_keys.union(node.node_family.full_parentage)

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

        documentation = f"#{product.name.upper()}\n"
        documentation += product.description + "\n"

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
    def _normalize_string_for_path(target_string: str) -> str:
        """Returns a lowercase, underscore-separated string."""
        return target_string.lower().replace(" ", "_")

    def generate_products_markdowns(self) -> bool:
        """Generates markdown files if necessary for the docs/calculation/products
        directories"""
        anything_modified = False
        for product in self.products:
            # Generate documentation for each product
            documentation = self._get_product_information(product)

            # Write documentation to markdown files
            product_name_for_path = self._normalize_string_for_path(product.name)
            product_dir_path = os.path.join(
                self.root_calc_docs_dir, "products", product_name_for_path
            )
            os.makedirs(product_dir_path, exist_ok=True)

            product_markdown_path = os.path.join(
                product_dir_path,
                f"{product_name_for_path}_summary.md",
            )

            anything_modified |= persist_file_contents(
                documentation, product_markdown_path
            )
        return anything_modified

    @staticmethod
    def _get_state_metric_calculations(
        pipelines: List[YAMLDict], frequency: str
    ) -> Dict[str, List[PipelineMetricInfo]]:
        """Returns a dict of state names to lists of info about their regularly
        calculated metrics."""
        state_metric_calculations = defaultdict(list)
        for pipeline in pipelines:
            state_metric_calculations[
                str(StateCode(pipeline.peek("state_code", str)).get_state())
            ].extend(
                [
                    PipelineMetricInfo(
                        name=metric,
                        month_count=pipeline.peek_optional(
                            "calculation_month_count", int
                        ),
                        frequency=frequency,
                    )
                    for metric in pipeline.peek("metric_types", str).split()
                ],
            )
        return state_metric_calculations

    def _get_sorted_state_metric_info(self) -> Dict[str, List[PipelineMetricInfo]]:
        """Returns a dictionary of state names (in alphabetical order) to their
        regularly calculated metric information (sorted by metric name)"""
        sorted_state_metric_calculations: Dict[str, List[PipelineMetricInfo]] = {
            state_name_key: sorted(
                self.metric_calculations_by_state[state_name_key],
                key=lambda info: info.name,
            )
            for state_name_key in sorted(self.metric_calculations_by_state)
        }
        return sorted_state_metric_calculations

    def _get_metrics_table_for_state(self, state_name: str) -> str:
        sorted_state_metric_calculations = self._get_sorted_state_metric_info()
        metric_names_to_tables = {
            metric.value: table
            for table, metric in DATAFLOW_TABLES_TO_METRIC_TYPES.items()
        }
        if state_name in sorted_state_metric_calculations:
            headers = [
                "**Metric**",
                "**Number of Months Calculated**",
                "**Calculation Frequency**",
            ]
            table_matrix = [
                [
                    f"[{metric_info.name}](../metrics/{self.generic_types_by_metric_name[metric_names_to_tables[metric_info.name]].lower()}/{metric_names_to_tables[metric_info.name]}.md)",
                    metric_info.month_count if metric_info.month_count else "N/A",
                    metric_info.frequency,
                ]
                for metric_info in sorted_state_metric_calculations[state_name]
            ]
            writer = MarkdownTableWriter(
                headers=headers, value_matrix=table_matrix, margin=0
            )
            return writer.dumps()
        return "_This state has no regularly calculated metrics._"

    def _get_state_information(self, state_code: StateCode, state_name: str) -> str:
        """Returns string contents for the state markdown."""
        documentation = f"#{state_name}\n\n"

        # Products section
        documentation += "##Shipped Products\n\n"
        documentation += self.products_list_for_env(
            state_code, GCPEnvironment.PRODUCTION
        )
        documentation += "\n\n##Products in Development\n\n"
        documentation += self.products_list_for_env(state_code, GCPEnvironment.STAGING)

        # Metrics section
        documentation += "\n\n##Regularly Calculated Metrics\n\n"

        documentation += self._get_metrics_table_for_state(state_name)

        return documentation

    def generate_states_markdowns(self) -> bool:
        """Generate markdown files for each state."""
        anything_modified = False

        states_dir_path = os.path.join(self.root_calc_docs_dir, "states")
        os.makedirs(states_dir_path, exist_ok=True)

        for state_code in self._get_dataflow_pipeline_enabled_states():
            state_name = str(state_code.get_state())

            # Generate documentation
            documentation = self._get_state_information(state_code, state_name)

            # Write to markdown files
            states_markdown_path = os.path.join(
                states_dir_path,
                f"{self._normalize_string_for_path(state_name)}.md",
            )
            anything_modified |= persist_file_contents(
                documentation, states_markdown_path
            )
        return anything_modified

    def _dependency_tree_formatter_for_gitbook(
        self,
        dag_key: DagKey,
        products_section: bool = False,
    ) -> str:
        """Gitbook-specific formatting for the generated dependency tree."""
        is_source_table = dag_key.dataset_id in OTHER_SOURCE_TABLE_DATASETS - {
            DATAFLOW_METRICS_DATASET
        }
        is_raw_data_table = dag_key.dataset_id in LATEST_VIEW_DATASETS
        is_metric = dag_key.dataset_id in DATAFLOW_METRICS_DATASET
        is_documented_view = not (is_source_table or is_raw_data_table or is_metric)
        if is_raw_data_table and (
            not dag_key.dataset_id.endswith("_raw_data_up_to_date_views")
            or not dag_key.table_id.endswith("_latest")
        ):
            raise ValueError(
                f"Unexpected raw data view address: [{dag_key.dataset_id}.{dag_key.table_id}]"
            )

        staging_link = StrictStringFormatter().format(
            BQ_LINK_TEMPLATE,
            project="recidiviz-staging",
            dataset_id=dag_key.dataset_id,
            table_id=dag_key.table_id,
        )
        prod_link = StrictStringFormatter().format(
            BQ_LINK_TEMPLATE,
            project="recidiviz-123",
            dataset_id=dag_key.dataset_id,
            table_id=dag_key.table_id,
        )

        table_name_str = (
            # Include brackets if metric or view
            ("[" if products_section else f"[{dag_key.dataset_id}.")
            + f"{dag_key.table_id.replace('__', ESCAPED_DOUBLE_UNDERSCORE)}]"
            if is_documented_view or is_metric
            else ("" if products_section else f"{dag_key.dataset_id}.")
            + f"{dag_key.table_id.replace('__', ESCAPED_DOUBLE_UNDERSCORE)}"
        )

        if is_source_table:
            table_name_str += (
                f" ([BQ Staging]({staging_link})) ([BQ Prod]({prod_link}))"
            )
        elif is_metric:
            table_name_str += f"(../../metrics/{self.generic_types_by_metric_name[dag_key.table_id].lower()}/{dag_key.table_id}.md)"
        elif is_raw_data_table:
            table_name_str += StrictStringFormatter().format(
                RAW_DATA_LINKS_TEMPLATE,
                region=dag_key.dataset_id[:-26],
                raw_data_table=dag_key.table_id[:-7],
                staging_link=staging_link,
                prod_link=prod_link,
            )
        else:
            table_name_str += f"(../{'../views/' if products_section else ''}{dag_key.dataset_id}/{dag_key.table_id}.md)"

        return table_name_str + " <br/>"

    @staticmethod
    def _get_view_tree_string(
        node_family: BigQueryViewDagNodeFamily,
        dag_key: DagKey,
        descendants: bool = False,
    ) -> str:
        """Gets the string representation of the view's tree.

        If it is too large a command to generate it will be output instead."""
        if (node_family.full_parentage and not descendants) or (
            node_family.full_descendants and descendants
        ):
            # Gitbook has a line count limit of ~525 for the view markdowns, so we
            # only print trees if they are small enough. Otherwise, we direct readers
            # to a script that prints the tree to console.
            if descendants:
                if (
                    node_family.child_dfs_tree_str.count("<br/>")
                    < MAX_DEPENDENCY_TREE_LENGTH
                ):
                    return node_family.child_dfs_tree_str
            else:
                if (
                    node_family.parent_dfs_tree_str.count("<br/>")
                    < MAX_DEPENDENCY_TREE_LENGTH
                ):
                    return node_family.parent_dfs_tree_str
            return StrictStringFormatter().format(
                DEPENDENCY_TREE_SCRIPT_TEMPLATE,
                dataset_id=dag_key.dataset_id,
                table_id=dag_key.table_id,
                descendants=descendants,
            )
        return f"This view has no {'child' if descendants else 'parent'} dependencies."

    @staticmethod
    def _create_script_text_for_dependencies(metric_name: str) -> str:
        metric_view_names = generate_metric_view_names(metric_name)
        output = ""

        for metric_view_name in metric_view_names:
            output = (
                output
                + f"```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging "
                f"--dataset_id dataflow_metrics_materialized --view_id most_recent_{metric_view_name} "
                f"--show_downstream_dependencies True```\n"
            )
        return output

    def _get_view_information(self, view_key: DagKey) -> str:
        """Returns string contents for a view markdown."""
        view_node = self.dag_walker.nodes_by_key[view_key]

        staging_link = StrictStringFormatter().format(
            BQ_LINK_TEMPLATE,
            project="recidiviz-staging",
            dataset_id=view_key.dataset_id,
            table_id=view_key.table_id,
        )
        prod_link = StrictStringFormatter().format(
            BQ_LINK_TEMPLATE,
            project="recidiviz-123",
            dataset_id=view_key.dataset_id,
            table_id=view_key.table_id,
        )
        documentation = StrictStringFormatter().format(
            VIEW_DOCS_TEMPLATE,
            view_dataset_id=view_key.dataset_id,
            view_table_id=view_key.table_id,
            description=view_node.view.description,
            staging_link=staging_link,
            prod_link=prod_link,
            parent_tree=self._get_view_tree_string(view_node.node_family, view_key),
            child_tree=self._get_view_tree_string(
                view_node.node_family, view_key, descendants=True
            ),
        )
        return documentation

    def _get_all_views_to_document(self) -> Set[DagKey]:
        """Retrieve all DAG Walker views that we want to document"""
        all_nodes = self.dag_walker.nodes_by_key.values()
        all_view_keys = {
            DagKey(view_address=node.view.address)
            for node in all_nodes
            if node.dag_key.dataset_id not in DATASETS_TO_SKIP_VIEW_DOCUMENTATION
        }
        return all_view_keys

    def generate_view_markdowns(self) -> bool:
        """Generate markdown files for each view."""
        anything_modified = False
        views_dir_path = os.path.join(self.root_calc_docs_dir, "views")
        os.makedirs(views_dir_path, exist_ok=True)

        for view_key in self.all_views_to_document:
            # Generate documentation
            documentation = self._get_view_information(view_key)

            # Write to markdown files
            dataset_dir = os.path.join(
                views_dir_path,
                view_key.dataset_id,
            )
            os.makedirs(dataset_dir, exist_ok=True)

            view_markdown_path = os.path.join(
                dataset_dir,
                f"{view_key.table_id}.md",
            )
            anything_modified |= persist_file_contents(
                documentation, view_markdown_path
            )
        return anything_modified

    def _get_metric_information(self, metric: Type[RecidivizMetric]) -> str:
        """Returns string contents for a metric markdown."""
        metric_table_id = DATAFLOW_METRICS_TO_TABLES[metric]
        metric_type = DATAFLOW_TABLES_TO_METRIC_TYPES[metric_table_id].value

        state_infos_list = sorted(
            self.state_metric_calculations_by_metric[metric_type],
            key=lambda info: (info.name, info.month_count),
        )
        state_info_headers = [
            "**State**",
            "**Number of Months Calculated**",
            "**Calculation Frequency**",
        ]
        state_info_table_matrix = [
            [
                f"[{state_info.name}](../../states/{self._normalize_string_for_path(state_info.name)}.md)",
                state_info.month_count if state_info.month_count else "N/A",
                state_info.frequency,
            ]
            for state_info in state_infos_list
        ]
        state_info_writer = MarkdownTableWriter(
            headers=state_info_headers, value_matrix=state_info_table_matrix, margin=0
        )

        def _get_enum_class_name_for_schema_field(
            schema_field: bigquery.SchemaField,
        ) -> str:
            enum_cls = get_enum_cls(attr.fields_dict(metric)[schema_field.name])

            if not enum_cls:
                return ""

            if schema_field.name == "metric_type":
                return RecidivizMetricType.__name__

            return enum_cls.__name__

        attribute_info_headers = [
            "**Attribute Name**",
            "**Type**",
            "**Enum Class**",
        ]

        shared_attributes = set(attr.fields_dict(RecidivizMetric).keys()).union(
            set(attr.fields_dict(PersonLevelMetric).keys())
        )

        metric_attribute_table_matrix = [
            [
                field.name,
                field.field_type,
                _get_enum_class_name_for_schema_field(field),
            ]
            for field in metric.bq_schema_for_metric_table()
            if field.name not in shared_attributes
        ]
        metric_attribute_info_writer = MarkdownTableWriter(
            headers=attribute_info_headers,
            value_matrix=metric_attribute_table_matrix,
            margin=0,
        )

        common_attribute_table_matrix = [
            [
                field.name,
                field.field_type,
                _get_enum_class_name_for_schema_field(field),
            ]
            for field in metric.bq_schema_for_metric_table()
            if field.name in shared_attributes
        ]
        common_attribute_info_writer = MarkdownTableWriter(
            headers=attribute_info_headers,
            value_matrix=common_attribute_table_matrix,
            margin=0,
        )

        documentation = StrictStringFormatter().format(
            METRIC_DOCS_TEMPLATE,
            metric_attributes=metric_attribute_info_writer.dumps(),
            common_attributes=common_attribute_info_writer.dumps(),
            staging_link=StrictStringFormatter().format(
                BQ_LINK_TEMPLATE,
                project="recidiviz-staging",
                dataset_id="dataflow_metrics",
                table_id=metric_table_id,
            ),
            prod_link=StrictStringFormatter().format(
                BQ_LINK_TEMPLATE,
                project="recidiviz-123",
                dataset_id="dataflow_metrics",
                table_id=metric_table_id,
            ),
            metric_name=metric.__name__,
            description=metric.get_description(),
            metrics_cadence_table=state_info_writer.dumps(),
            dependency_scripts_information_text=self._create_script_text_for_dependencies(
                metric_table_id
            ),
        )

        return documentation

    def generate_metric_markdowns(self) -> bool:
        """Generate markdown files for each metric."""
        anything_modified = False
        metrics_dir_path = os.path.join(self.root_calc_docs_dir, "metrics")
        os.makedirs(metrics_dir_path, exist_ok=True)

        for generic_type, class_list in sorted(self.metrics_by_generic_types.items()):
            generic_type_dir = os.path.join(
                metrics_dir_path,
                generic_type.lower(),
            )
            os.makedirs(generic_type_dir, exist_ok=True)

            for metric in class_list:
                # Generate documentation
                documentation = self._get_metric_information(metric)

                # Write to markdown files
                metric_markdown_path = os.path.join(
                    generic_type_dir,
                    f"{DATAFLOW_METRICS_TO_TABLES[metric]}.md",
                )
                anything_modified |= persist_file_contents(
                    documentation, metric_markdown_path
                )

        return anything_modified


def _create_ingest_catalog_calculation_summary(
    docs_generator: CalculationDocumentationGenerator,
) -> List[str]:
    return docs_generator.generate_summary_strings()


def generate_calculation_documentation(
    docs_generator: CalculationDocumentationGenerator,
) -> bool:
    modified = docs_generator.generate_products_markdowns()
    modified |= docs_generator.generate_states_markdowns()
    modified |= docs_generator.generate_view_markdowns()
    modified |= docs_generator.generate_metric_markdowns()
    return modified


def main() -> int:
    with local_project_id_override(GCP_PROJECT_STAGING):
        products = ProductConfigs.from_file(PRODUCTS_CONFIG_PATH).products
        docs_generator = CalculationDocumentationGenerator(products, CALC_DOCS_PATH)
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
