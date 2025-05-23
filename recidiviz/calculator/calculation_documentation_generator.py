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
from typing import Any, Dict, List, Literal, Optional, Set, Type

import attr
from google.cloud import bigquery
from pytablewriter import MarkdownTableWriter

import recidiviz
from recidiviz.aggregated_metrics.models import (
    aggregated_metric_configurations as metric_config,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.build_views_to_update import build_views_to_update
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    generate_metric_view_names,
)
from recidiviz.calculator.view_type_specific_documentation import (
    get_view_type_specific_documentation,
)
from recidiviz.common import attr_validators
from recidiviz.common.attr_utils import get_enum_cls
from recidiviz.common.constants.states import StateCode
from recidiviz.common.file_system import delete_files, get_all_files_recursive
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_VIEWS_DATASET
from recidiviz.metrics.export.export_config import VIEW_COLLECTION_EXPORT_INDEX
from recidiviz.metrics.export.products.product_configs import (
    PRODUCTS_CONFIG_PATH,
    ProductConfig,
    ProductConfigs,
    ProductName,
)
from recidiviz.observations.observation_big_query_view_collector import (
    ObservationBigQueryViewCollector,
)
from recidiviz.observations.observation_type_utils import ObservationType
from recidiviz.pipelines.config_paths import PIPELINE_CONFIG_YAML_PATH
from recidiviz.pipelines.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
)
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.pipelines.metrics.incarceration.metrics import IncarcerationMetric
from recidiviz.pipelines.metrics.population_spans.metrics import PopulationSpanMetric
from recidiviz.pipelines.metrics.program.metrics import ProgramMetric
from recidiviz.pipelines.metrics.recidivism.metrics import (
    ReincarcerationRecidivismMetric,
)
from recidiviz.pipelines.metrics.supervision.metrics import SupervisionMetric
from recidiviz.pipelines.metrics.utils.metric_utils import (
    PersonLevelMetric,
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.pipelines.metrics.violation.metrics import ViolationMetric
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_all_source_table_datasets,
    get_source_table_datasets_to_descriptions,
)
from recidiviz.tools.docs.summary_file_generator import update_summary_file
from recidiviz.tools.docs.utils import persist_file_contents
from recidiviz.utils.environment import (
    DATA_PLATFORM_GCP_PROJECTS,
    GCP_PROJECT_STAGING,
    GCPEnvironment,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type
from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.view_registry.deployed_views import all_deployed_view_builders

RAW_TABLE_DATASETS = {
    raw_tables_dataset_for_region(
        state_code=state_code,
        instance=instance,
        sandbox_dataset_prefix=None,
    )
    for instance in DirectIngestInstance
    for state_code in StateCode
}

LATEST_VIEW_DATASETS = {
    raw_latest_views_dataset_for_region(
        state_code=state_code,
        instance=instance,
        sandbox_dataset_prefix=None,
    )
    for instance in DirectIngestInstance
    for state_code in StateCode
}

STATE_SPECIFIC_NORMALIZED_STATE_DATASETS = {
    normalized_state_dataset_for_state_code(
        state_code=state_code,
        sandbox_dataset_prefix=None,
    )
    for instance in DirectIngestInstance
    for state_code in StateCode
}

ESCAPED_DOUBLE_UNDERSCORE = r"\__"
DATASETS_TO_SKIP_VIEW_DOCUMENTATION = LATEST_VIEW_DATASETS
CALC_DOCS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(recidiviz.__file__)), "docs/calculation"
)

MAX_DEPENDENCY_TREE_LENGTH = 250

DEPENDENCY_TREE_SCRIPT_TEMPLATE = """This dependency tree is too large to display in its entirety. To see the full tree, run the following script in your shell: <br/>
```python -m recidiviz.tools.display_bq_dag_for_view --project_id recidiviz-staging --dataset_id {dataset_id} --view_id {table_id} --show_downstream_dependencies {descendants}```"""

BQ_LINK_TEMPLATE = """https://console.cloud.google.com/bigquery?pli=1&p={project}&page=table&project={project}&d={dataset_id}&t={table_id}"""

VIEW_DOCS_TEMPLATE = """## {view_dataset_id}.{view_table_id}
{description}
{view_type_specific_contents}
#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**]({staging_link})
<br/>
[**Production**]({prod_link})
<br/>

#### Direct Parents
{parent_list}

#### Direct Children
{child_list}
"""

METRIC_DOCS_TEMPLATE = """## {metric_name}
{description}

#### Metric attributes
Attributes specific to the `{metric_name}`:

{metric_attributes}

Attributes on all metrics:

{common_attributes}

#### Metric tables in BigQuery

* [**Staging**]({staging_link})
<br/>
* [**Production**]({prod_link})
<br/>

#### Calculation Cadences

{metrics_cadence_table}

#### Dependent Views

If you are interested in what views rely on this metric, please run the following script(s) in your shell:

{dependency_scripts_information_text}
"""


@attr.s
class PipelineMetricInfo:
    """Stores info about calculation metric from the pipeline templates config."""

    # Metric name
    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # How many months the calculation includes
    month_count: Optional[int] = attr.ib(validator=attr_validators.is_opt_int)
    # Frequency of calculation
    frequency: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # Whether the calculation is only being run in staging
    staging_only: Optional[bool] = attr.ib(validator=attr_validators.is_opt_bool)


@attr.s
class StateMetricInfo:
    """Stores info about a state metric calculation from the pipeline templates config."""

    # State name
    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # How many months the calculation includes
    month_count: Optional[int] = attr.ib(validator=attr_validators.is_opt_int)
    # Frequency of calculation
    frequency: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # Whether the calculation is only being run in staging
    staging_only: Optional[bool] = attr.ib(validator=attr_validators.is_opt_bool)


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
        all_view_builders = all_deployed_view_builders()
        self.view_builders_by_address = {vb.address: vb for vb in all_view_builders}

        self.all_source_table_datasets = get_all_source_table_datasets()

        self.all_deployed_view_and_materialized_addresses = set(
            self.view_builders_by_address
        ) | {vb.table_for_query for vb in all_view_builders}

        # It doesn't matter what the project_id is - we're just building the graph to
        # understand view relationships
        with local_project_id_override(GCP_PROJECT_STAGING):
            views_to_update = build_views_to_update(
                view_source_table_datasets=self.all_source_table_datasets,
                candidate_view_builders=all_view_builders,
                sandbox_context=None,
            )

        self.all_source_table_datasets_to_descriptions = (
            self._get_all_source_table_datasets_to_descriptions()
        )

        self.dag_walker = BigQueryViewDagWalker(views_to_update)
        self.prod_templates_yaml = YAMLDict.from_path(PIPELINE_CONFIG_YAML_PATH)

        self.metric_pipelines = self.prod_templates_yaml.pop_dicts("metric_pipelines")

        self.metric_calculations_by_state = self._get_state_metric_calculations(
            self.metric_pipelines, "daily"
        )

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
                        staging_only=metric_info.staging_only,
                    )
                )

        self.metrics_by_generic_types = self._get_metrics_by_generic_types()

        self.generic_types_by_metric_name = {}
        for generic_type, metric_list in self.metrics_by_generic_types.items():
            for metric in metric_list:
                self.generic_types_by_metric_name[
                    DATAFLOW_METRICS_TO_TABLES[metric]
                ] = generic_type

        self.all_views_to_document = [
            v
            for v in views_to_update
            if not v.address.dataset_id in DATASETS_TO_SKIP_VIEW_DOCUMENTATION
        ]

        # Cached results of calls to _big_query_address_formatter_for_gitbook().
        self.formatted_address_cache: dict[BigQueryAddress, str] = {}

    @staticmethod
    def _get_all_source_table_datasets_to_descriptions() -> dict[str, str]:
        all_datasets_to_descriptions: dict[str, str] = {}
        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            with local_project_id_override(project_id):
                all_datasets_to_descriptions = {
                    **all_datasets_to_descriptions,
                    **get_source_table_datasets_to_descriptions(project_id),
                }
        return all_datasets_to_descriptions

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
        """Returns the set of StateCodes for all states present in our calculation_pipeline_templates.yaml."""
        states = {
            pipeline.peek("state_code", str).upper()
            for pipeline in self.metric_pipelines
        }

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

    def _get_views_summary_str(self) -> str:
        header = "\n- Views"
        bullets = ""
        for dataset_id, address_list in self._get_addresses_by_dataset(
            {view.address for view in self.all_views_to_document}
        ).items():
            bullets += f"\n  - {dataset_id}\n"
            bullets += self.bulleted_list(
                [
                    f"[{address.table_id.replace('__', ESCAPED_DOUBLE_UNDERSCORE)}](calculation/views/{dataset_id}/{address.table_id}.md)"
                    for address in address_list
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
            elif issubclass(metric, PopulationSpanMetric):
                metrics_dict["PopulationSpan"].append(metric)
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

    def _get_aggregated_metrics_summary_str(self) -> str:
        agg_metric_str = "- Aggregated Metrics\n"

        agg_metric_pages = {
            "observations": "Observations",
            "aggregated_metrics": "Metrics",
        }
        agg_metric_str += (
            self.bulleted_list(
                [
                    f"[{page_title}](calculation/aggregated_metrics/{page_name}.md)"
                    for page_name, page_title in agg_metric_pages.items()
                ],
            )
            + "\n"
        )
        return agg_metric_str

    def generate_summary_strings(self) -> List[str]:
        logging.info("Generating calculation summary markdown")
        calculation_catalog_summary = ["## Calculation Catalog\n\n"]

        calculation_catalog_summary.extend([self._get_aggregated_metrics_summary_str()])
        calculation_catalog_summary.extend([self._get_calculation_states_summary_str()])
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

        return self.bulleted_list(list(self.products_by_state[state_code][environment]))

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

    @staticmethod
    def _get_addresses_by_dataset(
        addresses: Set[BigQueryAddress],
    ) -> Dict[str, List[BigQueryAddress]]:
        """
        Given a set of BigQueryAddresses, returns a sorted dictionary of
        those addresses, organized by dataset.
        """
        datasets_to_views = defaultdict(list)
        for key in sorted(addresses):
            datasets_to_views[key.dataset_id].append(key)
        return datasets_to_views

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

    @staticmethod
    def _normalize_string_for_path(target_string: str) -> str:
        """Returns a lowercase, underscore-separated string."""
        return target_string.lower().replace(" ", "_")

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
                        staging_only=pipeline.peek_optional("staging_only", bool),
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
        documentation = f"# {state_name}\n\n"

        # Products section
        documentation += "## Enabled Product Metric Exports (PROD)\n\n"
        documentation += self.products_list_for_env(
            state_code, GCPEnvironment.PRODUCTION
        )
        documentation += "\n\n## Enabled Product Metric Exports (STAGING)\n\n"
        documentation += self.products_list_for_env(state_code, GCPEnvironment.STAGING)

        # Metrics section
        documentation += "\n\n## Regularly Calculated Metrics\n\n"

        documentation += self._get_metrics_table_for_state(state_name)

        return documentation

    def generate_states_markdowns(self) -> bool:
        """Generate markdown files for each state."""
        logging.info("Generating state documenation")
        anything_modified = False

        states_dir_path = os.path.join(self.root_calc_docs_dir, "states")
        os.makedirs(states_dir_path, exist_ok=True)

        existing_state_files: Set[str] = get_all_files_recursive(states_dir_path)
        new_state_files: Set[str] = set()

        for state_code in self._get_dataflow_pipeline_enabled_states():
            state_name = str(state_code.get_state())

            # Generate documentation
            documentation = self._get_state_information(state_code, state_name)

            # Write to markdown files
            state_file_name = f"{self._normalize_string_for_path(state_name)}.md"
            states_markdown_path = os.path.join(
                states_dir_path,
                state_file_name,
            )

            anything_modified |= persist_file_contents(
                documentation, states_markdown_path
            )

            # Keep track of new state files
            new_state_files.add(states_markdown_path)

        # Delete any deprecated state files
        deprecated_files = existing_state_files.difference(new_state_files)
        if deprecated_files:
            delete_files(deprecated_files, delete_empty_dirs=True)
            anything_modified |= True

        return anything_modified

    def _big_query_address_formatter_for_gitbook(
        self, *, address: BigQueryAddress
    ) -> str:
        """Gitbook-specific formatting for the generated dependency tree."""

        if address in self.formatted_address_cache:
            return self.formatted_address_cache[address]

        is_documented_view = (
            address in self.all_deployed_view_and_materialized_addresses
        )
        is_raw_data_table = address.dataset_id in RAW_TABLE_DATASETS
        is_raw_data_view = address.dataset_id in LATEST_VIEW_DATASETS

        staging_link = StrictStringFormatter().format(
            BQ_LINK_TEMPLATE,
            project="recidiviz-staging",
            dataset_id=address.dataset_id,
            table_id=address.table_id,
        )
        prod_link = StrictStringFormatter().format(
            BQ_LINK_TEMPLATE,
            project="recidiviz-123",
            dataset_id=address.dataset_id,
            table_id=address.table_id,
        )

        display_address = address
        # We want to display the unioned normalized state views as being in the
        #  normalized_state dataset, not normalized_state_views.
        if address.dataset_id == NORMALIZED_STATE_VIEWS_DATASET:
            display_address = assert_type(
                self.dag_walker.nodes_by_address[address].view.materialized_address,
                BigQueryAddress,
            )

        if is_raw_data_table or is_raw_data_view:
            # We don't produce separate view docs for the _latest views because it's
            # mostly redundant with the raw data docs, so we just link directly to the
            # underlying raw data docs for these.
            state_code = assert_type(address.state_code_for_address(), StateCode)
            raw_data_table = address.table_id.removesuffix("_latest")
            table_name_str = f"{display_address.to_str()} ([Raw Data Doc](../../../ingest/{state_code.value.lower()}/raw_data/{raw_data_table}.md))"
        elif is_documented_view:
            table_name_str = f"[{display_address.to_str()}](../{address.dataset_id}/{address.table_id}.md)"
        else:
            table_name_str = f"{display_address.to_str()} ([BQ Staging]({staging_link})) ([BQ Prod]({prod_link}))"

        table_name_str = table_name_str.replace("__", ESCAPED_DOUBLE_UNDERSCORE)
        self.formatted_address_cache[address] = table_name_str
        return table_name_str

    def _get_view_direct_relatives_string(
        self, view: BigQueryView, relative_type: Literal["parents", "children"]
    ) -> str:
        dag_node = self.dag_walker.nodes_by_address[view.address]

        match relative_type:
            case "parents":
                relatives = dag_node.parent_node_addresses | dag_node.source_addresses
                if not relatives:
                    return "This view has no parent tables / views."
            case "children":
                relatives = dag_node.child_node_addresses
                if not relatives:
                    return "This view has no child tables / views."
            case _:
                raise ValueError(f"Found unexpected relative_type [{relative_type}]")

        if not relatives:
            raise ValueError(
                f"Expected to find relatives but found none: "
                f"{view.address.to_str()}, {relative_type=}"
            )

        linked_address_strs = [
            self._big_query_address_formatter_for_gitbook(address=address) + " <br/>"
            for address in sorted(relatives, key=lambda a: (a.dataset_id, a.table_id))
        ]

        return self.bulleted_list(linked_address_strs, escape_underscores=False)

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

    def _get_view_information(self, view: BigQueryView) -> str:
        """Returns string contents for a view markdown."""
        address = view.address
        description = view.description

        staging_link = StrictStringFormatter().format(
            BQ_LINK_TEMPLATE,
            project="recidiviz-staging",
            dataset_id=address.dataset_id,
            table_id=address.table_id,
        )
        prod_link = StrictStringFormatter().format(
            BQ_LINK_TEMPLATE,
            project="recidiviz-123",
            dataset_id=address.dataset_id,
            table_id=address.table_id,
        )
        documentation = StrictStringFormatter().format(
            VIEW_DOCS_TEMPLATE,
            view_dataset_id=address.dataset_id,
            view_table_id=address.table_id,
            description=description,
            staging_link=staging_link,
            prod_link=prod_link,
            view_type_specific_contents=(
                get_view_type_specific_documentation(
                    self.view_builders_by_address[view.address]
                )
                or ""
            ),
            parent_list=self._get_view_direct_relatives_string(view, "parents"),
            child_list=self._get_view_direct_relatives_string(view, "children"),
        )
        return documentation

    def generate_view_markdowns(self) -> bool:
        """Generate markdown files for each view."""
        logging.info("Generating view documentation")
        views_dir_path = os.path.join(self.root_calc_docs_dir, "views")
        os.makedirs(views_dir_path, exist_ok=True)

        datasets = {v.dataset_id for v in self.all_views_to_document}
        for dataset_id in datasets:
            dataset_dir = os.path.join(views_dir_path, dataset_id)
            os.makedirs(dataset_dir, exist_ok=True)

        existing_view_files: Set[str] = get_all_files_recursive(views_dir_path)
        new_view_files: Set[str] = set()

        anything_modified = False
        for view in self.all_views_to_document:
            # Generate documentation
            documentation = self._get_view_information(view=view)

            # Write to markdown files
            view_markdown_path = os.path.join(
                views_dir_path,
                view.address.dataset_id,
                f"{view.address.table_id}.md",
            )

            anything_modified |= persist_file_contents(
                documentation, view_markdown_path
            )

            # Keep track of new view files
            new_view_files.add(view_markdown_path)

        # Delete deprecated view files
        deprecated_files = existing_view_files.difference(new_view_files)
        if deprecated_files:
            delete_files(deprecated_files, delete_empty_dirs=True)
            anything_modified |= True

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
            "**Staging Only**",
        ]
        state_info_table_matrix = [
            [
                f"[{state_info.name}](../../states/{self._normalize_string_for_path(state_info.name)}.md)",
                state_info.month_count if state_info.month_count else "N/A",
                state_info.frequency,
                "X" if state_info.staging_only else "",
            ]
            for state_info in state_infos_list
        ]
        state_info_writer = MarkdownTableWriter(
            headers=state_info_headers, value_matrix=state_info_table_matrix, margin=0
        )

        def _get_enum_class_name_for_schema_field(
            schema_field: bigquery.SchemaField,
        ) -> str:
            enum_cls = get_enum_cls(attr.fields_dict(metric)[schema_field.name])  # type: ignore[arg-type]

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

        # Cast these as Any so mypy does not complain that 'Only concrete class can be
        # given where "Type[AttrsInstance]" is expected'
        recidiviz_metric_cls: Any = RecidivizMetric
        person_level_metric_cls: Any = PersonLevelMetric
        shared_attributes = set(attr.fields_dict(recidiviz_metric_cls).keys()).union(
            set(attr.fields_dict(person_level_metric_cls).keys())
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
        logging.info("Generating Dataflow metric documentation")
        anything_modified = False
        metrics_dir_path = os.path.join(self.root_calc_docs_dir, "metrics")
        os.makedirs(metrics_dir_path, exist_ok=True)

        existing_metric_files: Set[str] = get_all_files_recursive(metrics_dir_path)
        new_metric_files: Set[str] = set()

        for generic_type, class_list in sorted(self.metrics_by_generic_types.items()):
            metric_dir_name = generic_type.lower()
            generic_type_dir = os.path.join(metrics_dir_path, metric_dir_name)
            os.makedirs(generic_type_dir, exist_ok=True)

            for metric in class_list:
                # Generate documentation
                documentation = self._get_metric_information(metric)

                # Write to markdown files
                metric_file_name = f"{DATAFLOW_METRICS_TO_TABLES[metric]}.md"
                metric_markdown_path = os.path.join(
                    generic_type_dir,
                    metric_file_name,
                )

                anything_modified |= persist_file_contents(
                    documentation, metric_markdown_path
                )

                # Keep track of new metric files
                new_metric_files.add(metric_markdown_path)

        # Delete deprecated metric files
        deprecated_files = existing_metric_files.difference(new_metric_files)
        if deprecated_files:
            delete_files(deprecated_files, delete_empty_dirs=True)
            anything_modified |= True

        return anything_modified

    def _generate_agregated_metric_table_view(
        self, metrics: list[AggregatedMetric]
    ) -> str:
        """Generates table with each aggregated metric's name, column name, description and observation conditions"""
        headers = [
            "**Metric**",
            "**Column Name**",
            "**Description**",
            "**Metric Type**",
            "**Observation Conditions**",
        ]
        table_matrix = [
            [
                metric.display_name,
                metric.name,
                metric.description,
                type(metric).__name__,
                metric.get_observation_conditions_string(
                    filter_by_observation_type=False,
                    read_observation_attributes_from_json=False,
                ),
            ]
            for metric in metrics
        ]
        writer = MarkdownTableWriter(
            headers=headers, value_matrix=table_matrix, margin=0
        )
        return writer.dumps()

    def _generate_aggregated_metric_summary_markdown(self) -> str:
        """Generates markdown for all aggregated metrics"""
        all_aggregated_metrics: List[AggregatedMetric] = []
        for item in dir(metric_config):
            metric_object = getattr(metric_config, item)
            if isinstance(metric_object, AggregatedMetric):
                all_aggregated_metrics.append(metric_object)
            elif isinstance(metric_object, list) and all(
                isinstance(metric, AggregatedMetric) for metric in metric_object
            ):
                all_aggregated_metrics.extend(metric_object)

        metrics_by_observation_type: dict[ObservationType, list] = defaultdict(list)
        for metric in all_aggregated_metrics:
            metrics_by_observation_type[metric.observation_type].append(metric)

        metrics_by_observation_documentation = []
        for observation_type, metrics in metrics_by_observation_type.items():
            documentation = f"""
# {snake_to_title(observation_type.value)} Metrics
[{snake_to_title(observation_type.unit_of_observation_type.value)}-Level Observation Type: {observation_type}]({self._get_github_builder_code_url_for_observation_type(observation_type)})
{self._generate_agregated_metric_table_view(metrics)}
"""
            metrics_by_observation_documentation.append(documentation)

        table_of_contents = "\n".join(
            [
                f"- [{snake_to_title(observation_type.value)} Metrics](#{observation_type.value.lower()}-metrics)"
                for observation_type in sorted(
                    metrics_by_observation_type,
                    key=lambda observation_type: observation_type.value,
                )
            ]
        )
        all_tables = "\n".join(sorted(metrics_by_observation_documentation))
        return f"""
# Table Of Contents
{table_of_contents}


{all_tables}"""

    def _get_github_builder_code_url_for_observation_type(
        self, observation_type: ObservationType
    ) -> str:
        """Returns a URL for the query builder code associated with an observation type"""
        observation_type_folder = f"{observation_type.observation_type_category()}s"
        observation_type_name = observation_type.name.lower()
        unit_of_observation_name = (
            observation_type.unit_of_observation_type.value.lower()
        )
        return f"https://github.com/Recidiviz/pulse-data/tree/main/recidiviz/observations/views/{observation_type_folder}/{unit_of_observation_name}/{observation_type_name}.py"

    def _generate_events_summary_markdown(self) -> str:
        headers = [
            "**Event Observation**",
            "**Unit of Observation**",
            "**Attributes**",
        ]
        event_builders = ObservationBigQueryViewCollector().collect_event_builders()
        table_matrix = [
            [
                f"[{event.event_type.value}]({self._get_github_builder_code_url_for_observation_type(event.event_type)})",
                event.event_type.unit_of_observation_type.value,
                ",<br/>".join(event.attribute_cols),
            ]
            for event in sorted(event_builders, key=lambda event: event.event_type.name)
        ]
        writer = MarkdownTableWriter(
            headers=headers, value_matrix=table_matrix, margin=0
        )
        return writer.dumps()

    def _generate_spans_summary_markdown(self) -> str:
        headers = [
            "**Span Observation**",
            "**Unit of Observation**",
            "**Attributes**",
        ]
        span_builders = ObservationBigQueryViewCollector().collect_span_builders()
        table_matrix = [
            [
                f"[{span.span_type.value}]({self._get_github_builder_code_url_for_observation_type(span.span_type)})",
                span.span_type.unit_of_observation_type.value,
                ",<br/>".join(span.attribute_cols),
            ]
            for span in sorted(span_builders, key=lambda span: span.span_type.name)
        ]
        writer = MarkdownTableWriter(
            headers=headers, value_matrix=table_matrix, margin=0
        )
        return writer.dumps()

    def _generate_observations_summary_markdown(self) -> str:
        return f"""
# Event Observations
{self._generate_events_summary_markdown()}
# Span Observations
{self._generate_spans_summary_markdown()}
"""

    def generate_aggregated_metric_markdowns(self) -> bool:
        logging.info("Generating aggregated metric documentation")
        anything_modified = False
        metrics_dir_path = os.path.join(self.root_calc_docs_dir, "aggregated_metrics")

        observations_markdown_path = os.path.join(metrics_dir_path, "observations.md")
        anything_modified |= persist_file_contents(
            self._generate_observations_summary_markdown(), observations_markdown_path
        )

        aggregated_metrics_markdown_path = os.path.join(
            metrics_dir_path, "aggregated_metrics.md"
        )
        anything_modified |= persist_file_contents(
            self._generate_aggregated_metric_summary_markdown(),
            aggregated_metrics_markdown_path,
        )
        return anything_modified


def _create_ingest_catalog_calculation_summary(
    docs_generator: CalculationDocumentationGenerator,
) -> List[str]:
    return docs_generator.generate_summary_strings()


def generate_calculation_documentation(
    docs_generator: CalculationDocumentationGenerator,
) -> bool:
    modified = docs_generator.generate_states_markdowns()
    modified |= docs_generator.generate_view_markdowns()
    modified |= docs_generator.generate_metric_markdowns()
    modified |= docs_generator.generate_aggregated_metric_markdowns()
    return modified


def main() -> int:
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
