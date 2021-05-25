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
"""Config classes for exporting metric views to Google Cloud Storage."""
import os
from typing import Optional, Sequence, List, Dict, Set, TypedDict

import attr

from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_views import (
    VITALS_VIEW_BUILDERS,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.metrics import export as export_module
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
)
from recidiviz.view_registry.namespaces import BigQueryViewNamespace
from recidiviz.calculator.query.justice_counts.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_EXPORT as JUSTICE_COUNTS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.covid_dashboard.covid_dashboard_views import (
    COVID_DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import (
    LANTERN_DASHBOARD_VIEW_BUILDERS,
    CORE_DASHBOARD_VIEW_BUILDERS,
    DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.po_report.po_monthly_report_data import (
    PO_MONTHLY_REPORT_DATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.public_dashboard_views import (
    PUBLIC_DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.case_triage.views.view_config import CASE_TRIAGE_EXPORTED_VIEW_BUILDERS
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common import attr_validators
from recidiviz.ingest.views.view_config import INGEST_METADATA_BUILDERS
from recidiviz.utils import environment
from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.validation.views.view_config import VALIDATION_METADATA_BUILDERS

PRODUCTS_CONFIG_PATH = os.path.join(
    os.path.dirname(export_module.__file__),
    "products.yaml",
)

ProductName = str


class BadProductExportSpecificationError(ValueError):
    pass


@attr.s
class ProductStateConfig:
    """Stores a product's status information for a given state"""

    # State code
    state_code: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # Product launch environment for this specific state
    environment: str = attr.ib(validator=attr_validators.is_non_empty_str)

    def is_state_launched_in_env(self) -> bool:
        """Returns true if product can be launched in the given state in the current
         environment

        If we are in prod, the product config must be explicitly set to specify
        this product can be launched in prod. All exports can be triggered to run in
        staging.
        """
        return (
            not environment.in_gcp_production()
            or self.environment == environment.get_gcp_environment()
        )


@attr.s
class ProductConfig:
    """Stores information about a product, parsed from the product config file"""

    # Product name
    name: ProductName = attr.ib(validator=attr_validators.is_non_empty_str)
    # List of export names required for this product
    exports: List[str] = attr.ib(validator=attr_validators.is_list)
    # Product launch environment, only for state agnostic products
    environment: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)
    # List of relevant state status information for a product. None if product is
    # state-agnostic.
    states: Optional[List[ProductStateConfig]] = attr.ib(
        validator=attr_validators.is_opt_list
    )
    # Identifies product as state agnostic.
    is_state_agnostic: Optional[bool] = attr.ib(
        validator=attr_validators.is_bool, default=False
    )

    def __attrs_post_init__(self) -> None:
        if self.states and self.environment is not None:
            raise ValueError(
                f"Product with name: [{self.name}] has relevant state statuses."
                f" Environment information should be set at the state level, not the"
                f" product level."
            )
        if self.is_state_agnostic and self.states is not None:
            raise ValueError(
                f"Product with name: [{self.name}] is state agnostic"
                f" but contains enabled states: [{self.states}] in its config."
                f" Check the product config."
            )
        if not self.is_state_agnostic and self.states is None:
            raise ValueError(
                f"Product with name: [{self.name}] is not state agnostic"
                f" but does not contain enabled states in its config."
                f" Check the product config."
            )

    @property
    def launched_states(self) -> Optional[Set[str]]:
        return (
            None
            if self.states is None
            else {
                state.state_code.upper()
                for state in self.states
                if state.is_state_launched_in_env()
            }
        )


class ProductExportConfig(TypedDict):
    export_job_name: str
    state_code: Optional[str]


@attr.s
class ProductConfigs:
    """Loads product configs from file, stores them, and finds a product by job name and state code"""

    products: List[ProductConfig] = attr.ib()

    def get_export_configs_for_job_filter(
        self, export_job_filter: str
    ) -> List[ProductExportConfig]:
        """Returns the export configs for the given export_job_filter,
        which can be either state_code or export job name."""
        filter_uppercase = export_job_filter.upper()
        if StateCode.is_state_code(filter_uppercase):
            return [
                export
                for export in self.get_all_export_configs()
                if export["state_code"] == filter_uppercase
            ]
        return [
            export
            for export in self.get_all_export_configs()
            if export["export_job_name"] == filter_uppercase
        ]

    def get_export_config(
        self, export_job_name: str, state_code: Optional[str] = None
    ) -> ProductExportConfig:
        relevant_product_exports = [
            product
            for product in self.products
            if export_job_name.upper() in product.exports
        ]
        if len(relevant_product_exports) != 1:
            raise BadProductExportSpecificationError(
                f"Wrong number of products returned for export for export_job_name {export_job_name.upper()}",
            )
        if state_code is None and relevant_product_exports[0].states is not None:
            raise BadProductExportSpecificationError(
                f"Missing required state_code parameter for export_job_name {export_job_name.upper()}",
            )
        return ProductExportConfig(
            export_job_name=export_job_name, state_code=state_code
        )

    def get_all_export_configs(self) -> List["ProductExportConfig"]:
        exports = []
        for product in self.products:
            if product.is_state_agnostic:
                for export in product.exports:
                    exports.append(self.get_export_config(export_job_name=export))
            else:
                for export in product.exports:
                    if product.states is not None:
                        for state in product.states:
                            exports.append(
                                self.get_export_config(
                                    export_job_name=export, state_code=state.state_code
                                )
                            )
        return exports

    @classmethod
    def from_file(cls, path: str = PRODUCTS_CONFIG_PATH) -> "ProductConfigs":
        """Reads a product config file and returns a list of corresponding ProductConfig objects."""

        product_config = YAMLDict.from_path(path).pop("products", list)
        products = [
            ProductConfig(
                name=product["name"],
                exports=product["exports"],
                states=[
                    ProductStateConfig(
                        state_code=state["state_code"], environment=state["environment"]
                    )
                    for state in product["states"]
                ]
                if "states" in product
                else None,
                environment=product.get("environment"),
                is_state_agnostic=product.get("is_state_agnostic", False),
            )
            for product in product_config
        ]
        return cls(products=products)


@attr.s(frozen=True)
class ExportViewCollectionConfig:
    """Stores information necessary for exporting metric data from a list of views in a dataset to a Google Cloud
    Storage Bucket."""

    # The list of views to be exported
    view_builders_to_export: Sequence[BigQueryViewBuilder] = attr.ib()

    # A string template defining the output URI path for the destination directory of the export
    output_directory_uri_template: str = attr.ib()

    # The name of the export config, used to filter to exports with specific names
    export_name: str = attr.ib()

    # The category of BigQuery views of which this export belongs
    bq_view_namespace: BigQueryViewNamespace = attr.ib()

    # List of output formats for these configs
    export_output_formats: Optional[List[ExportOutputFormatType]] = attr.ib(
        default=None
    )

    # If set to True then empty files are considered valid. If False then some
    # validators may choose to mark empty files as invalid.
    allow_empty: bool = attr.ib(default=False)

    def export_configs_for_views_to_export(
        self,
        project_id: str,
        state_code_filter: Optional[str] = None,
        dataset_overrides: Optional[Dict[str, str]] = None,
        destination_override: Optional[str] = None,
    ) -> Sequence[ExportBigQueryViewConfig]:
        """Builds a list of ExportBigQueryViewConfig that define how all views in
        view_builders_to_export should be exported to Google Cloud Storage."""

        view_filter_clause = (
            f" WHERE state_code = '{state_code_filter}'" if state_code_filter else None
        )

        intermediate_table_name = "{export_view_name}_table"
        if destination_override:
            output_directory = destination_override.format(project_id=project_id)
        else:
            output_directory = self.output_directory_uri_template.format(
                project_id=project_id
            )
        if state_code_filter:
            intermediate_table_name += f"_{state_code_filter}"
            output_directory += f"/{state_code_filter}"

        configs = []
        for vb in self.view_builders_to_export:
            view = vb.build(dataset_overrides=dataset_overrides)
            optional_args = {}
            if self.export_output_formats is not None:
                optional_args["export_output_formats"] = self.export_output_formats
            configs.append(
                ExportBigQueryViewConfig(
                    bq_view_namespace=self.bq_view_namespace,
                    view=view,
                    view_filter_clause=view_filter_clause,
                    intermediate_table_name=intermediate_table_name.format(
                        export_view_name=view.view_id
                    ),
                    output_directory=GcsfsDirectoryPath.from_absolute_path(
                        output_directory
                    ),
                    allow_empty=self.allow_empty,
                    **optional_args,
                )
            )
        return configs


# The format for the destination of the files in the export
CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-case-triage-data"
COVID_DASHBOARD_OUTPUT_DIRECTORY_URI = "gs://{project_id}-covid-dashboard-data"
DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-dashboard-data"
DASHBOARD_USER_RESTRICTIONS_OUTPUT_DIRECTORY_URI = (
    "gs://{project_id}-dashboard-user-restrictions"
)
JUSTICE_COUNTS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-justice-counts-data"
PO_REPORT_OUTPUT_DIRECTORY_URI = "gs://{project_id}-report-data/po_monthly_report"
PUBLIC_DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-public-dashboard-data"
INGEST_METADATA_OUTPUT_DIRECTORY_URI = "gs://{project_id}-ingest-metadata"
VALIDATION_METADATA_OUTPUT_DIRECTORY_URI = "gs://{project_id}-validation-metadata"


_VIEW_COLLECTION_EXPORT_CONFIGS: List[ExportViewCollectionConfig] = [
    # PO Report views
    ExportViewCollectionConfig(
        view_builders_to_export=[PO_MONTHLY_REPORT_DATA_VIEW_BUILDER],
        output_directory_uri_template=PO_REPORT_OUTPUT_DIRECTORY_URI,
        export_name="PO_MONTHLY",
        bq_view_namespace=BigQueryViewNamespace.STATE,
    ),
    # COVID Dashboard views
    ExportViewCollectionConfig(
        view_builders_to_export=COVID_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=COVID_DASHBOARD_OUTPUT_DIRECTORY_URI,
        export_name="COVID_DASHBOARD",
        bq_view_namespace=BigQueryViewNamespace.STATE,
    ),
    # Case Triage views
    ExportViewCollectionConfig(
        view_builders_to_export=CASE_TRIAGE_EXPORTED_VIEW_BUILDERS,
        output_directory_uri_template=CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="CASE_TRIAGE",
        bq_view_namespace=BigQueryViewNamespace.CASE_TRIAGE,
        export_output_formats=[ExportOutputFormatType.HEADERLESS_CSV],
    ),
    # Ingest metadata views for admin panel
    ExportViewCollectionConfig(
        view_builders_to_export=INGEST_METADATA_BUILDERS,
        output_directory_uri_template=INGEST_METADATA_OUTPUT_DIRECTORY_URI,
        export_name="INGEST_METADATA",
        bq_view_namespace=BigQueryViewNamespace.INGEST_METADATA,
    ),
    # Validation metadata views for admin panel
    ExportViewCollectionConfig(
        view_builders_to_export=VALIDATION_METADATA_BUILDERS,
        output_directory_uri_template=VALIDATION_METADATA_OUTPUT_DIRECTORY_URI,
        export_name="VALIDATION_METADATA",
        bq_view_namespace=BigQueryViewNamespace.VALIDATION_METADATA,
    ),
    # Justice Counts views for frontend
    ExportViewCollectionConfig(
        view_builders_to_export=JUSTICE_COUNTS_VIEW_BUILDERS,
        output_directory_uri_template=JUSTICE_COUNTS_OUTPUT_DIRECTORY_URI,
        export_name="JUSTICE_COUNTS",
        bq_view_namespace=BigQueryViewNamespace.JUSTICE_COUNTS,
    ),
    # Lantern Dashboard views
    ExportViewCollectionConfig(
        view_builders_to_export=LANTERN_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="LANTERN",
        bq_view_namespace=BigQueryViewNamespace.STATE,
    ),
    # Core Dashboard views
    ExportViewCollectionConfig(
        view_builders_to_export=CORE_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="CORE",
        bq_view_namespace=BigQueryViewNamespace.STATE,
    ),
    # Public Dashboard views
    ExportViewCollectionConfig(
        view_builders_to_export=PUBLIC_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=PUBLIC_DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PUBLIC_DASHBOARD",
        bq_view_namespace=BigQueryViewNamespace.STATE,
        # Not all views have data for every state, so it is okay if some of the files
        # are empty.
        allow_empty=True,
    ),
    # Unified Product -- Vitals
    ExportViewCollectionConfig(
        view_builders_to_export=VITALS_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="VITALS",
        bq_view_namespace=BigQueryViewNamespace.STATE,
    ),
    # Lantern and Core Dashboard User Restrictions views
    ExportViewCollectionConfig(
        view_builders_to_export=DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_USER_RESTRICTIONS_OUTPUT_DIRECTORY_URI,
        export_name="DASHBOARD_USER_RESTRICTIONS",
        bq_view_namespace=BigQueryViewNamespace.STATE,
    ),
]

VIEW_COLLECTION_EXPORT_INDEX: Dict[str, ExportViewCollectionConfig] = {
    export.export_name: export for export in _VIEW_COLLECTION_EXPORT_CONFIGS
}

NAMESPACES_REQUIRING_FULL_UPDATE: List[BigQueryViewNamespace] = [
    BigQueryViewNamespace.INGEST_METADATA,
    BigQueryViewNamespace.VALIDATION_METADATA,
]
