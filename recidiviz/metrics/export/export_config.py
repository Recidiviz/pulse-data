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
from typing import Optional, Sequence, List, Dict, Set

import attr

from recidiviz.metrics import export as export_module
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
)
from recidiviz.big_query.view_update_manager import BigQueryViewNamespace
from recidiviz.calculator.query.justice_counts.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_EXPORT as JUSTICE_COUNTS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.covid_dashboard.covid_dashboard_views import (
    COVID_DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import (
    LANTERN_DASHBOARD_VIEW_BUILDERS,
    CORE_DASHBOARD_VIEW_BUILDERS,
    UP_DASHBOARD_VIEW_BUILDERS,
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
    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # List of export names required for this product
    exports: List[str] = attr.ib(validator=attr_validators.is_list)
    # Product launch environment, only for state agnostic products
    environment: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)
    # List of relevant state status information for a product. Empty list if product is
    # state-agnostic.
    states: List[ProductStateConfig] = attr.ib(validator=attr_validators.is_list)

    def __attrs_post_init__(self) -> None:
        if self.states and self.environment is not None:
            raise ValueError(
                f"Product with name: [{self.name}] has relevant state statuses."
                f" Environment information should be set at the state level, not the"
                f" product level."
            )

    @property
    def launched_states(self) -> Set[str]:
        return {
            state.state_code.upper()
            for state in self.states
            if state.is_state_launched_in_env()
        }

    @classmethod
    def product_configs_from_file(cls, path: str) -> List["ProductConfig"]:
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
                ],
                environment=product.get("environment"),
            )
            for product in product_config
        ]
        return products


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
                    **optional_args,
                )
            )
        return configs


# The format for the destination of the files in the export
CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-case-triage-data"
COVID_DASHBOARD_OUTPUT_DIRECTORY_URI = "gs://{project_id}-covid-dashboard-data"
DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-dashboard-data"
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
    ),
    # Unified Product views
    ExportViewCollectionConfig(
        view_builders_to_export=UP_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="UP",
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
