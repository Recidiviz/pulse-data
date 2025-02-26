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
"""Config classes for exporting metric views to Google Cloud Storage."""
from typing import Any, Dict, List, Optional, Sequence

import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
    ExportValidationType,
)
from recidiviz.calculator.query.justice_counts.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_EXPORT as JUSTICE_COUNTS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import (
    LANTERN_DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_views import (
    PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
    PATHWAYS_LIBERTY_TO_PRISON_VIEW_BUILDERS,
    PATHWAYS_PRISON_TO_SUPERVISION_VIEW_BUILDERS,
    PATHWAYS_PRISON_VIEW_BUILDERS,
    PATHWAYS_SUPERVISION_TO_LIBERTY_VIEW_BUILDERS,
    PATHWAYS_SUPERVISION_TO_PRISON_VIEW_BUILDERS,
    PATHWAYS_SUPERVISION_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dashboard.population_projections.population_projections_views import (
    POPULATION_PROJECTION_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_views import (
    VITALS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.impact.impact_dashboard_views import (
    IMPACT_DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.outliers.outliers_views import (
    OUTLIERS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.overdue_discharge_alert.overdue_discharge_alert_data_views import (
    OVERDUE_DISCHARGE_ALERT_DATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.po_monthly_report_data import (
    PO_MONTHLY_REPORT_DATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.public_dashboard_views import (
    PUBLIC_DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.reference.dashboard_user_restrictions import (
    DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.ingested_product_users import (
    INGESTED_PRODUCT_USERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.firestore_views import (
    FIRESTORE_VIEW_BUILDERS,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.view_config import INGEST_METADATA_BUILDERS
from recidiviz.utils import metadata
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.validation.views.view_config import VALIDATION_METADATA_BUILDERS

RemapColumns = Dict[str, Dict[Any, Any]]


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

    # Map of output formats for these configs to any validations to perform on the export.
    export_output_formats_and_validations: Optional[
        Dict[ExportOutputFormatType, List[ExportValidationType]]
    ] = attr.ib(default=None)

    # If set to True then empty files are considered valid. If False then some
    # validators may choose to mark empty files as invalid.
    allow_empty: bool = attr.ib(default=False)

    export_override_state_codes: Dict[str, str] = attr.ib(factory=dict)

    @property
    def output_directory(self) -> GcsfsDirectoryPath:
        output_directory_uri = StrictStringFormatter().format(
            self.output_directory_uri_template, project_id=metadata.project_id()
        )
        return GcsfsDirectoryPath.from_absolute_path(output_directory_uri)

    def export_configs_for_views_to_export(
        self,
        state_code_filter: Optional[str] = None,
        address_overrides: Optional[BigQueryAddressOverrides] = None,
        destination_override: Optional[str] = None,
    ) -> Sequence[ExportBigQueryViewConfig]:
        """Builds a list of ExportBigQueryViewConfig that define how all views in
        view_builders_to_export should be exported to Google Cloud Storage."""

        view_filter_clause = (
            f" WHERE state_code = '{state_code_filter}'" if state_code_filter else None
        )

        intermediate_table_name_template = "{export_name}_{dataset_id}_{view_id}_table"
        output_directory = (
            GcsfsDirectoryPath.from_absolute_path(destination_override)
            if destination_override
            else self.output_directory
        )

        remap_columns: RemapColumns = {}

        if (
            state_code_filter
            and state_code_filter in self.export_override_state_codes.keys()
        ):
            override_state_code = self.export_override_state_codes[state_code_filter]
            remap_columns = {
                "state_code": {state_code_filter: override_state_code},
            }

            intermediate_table_name_template += f"_{override_state_code}"
            output_directory = GcsfsDirectoryPath.from_dir_and_subdir(
                output_directory, override_state_code
            )
        elif state_code_filter:
            intermediate_table_name_template += f"_{state_code_filter}"
            output_directory = GcsfsDirectoryPath.from_dir_and_subdir(
                output_directory, state_code_filter
            )

        configs = []
        for vb in self.view_builders_to_export:
            view = vb.build(address_overrides=address_overrides)

            optional_args = {}
            if self.export_output_formats_and_validations is not None:
                optional_args[
                    "export_output_formats_and_validations"
                ] = self.export_output_formats_and_validations
            configs.append(
                ExportBigQueryViewConfig(
                    view=view,
                    view_filter_clause=view_filter_clause,
                    intermediate_table_name=StrictStringFormatter().format(
                        intermediate_table_name_template,
                        export_name=self.export_name,
                        dataset_id=view.dataset_id,
                        view_id=view.view_id,
                    ),
                    output_directory=output_directory,
                    remap_columns=remap_columns,
                    allow_empty=self.allow_empty,
                    **optional_args,
                )
            )
        return configs


# The format for the destination of the files in the export
CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-case-triage-data"
COVID_DASHBOARD_OUTPUT_DIRECTORY_URI = "gs://{project_id}-covid-dashboard-data"
DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-dashboard-data"
DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI = (
    "gs://{project_id}-dashboard-event-level-data"
)
DASHBOARD_USER_RESTRICTIONS_OUTPUT_DIRECTORY_URI = (
    "gs://{project_id}-dashboard-user-restrictions"
)
INGEST_METADATA_OUTPUT_DIRECTORY_URI = "gs://{project_id}-ingest-metadata"
JUSTICE_COUNTS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-justice-counts-data"
OVERDUE_DISCHARGE_ALERT_OUTPUT_DIRECTORY_URI = (
    "gs://{project_id}-report-data/overdue_discharge_alert"
)
PO_REPORT_OUTPUT_DIRECTORY_URI = "gs://{project_id}-report-data/po_monthly_report"
PRODUCT_USER_IMPORT_OUTPUT_DIRECTORY_URI = "gs://{project_id}-product-user-import"
PUBLIC_DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-public-dashboard-data"
VALIDATION_METADATA_OUTPUT_DIRECTORY_URI = "gs://{project_id}-validation-metadata"
WORKFLOWS_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-practices-etl-data"
OUTLIERS_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-outliers-etl-data"


EXPORT_ATLAS_TO_ID = {StateCode.US_IX.value: StateCode.US_ID.value}

_VIEW_COLLECTION_EXPORT_CONFIGS: List[ExportViewCollectionConfig] = [
    # PO Report views
    ExportViewCollectionConfig(
        view_builders_to_export=[PO_MONTHLY_REPORT_DATA_VIEW_BUILDER],
        output_directory_uri_template=PO_REPORT_OUTPUT_DIRECTORY_URI,
        export_name="PO_MONTHLY",
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Overdue Discharge Report views
    ExportViewCollectionConfig(
        view_builders_to_export=[OVERDUE_DISCHARGE_ALERT_DATA_VIEW_BUILDER],
        output_directory_uri_template=OVERDUE_DISCHARGE_ALERT_OUTPUT_DIRECTORY_URI,
        export_name="OVERDUE_DISCHARGE",
        # This view has no entries for US_ID in staging (as of 2022-11-03)
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Ingest metadata views for admin panel
    ExportViewCollectionConfig(
        view_builders_to_export=INGEST_METADATA_BUILDERS,
        output_directory_uri_template=INGEST_METADATA_OUTPUT_DIRECTORY_URI,
        export_name="INGEST_METADATA",
        # This collection has empty files for state employment period views in staging (as of 2022-11-09)
        allow_empty=True,
    ),
    # Validation metadata views for admin panel
    ExportViewCollectionConfig(
        view_builders_to_export=VALIDATION_METADATA_BUILDERS,
        output_directory_uri_template=VALIDATION_METADATA_OUTPUT_DIRECTORY_URI,
        export_name="VALIDATION_METADATA",
    ),
    # Justice Counts views for frontend
    ExportViewCollectionConfig(
        view_builders_to_export=JUSTICE_COUNTS_VIEW_BUILDERS,
        output_directory_uri_template=JUSTICE_COUNTS_OUTPUT_DIRECTORY_URI,
        export_name="JUSTICE_COUNTS",
    ),
    # Lantern Dashboard views
    ExportViewCollectionConfig(
        view_builders_to_export=LANTERN_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="LANTERN",
    ),
    # Public Dashboard views
    ExportViewCollectionConfig(
        view_builders_to_export=PUBLIC_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=PUBLIC_DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PUBLIC_DASHBOARD",
        # Not all views have data for every state, so it is okay if some of the files
        # are empty.
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Vitals
    ExportViewCollectionConfig(
        view_builders_to_export=VITALS_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="VITALS",
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # User Restrictions
    ExportViewCollectionConfig(
        view_builders_to_export=[DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER],
        output_directory_uri_template=DASHBOARD_USER_RESTRICTIONS_OUTPUT_DIRECTORY_URI,
        export_name="DASHBOARD_USER_RESTRICTIONS",
        export_output_formats_and_validations={
            ExportOutputFormatType.HEADERLESS_CSV: [ExportValidationType.EXISTS]
        },
    ),
    # All modules for the Pathways product
    ExportViewCollectionConfig(
        view_builders_to_export=[
            *PATHWAYS_PRISON_VIEW_BUILDERS,
            *PATHWAYS_LIBERTY_TO_PRISON_VIEW_BUILDERS,
            *PATHWAYS_PRISON_TO_SUPERVISION_VIEW_BUILDERS,
            *PATHWAYS_SUPERVISION_VIEW_BUILDERS,
            *PATHWAYS_SUPERVISION_TO_LIBERTY_VIEW_BUILDERS,
            *PATHWAYS_SUPERVISION_TO_PRISON_VIEW_BUILDERS,
        ],
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PATHWAYS",
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # All modules for the Pathways with projected prison and supervision populations
    ExportViewCollectionConfig(
        view_builders_to_export=[
            *PATHWAYS_PRISON_VIEW_BUILDERS,
            *PATHWAYS_LIBERTY_TO_PRISON_VIEW_BUILDERS,
            *PATHWAYS_PRISON_TO_SUPERVISION_VIEW_BUILDERS,
            *PATHWAYS_SUPERVISION_VIEW_BUILDERS,
            *PATHWAYS_SUPERVISION_TO_LIBERTY_VIEW_BUILDERS,
            *PATHWAYS_SUPERVISION_TO_PRISON_VIEW_BUILDERS,
            *POPULATION_PROJECTION_VIEW_BUILDERS,
        ],
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PATHWAYS_AND_PROJECTIONS",
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Pathways Prison Module
    ExportViewCollectionConfig(
        view_builders_to_export=PATHWAYS_PRISON_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PATHWAYS_PRISON",
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Pathways event level
    ExportViewCollectionConfig(
        view_builders_to_export=[*PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS],
        output_directory_uri_template=DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PATHWAYS_EVENT_LEVEL",
        export_output_formats_and_validations={
            ExportOutputFormatType.HEADERLESS_CSV_WITH_METADATA: [
                ExportValidationType.EXISTS,
                ExportValidationType.NON_EMPTY_COLUMNS_HEADERLESS,
            ]
        },
        # TODO(#15981): Remove this
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Pathways and projections event level. This is a separate export type because our export
    # infrastructure assumes an export type can only belong to one product.
    ExportViewCollectionConfig(
        view_builders_to_export=[*PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS],
        output_directory_uri_template=DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PATHWAYS_AND_PROJECTIONS_EVENT_LEVEL",
        export_output_formats_and_validations={
            ExportOutputFormatType.HEADERLESS_CSV_WITH_METADATA: [
                ExportValidationType.EXISTS,
                ExportValidationType.NON_EMPTY_COLUMNS_HEADERLESS,
            ]
        },
        # TODO(#15981): Remove this
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Pathways prison event level. This is a separate export type because our export
    # infrastructure assumes an export type can only belong to one product.
    ExportViewCollectionConfig(
        view_builders_to_export=[*PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS],
        output_directory_uri_template=DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PATHWAYS_PRISON_EVENT_LEVEL",
        export_output_formats_and_validations={
            ExportOutputFormatType.HEADERLESS_CSV_WITH_METADATA: [
                ExportValidationType.EXISTS,
                ExportValidationType.NON_EMPTY_COLUMNS_HEADERLESS,
            ]
        },
        # TODO(#15981): Remove this
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Workflows Firestore ETL views
    ExportViewCollectionConfig(
        view_builders_to_export=FIRESTORE_VIEW_BUILDERS,
        output_directory_uri_template=WORKFLOWS_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="WORKFLOWS_FIRESTORE",
        # We export all opportunities for all states, so we expect some files to be empty.
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Product users
    ExportViewCollectionConfig(
        view_builders_to_export=[INGESTED_PRODUCT_USERS_VIEW_BUILDER],
        output_directory_uri_template=PRODUCT_USER_IMPORT_OUTPUT_DIRECTORY_URI,
        export_name="PRODUCT_USER_IMPORT",
        export_output_formats_and_validations={
            ExportOutputFormatType.HEADERLESS_CSV: [ExportValidationType.EXISTS]
        },
    ),
    # Outliers views
    ExportViewCollectionConfig(
        view_builders_to_export=OUTLIERS_VIEW_BUILDERS,
        output_directory_uri_template=OUTLIERS_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="OUTLIERS",
        export_output_formats_and_validations={
            # TODO(#20729): Validate that the export is non-empty
            ExportOutputFormatType.HEADERLESS_CSV: []
        },
    ),
    # Impact Views
    ExportViewCollectionConfig(
        view_builders_to_export=IMPACT_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="IMPACT",
        export_output_formats_and_validations={
            ExportOutputFormatType.HEADERLESS_CSV: []
        },
    ),
]

VIEW_COLLECTION_EXPORT_INDEX: Dict[str, ExportViewCollectionConfig] = {
    export.export_name: export for export in _VIEW_COLLECTION_EXPORT_CONFIGS
}
