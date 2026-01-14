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

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
    ExportValidationType,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_views import (
    PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
    PATHWAYS_SUPERVISION_TO_PRISON_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_views import (
    VITALS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.jii_texting.jii_texting_views import (
    JII_TEXTING_VIEWS_TO_EXPORT,
)
from recidiviz.calculator.query.state.views.lantern_revocations_matrix.dashboard_views.lantern_revocations_matrix_dashboard_views import (
    LANTERN_REVOCATIONS_MATRIX_DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.meetings.meetings_views import (
    MEETINGS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.outliers.outliers_views import (
    INSIGHTS_VIEW_BUILDERS_TO_EXPORT,
)
from recidiviz.calculator.query.state.views.prototypes.case_note_search.case_notes import (
    CASE_NOTES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.public_dashboard_views import (
    PUBLIC_DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.public_pathways.public_pathways_views import (
    PUBLIC_PATHWAYS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.reentry.reentry_views import (
    REENTRY_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.reference.ingested_supervision_product_users import (
    INGESTED_SUPERVISION_PRODUCT_USERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_failed_logins_monthly import (
    PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.sentencing_views import (
    SENTENCING_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.user_data_downloads.view_config import (
    USER_DATA_DOWNLOADS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.workflows.firestore.firestore_views import (
    FIRESTORE_VIEW_BUILDERS,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.view_config import collect_ingest_metadata_view_builders
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    GCP_PROJECT_DASHBOARDS_PRODUCTION,
    GCP_PROJECT_DASHBOARDS_STAGING,
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.validation.views.view_config import (
    build_validation_metadata_view_builders,
)

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

    # If set to True, the wildcard character '*' will be included in the URI path,
    # which will allow the exporter to split the export into multiple files if the
    # total export size would exceed 1 GB
    include_wildcard_in_uri: bool = attr.ib(default=False)

    # Dictionary defining, for each data platform project, which project to output
    # results to.
    output_project_by_data_project: Dict[str, str] | None = attr.ib(default=None)

    # If set to True, emit a Pub/Sub message to a topic in the project the views are
    # exported to, i.e. the output project, where the topic name has the format
    # <lowercase export_name>_export_success. there is a Terraform-managed topic that
    # already exists with that name before setting this to True for a given export.
    publish_success_pubsub_message: bool = attr.ib(default=False)

    # If set, overrides the Pub/Sub topic name that the message will be published to
    pubsub_topic_name_override: str | None = attr.ib(default=None)

    @property
    def output_directory(self) -> GcsfsDirectoryPath:
        output_directory_project_id = (
            self.output_project_by_data_project[metadata.project_id()]
            if self.output_project_by_data_project
            else metadata.project_id()
        )

        output_directory_uri = StrictStringFormatter().format(
            self.output_directory_uri_template, project_id=output_directory_project_id
        )
        return GcsfsDirectoryPath.from_absolute_path(output_directory_uri)

    @property
    def pubsub_topic_name(self) -> str | None:
        """The Pub/Sub topic name that the message will be published to if
        publish_success_pubsub_message is set to True"""

        if self.pubsub_topic_name_override:
            return self.pubsub_topic_name_override

        return (
            f"{self.export_name.lower()}_export_success"
            if self.publish_success_pubsub_message
            else None
        )

    def export_configs_for_views_to_export(
        self,
        state_code_filter: Optional[str] = None,
        view_sandbox_context: BigQueryViewSandboxContext | None = None,
        gcs_output_sandbox_subdir: Optional[str] = None,
    ) -> Sequence[ExportBigQueryViewConfig]:
        """Builds a list of ExportBigQueryViewConfig that define how all views in
        view_builders_to_export should be exported to Google Cloud Storage."""

        if (
            view_sandbox_context
            and not gcs_output_sandbox_subdir
            and metadata.project_id() != GCP_PROJECT_STAGING
        ):
            raise ValueError(
                f"Cannot set view_sandbox_context without gcs_output_sandbox_subdir "
                f"when exporting to [{metadata.project_id()}]. If we're reading from "
                f"views that are part of a sandbox, we must also output to a sandbox "
                f"location."
            )

        view_filter_clause = (
            f" WHERE state_code = '{state_code_filter}'" if state_code_filter else None
        )

        intermediate_table_name_template = "{export_name}_{dataset_id}_{view_id}_table"
        output_directory = self.output_directory

        if gcs_output_sandbox_subdir:
            output_directory = GcsfsDirectoryPath.from_dir_and_subdir(
                output_directory, f"sandbox/{gcs_output_sandbox_subdir}"
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
            if (
                (view_state_code := vb.address.state_code_for_address())
                and state_code_filter
                and view_state_code.value != state_code_filter
            ):
                continue

            view = vb.build(sandbox_context=view_sandbox_context)

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
                    include_wildcard_in_uri=self.include_wildcard_in_uri,
                    remap_columns=remap_columns,
                    allow_empty=self.allow_empty,
                    **optional_args,
                )
            )
        return configs


# The format for the destination of the files in the export
COVID_DASHBOARD_OUTPUT_DIRECTORY_URI = "gs://{project_id}-covid-dashboard-data"
DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-dashboard-data"
DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI = (
    "gs://{project_id}-dashboard-event-level-data"
)
INGEST_METADATA_OUTPUT_DIRECTORY_URI = "gs://{project_id}-ingest-metadata"
PRODUCT_USER_IMPORT_OUTPUT_DIRECTORY_URI = "gs://{project_id}-product-user-import"
PUBLIC_DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-public-dashboard-data"
VALIDATION_METADATA_OUTPUT_DIRECTORY_URI = "gs://{project_id}-validation-metadata"
WORKFLOWS_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-practices-etl-data"
OUTLIERS_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-outliers-etl-data"
INSIGHTS_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-insights-etl-data"
INSIGHTS_VIEWS_DEMO_OUTPUT_DIRECTORY_URI = "gs://{project_id}-insights-etl-data-demo"
PRODUCT_FAILED_LOGINS_MONTHLY_VIEWS_OUTPUT_DIRECTORY_URI = (
    "gs://{project_id}-product-failed-logins-monthly"
)
SENTENCING_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-sentencing-etl-data"
CASE_NOTES_VIEWS_OUTPUT_DIRECTORY_URI = (
    "gs://{project_id}-case-notes-vertex-search-data"
)
JII_TEXTING_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-jii-texting-etl-data"
REENTRY_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-reentry-etl-data"
MEETINGS_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-meetings-etl-data"
USER_DATA_DOWNLOADS_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-user-data-downloads"
PUBLIC_PATHWAYS_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-public-pathways-data"

EXPORT_ATLAS_TO_ID = {StateCode.US_IX.value: StateCode.US_ID.value}

_VIEW_COLLECTION_EXPORT_CONFIGS: List[ExportViewCollectionConfig] = [
    # Ingest metadata views for admin panel
    ExportViewCollectionConfig(
        view_builders_to_export=collect_ingest_metadata_view_builders(),
        output_directory_uri_template=INGEST_METADATA_OUTPUT_DIRECTORY_URI,
        export_name="INGEST_METADATA",
        # This collection has empty files for state employment period views in staging (as of 2022-11-09)
        allow_empty=True,
    ),
    # Validation metadata views for admin panel
    ExportViewCollectionConfig(
        view_builders_to_export=build_validation_metadata_view_builders(),
        output_directory_uri_template=VALIDATION_METADATA_OUTPUT_DIRECTORY_URI,
        export_name="VALIDATION_METADATA",
    ),
    # Lantern Dashboard views
    ExportViewCollectionConfig(
        view_builders_to_export=LANTERN_REVOCATIONS_MATRIX_DASHBOARD_VIEW_BUILDERS,
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
    # All modules for the Pathways product
    ExportViewCollectionConfig(
        view_builders_to_export=[
            *PATHWAYS_SUPERVISION_TO_PRISON_VIEW_BUILDERS,
        ],
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PATHWAYS",
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
        view_builders_to_export=[INGESTED_SUPERVISION_PRODUCT_USERS_VIEW_BUILDER],
        output_directory_uri_template=PRODUCT_USER_IMPORT_OUTPUT_DIRECTORY_URI,
        export_name="PRODUCT_USER_IMPORT",
        export_output_formats_and_validations={
            ExportOutputFormatType.HEADERLESS_CSV: [ExportValidationType.EXISTS]
        },
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Insights views
    ExportViewCollectionConfig(
        view_builders_to_export=INSIGHTS_VIEW_BUILDERS_TO_EXPORT,
        output_directory_uri_template=INSIGHTS_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="INSIGHTS",
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # Product failed logins monthly view
    ExportViewCollectionConfig(
        view_builders_to_export=[PRODUCT_FAILED_LOGINS_MONTHLY_VIEW_BUILDER],
        output_directory_uri_template=PRODUCT_FAILED_LOGINS_MONTHLY_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PRODUCT_FAILED_LOGINS",
        export_output_formats_and_validations={
            ExportOutputFormatType.HEADERLESS_CSV: []
        },
    ),
    # Sentencing views
    ExportViewCollectionConfig(
        view_builders_to_export=SENTENCING_VIEW_BUILDERS,
        output_directory_uri_template=SENTENCING_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="SENTENCING",
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
        output_project_by_data_project={
            GCP_PROJECT_STAGING: GCP_PROJECT_DASHBOARDS_STAGING,
            GCP_PROJECT_PRODUCTION: GCP_PROJECT_DASHBOARDS_PRODUCTION,
        },
        publish_success_pubsub_message=True,
    ),
    # Case notes views
    ExportViewCollectionConfig(
        view_builders_to_export=[CASE_NOTES_VIEW_BUILDER],
        output_directory_uri_template=CASE_NOTES_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="CASE_NOTES_VERTEX_SEARCH",
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
        include_wildcard_in_uri=True,
    ),
    # JII Texting views
    ExportViewCollectionConfig(
        view_builders_to_export=JII_TEXTING_VIEWS_TO_EXPORT,
        output_directory_uri_template=JII_TEXTING_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="JII_TEXTING",
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
        output_project_by_data_project={
            GCP_PROJECT_STAGING: GCP_PROJECT_DASHBOARDS_STAGING,
            GCP_PROJECT_PRODUCTION: GCP_PROJECT_DASHBOARDS_PRODUCTION,
        },
        publish_success_pubsub_message=True,
    ),
    # Reentry views
    ExportViewCollectionConfig(
        view_builders_to_export=REENTRY_VIEW_BUILDERS,
        output_directory_uri_template=REENTRY_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="REENTRY",
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
        output_project_by_data_project={
            GCP_PROJECT_STAGING: GCP_PROJECT_DASHBOARDS_STAGING,
            GCP_PROJECT_PRODUCTION: GCP_PROJECT_DASHBOARDS_PRODUCTION,
        },
        publish_success_pubsub_message=True,
    ),
    # Meeting Assistant views
    ExportViewCollectionConfig(
        view_builders_to_export=MEETINGS_VIEW_BUILDERS,
        output_directory_uri_template=MEETINGS_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="MEETINGS",
        allow_empty=True,
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
        output_project_by_data_project={
            GCP_PROJECT_STAGING: GCP_PROJECT_DASHBOARDS_STAGING,
            GCP_PROJECT_PRODUCTION: GCP_PROJECT_DASHBOARDS_PRODUCTION,
        },
        publish_success_pubsub_message=True,
    ),
    # User Data Downloads views
    ExportViewCollectionConfig(
        view_builders_to_export=USER_DATA_DOWNLOADS_VIEW_BUILDERS,
        output_directory_uri_template=USER_DATA_DOWNLOADS_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="USER_DATA_DOWNLOADS",
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
    # All modules for the Public Pathways product
    ExportViewCollectionConfig(
        view_builders_to_export=PUBLIC_PATHWAYS_VIEW_BUILDERS,
        output_directory_uri_template=PUBLIC_PATHWAYS_VIEWS_OUTPUT_DIRECTORY_URI,
        export_name="PUBLIC_PATHWAYS",
        export_override_state_codes=EXPORT_ATLAS_TO_ID,
    ),
]

VIEW_COLLECTION_EXPORT_INDEX: Dict[str, ExportViewCollectionConfig] = {
    export.export_name: export for export in _VIEW_COLLECTION_EXPORT_CONFIGS
}
