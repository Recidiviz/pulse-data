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
"""Config classes for exporting metric views to Google Cloud Storage."""
from typing import Optional, Sequence, List

import attr

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig, ExportOutputFormatType
from recidiviz.big_query.view_update_manager import BigQueryViewNamespace
from recidiviz.calculator.query.justice_counts.view_config import VIEW_BUILDERS_FOR_VIEWS_TO_EXPORT as \
    JUSTICE_COUNTS_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.covid_dashboard.covid_dashboard_views import COVID_DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import LANTERN_DASHBOARD_VIEW_BUILDERS, \
    CORE_DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.po_report.po_monthly_report_data import PO_MONTHLY_REPORT_DATA_VIEW_BUILDER
from recidiviz.calculator.query.state.views.public_dashboard.public_dashboard_views import \
    PUBLIC_DASHBOARD_VIEW_BUILDERS
from recidiviz.case_triage.views.view_config import CASE_TRIAGE_EXPORTED_VIEW_BUILDERS
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.ingest.views.view_config import INGEST_METADATA_BUILDERS


@attr.s(frozen=True)
class ExportViewCollectionConfig:
    """Stores information necessary for exporting metric data from a list of views in a dataset to a Google Cloud
    Storage Bucket."""

    # The list of views to be exported
    view_builders_to_export: Sequence[BigQueryViewBuilder] = attr.ib()

    # A string template defining the output URI path for the destination directory of the export
    output_directory_uri_template: str = attr.ib()

    # If this export should filter tables by state-specific output, this is the state_code that should be exported
    state_code_filter: Optional[str] = attr.ib()

    # The name of the export config, used to filter to exports with specific names
    export_name: str = attr.ib()

    # The category of BigQuery views of which this export belongs
    bq_view_namespace: BigQueryViewNamespace = attr.ib()

    # List of output formats for these configs
    export_output_formats: Optional[List[ExportOutputFormatType]] = attr.ib(default=None)

    def matches_filter(self, export_job_filter: Optional[str] = None) -> bool:
        if export_job_filter is None:
            return True
        if self.export_name.upper() == export_job_filter.upper():
            return True
        if self.state_code_filter and self.state_code_filter.upper() == export_job_filter.upper():
            return True
        return False

    def export_configs_for_views_to_export(self, project_id: str) -> Sequence[ExportBigQueryViewConfig]:
        """Builds a list of ExportBigQueryViewConfig that define how all views in
        view_builders_to_export should be exported to Google Cloud Storage."""
        view_filter_clause = (f" WHERE state_code = '{self.state_code_filter}'"
                              if self.state_code_filter else None)

        intermediate_table_name = "{export_view_name}_table"
        output_directory = self.output_directory_uri_template.format(
            project_id=project_id
        )

        if self.state_code_filter:
            intermediate_table_name += f"_{self.state_code_filter}"
            output_directory += f"/{self.state_code_filter}"

        configs = []
        for vb in self.view_builders_to_export:
            view = vb.build()
            optional_args = {}
            if self.export_output_formats is not None:
                optional_args['export_output_formats'] = self.export_output_formats
            configs.append(
                ExportBigQueryViewConfig(
                    view=view,
                    view_filter_clause=view_filter_clause,
                    intermediate_table_name=intermediate_table_name.format(
                        export_view_name=view.view_id
                    ),
                    output_directory=GcsfsDirectoryPath.from_absolute_path(output_directory),
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

# The configurations for exporting BigQuery views from various datasets to GCS buckets
VIEW_COLLECTION_EXPORT_CONFIGS: List[ExportViewCollectionConfig] = [
    # PO Report views for US_ID
    ExportViewCollectionConfig(
        view_builders_to_export=[PO_MONTHLY_REPORT_DATA_VIEW_BUILDER],
        output_directory_uri_template=PO_REPORT_OUTPUT_DIRECTORY_URI,
        state_code_filter='US_ID',
        export_name='PO_MONTHLY',
        bq_view_namespace=BigQueryViewNamespace.STATE,
    ),
    # Public Dashboard views for US_ND
    ExportViewCollectionConfig(
        view_builders_to_export=PUBLIC_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=PUBLIC_DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        state_code_filter='US_ND',
        export_name='PUBLIC_DASHBOARD',
        bq_view_namespace=BigQueryViewNamespace.STATE,
    ),
    # COVID Dashboard views (not state-specific)
    ExportViewCollectionConfig(
        view_builders_to_export=COVID_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=COVID_DASHBOARD_OUTPUT_DIRECTORY_URI,
        state_code_filter=None,
        export_name='COVID_DASHBOARD',
        bq_view_namespace=BigQueryViewNamespace.STATE,
    ),
    # Case Triage views for US_ID
    ExportViewCollectionConfig(
        view_builders_to_export=CASE_TRIAGE_EXPORTED_VIEW_BUILDERS,
        output_directory_uri_template=CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI,
        state_code_filter=None,
        export_name='CASE_TRIAGE',
        bq_view_namespace=BigQueryViewNamespace.CASE_TRIAGE,
        export_output_formats=[ExportOutputFormatType.HEADERLESS_CSV],
    ),
    # Ingest metadata views for admin panel
    ExportViewCollectionConfig(
        view_builders_to_export=INGEST_METADATA_BUILDERS,
        output_directory_uri_template=INGEST_METADATA_OUTPUT_DIRECTORY_URI,
        state_code_filter=None,
        export_name='INGEST_METADATA',
        bq_view_namespace=BigQueryViewNamespace.INGEST_METADATA,
    ),
    # Justice Counts views for frontend
    ExportViewCollectionConfig(
        view_builders_to_export=JUSTICE_COUNTS_VIEW_BUILDERS,
        output_directory_uri_template=JUSTICE_COUNTS_OUTPUT_DIRECTORY_URI,
        state_code_filter=None,
        export_name='JUSTICE_COUNTS',
        bq_view_namespace=BigQueryViewNamespace.JUSTICE_COUNTS,
    ),
] + [
    # Lantern Dashboard views for all relevant states
    ExportViewCollectionConfig(
        view_builders_to_export=LANTERN_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        state_code_filter=state_code,
        export_name='LANTERN',
        bq_view_namespace=BigQueryViewNamespace.STATE,
    )
    for state_code in ['US_MO', 'US_PA']
] + [
    # Core Dashboard views for all relevant states
    ExportViewCollectionConfig(
        view_builders_to_export=CORE_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        state_code_filter=state_code,
        export_name='CORE',
        bq_view_namespace=BigQueryViewNamespace.STATE,
    )
    for state_code in ['US_ND']
]

NAMESPACES_REQUIRING_FULL_UPDATE: List[BigQueryViewNamespace] = [BigQueryViewNamespace.INGEST_METADATA]
