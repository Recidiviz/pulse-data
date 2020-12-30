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
"""Organizes the views into which BigQuery dataset they belong to, and which ones should be included in the exports to
cloud storage."""
import itertools
from typing import List, Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.covid_dashboard.covid_dashboard_views import COVID_DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.covid_report.covid_report_views import COVID_REPORT_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import DASHBOARD_VIEW_BUILDERS, \
    CORE_DASHBOARD_VIEW_BUILDERS, LANTERN_DASHBOARD_VIEW_BUILDERS, LANTERN_VIEW_BUILDERS_EXCLUDED_FROM_EXPORT
from recidiviz.calculator.query.state.views.po_report.po_report_views import PO_REPORT_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.public_dashboard.public_dashboard_views import \
    PUBLIC_DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.reference.reference_views import REFERENCE_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.po_report.po_monthly_report_data import PO_MONTHLY_REPORT_DATA_VIEW_BUILDER
from recidiviz.calculator.query.state.views.analyst_data.analyst_data_views import ANALYST_DATA_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.population_projection.population_projection_views import \
    POPULATION_PROJECTION_VIEW_BUILDERS
from recidiviz.ingest.views.view_config import INGEST_METADATA_BUILDERS
from recidiviz.metrics.export.export_config import ExportViewCollectionConfig


VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[BigQueryViewBuilder] = list(itertools.chain.from_iterable((
    REFERENCE_VIEW_BUILDERS,
    COVID_DASHBOARD_VIEW_BUILDERS,
    COVID_REPORT_VIEW_BUILDERS,
    DASHBOARD_VIEW_BUILDERS,
    PO_REPORT_VIEW_BUILDERS,
    PUBLIC_DASHBOARD_VIEW_BUILDERS,
    ANALYST_DATA_VIEW_BUILDERS,
    POPULATION_PROJECTION_VIEW_BUILDERS,
)))

# The format for the destination of the files in the export
COVID_DASHBOARD_OUTPUT_DIRECTORY_URI = "gs://{project_id}-covid-dashboard-data"
DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-dashboard-data"
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
        export_name='PO_MONTHLY'
    ),
    # Public Dashboard views for US_ND
    ExportViewCollectionConfig(
        view_builders_to_export=PUBLIC_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=PUBLIC_DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        state_code_filter='US_ND',
        export_name='PUBLIC_DASHBOARD'
    ),
    # COVID Dashboard views (not state-specific)
    ExportViewCollectionConfig(
        view_builders_to_export=COVID_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=COVID_DASHBOARD_OUTPUT_DIRECTORY_URI,
        state_code_filter=None,
        export_name='COVID_DASHBOARD'
    ),
    # Ingest metadata views for admin panel
    ExportViewCollectionConfig(
        view_builders_to_export=INGEST_METADATA_BUILDERS,
        output_directory_uri_template=INGEST_METADATA_OUTPUT_DIRECTORY_URI,
        state_code_filter=None,
        export_name='INGEST_METADATA',
    ),
] + [
    # Lantern Dashboard views for all relevant states
    ExportViewCollectionConfig(
        view_builders_to_export=[b for b in LANTERN_DASHBOARD_VIEW_BUILDERS
                                 if b not in LANTERN_VIEW_BUILDERS_EXCLUDED_FROM_EXPORT],
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        state_code_filter=state_code,
        export_name='LANTERN'
    )
    for state_code in ['US_MO', 'US_PA']
] + [
    # Core Dashboard views for all relevant states
    ExportViewCollectionConfig(
        view_builders_to_export=CORE_DASHBOARD_VIEW_BUILDERS,
        output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
        state_code_filter=state_code,
        export_name='CORE',
    )
    for state_code in ['US_ND']
]
