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
from typing import Dict, List, Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.covid_dashboard.covid_dashboard_views import COVID_DASHBOARD_VIEW_BUILDERS
from recidiviz.metrics.export.metric_export_config import ExportMetricDatasetConfig
from recidiviz.calculator.query.state.dataset_config import DASHBOARD_VIEWS_DATASET, \
    COVID_REPORT_DATASET, PO_REPORT_DATASET, PUBLIC_DASHBOARD_VIEWS_DATASET, REFERENCE_VIEWS_DATASET, \
    COVID_DASHBOARD_DATASET
from recidiviz.calculator.query.state.views.covid_report.covid_report_views import COVID_REPORT_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import DASHBOARD_VIEW_BUILDERS, \
    CORE_DASHBOARD_VIEW_BUILDERS, LANTERN_DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.po_report.po_report_views import PO_REPORT_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.public_dashboard.public_dashboard_views import \
    PUBLIC_DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.reference.reference_views import REFERENCE_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.po_report.po_monthly_report_data import PO_MONTHLY_REPORT_DATA_VIEW_BUILDER


VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Dict[str, Sequence[BigQueryViewBuilder]] = {
    REFERENCE_VIEWS_DATASET: REFERENCE_VIEW_BUILDERS,
    COVID_DASHBOARD_DATASET: COVID_DASHBOARD_VIEW_BUILDERS,
    COVID_REPORT_DATASET: COVID_REPORT_VIEW_BUILDERS,
    DASHBOARD_VIEWS_DATASET: DASHBOARD_VIEW_BUILDERS,
    PO_REPORT_DATASET: PO_REPORT_VIEW_BUILDERS,
    PUBLIC_DASHBOARD_VIEWS_DATASET: PUBLIC_DASHBOARD_VIEW_BUILDERS
}

# The format for the destination of the files in the export
COVID_DASHBOARD_OUTPUT_DIRECTORY_URI = "gs://{project_id}-covid-dashboard-data"
DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-dashboard-data"
PO_REPORT_OUTPUT_DIRECTORY_URI = "gs://{project_id}-report-data/po_monthly_report"
PUBLIC_DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI = "gs://{project_id}-public-dashboard-data"


# The configurations for exporting metric views from various datasets to GCS buckets
METRIC_DATASET_EXPORT_CONFIGS: List[ExportMetricDatasetConfig] = \
    [
        # PO Report views for US_ID
        ExportMetricDatasetConfig(
            dataset_id=PO_REPORT_DATASET,
            metric_view_builders_to_export=[PO_MONTHLY_REPORT_DATA_VIEW_BUILDER],
            output_directory_uri_template=PO_REPORT_OUTPUT_DIRECTORY_URI,
            state_code_filter='US_ID'
        ),
        # Public Dashboard views for US_ND
        ExportMetricDatasetConfig(
            dataset_id=PUBLIC_DASHBOARD_VIEWS_DATASET,
            metric_view_builders_to_export=PUBLIC_DASHBOARD_VIEW_BUILDERS,
            output_directory_uri_template=PUBLIC_DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
            state_code_filter='US_ND'
        ),
        # COVID Dashboard views (not state-specific)
        ExportMetricDatasetConfig(
            dataset_id=COVID_DASHBOARD_DATASET,
            metric_view_builders_to_export=COVID_DASHBOARD_VIEW_BUILDERS,
            output_directory_uri_template=COVID_DASHBOARD_OUTPUT_DIRECTORY_URI,
            state_code_filter=None
        ),
    ] + [
        # Lantern Dashboard views for all relevant states
        ExportMetricDatasetConfig(
            dataset_id=DASHBOARD_VIEWS_DATASET,
            metric_view_builders_to_export=LANTERN_DASHBOARD_VIEW_BUILDERS,
            output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
            state_code_filter=state_code
        )
        for state_code in ['US_MO', 'US_PA']
    ] + [
        # Core Dashboard views for all relevant states
        ExportMetricDatasetConfig(
            dataset_id=DASHBOARD_VIEWS_DATASET,
            metric_view_builders_to_export=CORE_DASHBOARD_VIEW_BUILDERS,
            output_directory_uri_template=DASHBOARD_VIEWS_OUTPUT_DIRECTORY_URI,
            state_code_filter=state_code
        )
        for state_code in ['US_ND']
    ]
