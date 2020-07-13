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
from typing import Dict, List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_TABLES_DATASET, DASHBOARD_VIEWS_DATASET, \
    COVID_REPORT_DATASET, PO_REPORT_DATASET, PUBLIC_DASHBOARD_VIEWS_DATASET
from recidiviz.calculator.query.state.views.covid_report.covid_report_views import COVID_REPORT_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.po_report.po_report_views import PO_REPORT_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.public_dashboard.public_dashboard_views import \
    PUBLIC_DASHBOARD_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.reference.reference_views import REFERENCE_VIEW_BUILDERS
from recidiviz.calculator.query.state.views.reference.most_recent_job_id_by_metric_and_state_code import \
    MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_BUILDER
from recidiviz.calculator.query.state.views.po_report.po_monthly_report_data import PO_MONTHLY_REPORT_DATA_VIEW_BUILDER


VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Dict[str, List[BigQueryViewBuilder]] = {
    REFERENCE_TABLES_DATASET: REFERENCE_VIEW_BUILDERS,
    COVID_REPORT_DATASET: COVID_REPORT_VIEW_BUILDERS,
    DASHBOARD_VIEWS_DATASET: DASHBOARD_VIEW_BUILDERS,
    PO_REPORT_DATASET: PO_REPORT_VIEW_BUILDERS,
    PUBLIC_DASHBOARD_VIEWS_DATASET: PUBLIC_DASHBOARD_VIEW_BUILDERS
}

VIEW_BUILDERS_FOR_VIEWS_TO_MATERIALIZE_FOR_DASHBOARD_EXPORT: List[BigQueryViewBuilder] = [
    MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW_BUILDER,
    PO_MONTHLY_REPORT_DATA_VIEW_BUILDER
]

# Dictionary mapping the datasets to the state codes and view builders for the views that should be included in the
# export of that dataset.
DATASETS_STATES_AND_VIEW_BUILDERS_TO_EXPORT = {
    DASHBOARD_VIEWS_DATASET: {
        'US_MO': DASHBOARD_VIEW_BUILDERS,
        'US_ND': DASHBOARD_VIEW_BUILDERS
    },
    PO_REPORT_DATASET: {
        'US_ID': [PO_MONTHLY_REPORT_DATA_VIEW_BUILDER]
    },
    PUBLIC_DASHBOARD_VIEWS_DATASET: {
        'US_ND': PUBLIC_DASHBOARD_VIEW_BUILDERS
    }
}

# The format for the destination of the files in the export
OUTPUT_URI_TEMPLATE_FOR_DATASET_EXPORT: Dict[str, str] = {
    DASHBOARD_VIEWS_DATASET: "gs://{project_id}-dashboard-data/{state_code}/{view_id}.json",
    PO_REPORT_DATASET: "gs://{project_id}-report-data/po_monthly_report/{state_code}/{view_id}.json",
    PUBLIC_DASHBOARD_VIEWS_DATASET: "gs://{project_id}-public-dashboard-data/{state_code}/{view_id}.json"
}
