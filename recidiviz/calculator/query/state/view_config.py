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

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state.dataset_config import REFERENCE_TABLES_DATASET, DASHBOARD_VIEWS_DATASET, \
    COVID_REPORT_DATASET, PO_REPORT_DATASET
from recidiviz.calculator.query.state.views.covid_report.covid_report_views import COVID_REPORT_VIEWS
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import DASHBOARD_VIEWS
from recidiviz.calculator.query.state.views.po_report.po_report_views import PO_REPORT_VIEWS
from recidiviz.calculator.query.state.views.reference.reference_views import REFERENCE_VIEWS
from recidiviz.calculator.query.state.views.reference.most_recent_job_id_by_metric_and_state_code import \
    MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW
from recidiviz.calculator.query.state.views.po_report.po_monthly_report_data import PO_MONTHLY_REPORT_DATA_VIEW


VIEWS_TO_UPDATE: Dict[str, List[BigQueryView]] = {
    REFERENCE_TABLES_DATASET: REFERENCE_VIEWS,
    COVID_REPORT_DATASET: COVID_REPORT_VIEWS,
    DASHBOARD_VIEWS_DATASET: DASHBOARD_VIEWS,
    PO_REPORT_DATASET: PO_REPORT_VIEWS
}

VIEWS_TO_MATERIALIZE_FOR_DASHBOARD_EXPORT = [
    MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW
]

# Dictionary mapping the datasets to the state codes and views that should be included in the export of that dataset.
DATASETS_STATES_AND_VIEWS_TO_EXPORT: Dict[str, Dict[str, List[BigQueryView]]] = {
    DASHBOARD_VIEWS_DATASET: {
        'US_MO': DASHBOARD_VIEWS,
        'US_ND': DASHBOARD_VIEWS
    },
    PO_REPORT_DATASET: {
        'US_ID': [PO_MONTHLY_REPORT_DATA_VIEW]
    }
}

# The format for the destination of the files in the export
OUTPUT_URI_TEMPLATE_FOR_DATASET_EXPORT: Dict[str, str] = {
    DASHBOARD_VIEWS_DATASET: "gs://{project_id}-dashboard-data/{state_code}/{view_id}.json",
    PO_REPORT_DATASET: "gs://{project_id}-report-data/po_monthly_report/{state_code}/{view_id}.json",
}
