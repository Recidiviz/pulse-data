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
"""Various BigQuery datasets."""

# Where the actual, final dashboard views live
DASHBOARD_VIEWS_DATASET: str = "dashboard_views"

# Where the metrics that Dataflow jobs produce live
DATAFLOW_METRICS_DATASET: str = "dataflow_metrics"

# Where the most recent metrics that Dataflow jobs produce live
DATAFLOW_METRICS_MATERIALIZED_DATASET: str = "dataflow_metrics_materialized"

# Where static reference tables live
STATIC_REFERENCE_TABLES_DATASET: str = "static_reference_tables"

# Where reference views (views used by other views) live
REFERENCE_VIEWS_DATASET: str = "reference_views"

# Where the base tables for the state schema live
STATE_BASE_DATASET: str = "state"

# Where case triage views live
CASE_TRIAGE_DATASET: str = "case_triage"

# Where the views for the COVID dashboard live
COVID_DASHBOARD_DATASET: str = "covid_public_data"

# Where the tables used to populate the COVID dashboard views live
COVID_DASHBOARD_REFERENCE_DATASET: str = "covid_public_data_reference_tables"

# Where the views for the PO Monthly report live
PO_REPORT_DATASET: str = "po_report_views"

# Where the views for the public dashboard live
PUBLIC_DASHBOARD_VIEWS_DATASET: str = "public_dashboard_views"

# Where analyst datasets live
ANALYST_VIEWS_DATASET: str = "analyst_data"

# Where the population projection simulation views live
POPULATION_PROJECTION_DATASET: str = "population_projection_data"

# Where the population projection simulation output tables live
POPULATION_PROJECTION_OUTPUT_DATASET: str = "population_projection_output_data"

# Where the views for the vitals report live
VITALS_REPORT_DATASET: str = "vitals_report_views"
