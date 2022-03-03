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
from recidiviz.common.constants.states import StateCode

DASHBOARD_VIEWS_DATASET: str = "dashboard_views"

# Where the metrics that Dataflow jobs produce live
DATAFLOW_METRICS_DATASET: str = "dataflow_metrics"

# Where the most recent metrics that Dataflow jobs produce live
DATAFLOW_METRICS_MATERIALIZED_DATASET: str = "dataflow_metrics_materialized"

# Where static reference tables live
STATIC_REFERENCE_TABLES_DATASET: str = "static_reference_tables"

# Views that rely on Dataflow metric output that are shared by views in multiple
# different packages
SHARED_METRIC_VIEWS_DATASET: str = "shared_metric_views"

# Views that do not rely on Dataflow metric output (e.g. they only read from static
# reference tables or the state dataset) that may be used as input to our Dataflow
# pipelines, or which may be referenced by other views.
REFERENCE_VIEWS_DATASET: str = "reference_views"

# Transitional dataset in the same region (e.g. us-east1) as the State CloudSQL
# instance where State CloudSQL data is stored before the CloudSQL -> BQ refresh
# copies it to a dataset in the 'US' multi-region.
STATE_BASE_REGIONAL_DATASET: str = "state_regional"

# Where the base tables for the state schema live. These are a mirror of the data in our
# state CloudSQL instance, refreshed daily via the CloudSQL -> BQ federated export.
STATE_BASE_DATASET: str = "state"

# Where the views for the COVID dashboard live
COVID_DASHBOARD_DATASET: str = "covid_public_data"

# Where the tables used to populate the COVID dashboard views live
COVID_DASHBOARD_REFERENCE_DATASET: str = "covid_public_data_reference_tables"

# Where the views for the Overdue Discharge alert live
OVERDUE_DISCHARGE_ALERT_DATASET: str = "overdue_discharge_alert"

# Where the views for the PO Monthly report live
PO_REPORT_DATASET: str = "po_report_views"

LINESTAFF_DATA_VALIDATION: str = "linestaff_data_validation"

# Where the views for the public dashboard live
PUBLIC_DASHBOARD_VIEWS_DATASET: str = "public_dashboard_views"

# Where analyst datasets live
ANALYST_VIEWS_DATASET: str = "analyst_data"

# Where analyst dataset scratch space views live
ANALYST_DATA_SCRATCH_SPACE_DATASET: str = "analyst_data_scratch_space"

# Where sessions datasets live
SESSIONS_DATASET: str = "sessions"

# Where the population projection simulation views live
POPULATION_PROJECTION_DATASET: str = "population_projection_data"

# Where the population projection simulation output tables live
POPULATION_PROJECTION_OUTPUT_DATASET: str = "population_projection_output_data"

# Where the views for the vitals report live
VITALS_REPORT_DATASET: str = "vitals_report_views"

# Where Sendgrid datasets live
SENDGRID_EMAIL_DATA_DATASET: str = "sendgrid_email_data"

# Where US_TN raw data lives
US_TN_RAW_DATASET: str = "us_tn_raw_data_up_to_date_views"


def normalized_state_dataset_for_state_code(state_code: StateCode) -> str:
    """Where the output of state-specific entity normalization pipelines is stored."""
    return f"{state_code.value.lower()}_normalized_state"
