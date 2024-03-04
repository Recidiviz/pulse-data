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
from typing import Optional

# Where the actual, final dashboard views live
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

DASHBOARD_VIEWS_DATASET: str = "dashboard_views"

# Where the metrics that Dataflow jobs produce live
DATAFLOW_METRICS_DATASET: str = "dataflow_metrics"

# Where the most recent metrics that Dataflow jobs produce live
DATAFLOW_METRICS_MATERIALIZED_DATASET: str = "dataflow_metrics_materialized"

# Where the metrics that the impact dashboard uses live
IMPACT_DASHBOARD_DATASET: str = "impact_dashboard"

# Where static reference tables live
STATIC_REFERENCE_TABLES_DATASET: str = "static_reference_tables"

# Views that rely on Dataflow metric output that are shared by views in multiple
# different packages
SHARED_METRIC_VIEWS_DATASET: str = "shared_metric_views"

# Views that do not rely on Dataflow metric output (e.g. they only read from static
# reference tables or the state dataset) that may be used as input to our Dataflow
# pipelines, or which may be referenced by other views.
REFERENCE_VIEWS_DATASET: str = "reference_views"

# Views that are the union of the output from each state's PRIMARY Dataflow ingest
# pipeline.
STATE_BASE_VIEWS_DATASET: str = "state_views"

# The tables for the state schema, including output from each state's PRIMARY Dataflow
# ingest pipeline.
STATE_BASE_DATASET: str = "state"

# Where the normalized state tables live, with data from all states. For each entity
# that is not normalized, these are a copy of the corresponding table in the `state`
# dataset. For each entity that is normalized, the entity table contains the normalized
# output for that entity in each state.
NORMALIZED_STATE_DATASET: str = "normalized_state"

# Where the tables used to populate the COVID dashboard views live
COVID_DASHBOARD_REFERENCE_DATASET: str = "covid_public_data_reference_tables"

# Where the views for the public dashboard live
PUBLIC_DASHBOARD_VIEWS_DATASET: str = "public_dashboard_views"

# Where analyst datasets live
ANALYST_VIEWS_DATASET: str = "analyst_data"

# Where sessions datasets live
SESSIONS_DATASET: str = "sessions"

# Where the population projection simulation views live
POPULATION_PROJECTION_DATASET: str = "population_projection_data"

# Where the population projection simulation output tables live
POPULATION_PROJECTION_OUTPUT_DATASET: str = "population_projection_output_data"

# Where the spark simulation output data tables live
SPARK_OUTPUT_DATASET: str = "spark_public_output_data"

# Contains views that select the most recent data for each spark simulation tag from spark_public_output_data
SPARK_OUTPUT_DATASET_MOST_RECENT: str = "spark_public_output_data_most_recent"

# Where the views for the vitals report live
VITALS_REPORT_DATASET: str = "vitals_report_views"

# Where Sendgrid datasets live
SENDGRID_EMAIL_DATA_DATASET: str = "sendgrid_email_data"

# TODO(#20930): Delete these US_XX_RAW_DATA constants in favor of calls to
#  raw_latest_views_dataset_for_region().
# Where US_TN raw data lives
US_TN_RAW_DATASET: str = "us_tn_raw_data_up_to_date_views"

# Where US_PA raw data lives
US_PA_RAW_DATASET: str = "us_pa_raw_data_up_to_date_views"

# Where US_CA raw data lives
US_CA_RAW_DATASET: str = "us_ca_raw_data_up_to_date_views"

# Views that power the workflows part of the state dashboard
WORKFLOWS_VIEWS_DATASET: str = "workflows_views"

EXPORT_ARCHIVES_DATASET = "export_archives"

PULSE_DASHBOARD_SEGMENT_DATASET = "pulse_dashboard_segment_metrics"

# Views that are based on the static data in the `external_reference` dataset.
EXTERNAL_REFERENCE_VIEWS_DATASET = "external_reference_views"

# Views that power outliers
OUTLIERS_VIEWS_DATASET: str = "outliers_views"

# Views that contain events logged from Auth0 actions via Segment
AUTH0_PROD_ACTION_LOGS: str = "auth0_prod_action_logs"


def normalized_state_dataset_for_state_code(state_code: StateCode) -> str:
    """Where the output of state-specific entity normalization pipelines is stored."""
    return f"{state_code.value.lower()}_{NORMALIZED_STATE_DATASET}"


def state_dataset_for_state_code(
    state_code: StateCode,
    instance: DirectIngestInstance,
    sandbox_dataset_prefix: Optional[str] = None,
) -> str:
    """Where the output of the state-specific ingest pipelines is stored."""
    dataset_prefix = f"{sandbox_dataset_prefix}_" if sandbox_dataset_prefix else ""
    return f"{dataset_prefix}{state_code.value.lower()}_state_{instance.value.lower()}"
