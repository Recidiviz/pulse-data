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

# Views that rely on Dataflow metric output that are shared by views in multiple
# different packages
SHARED_METRIC_VIEWS_DATASET: str = "shared_metric_views"

# Views that do not rely on Dataflow metric output (e.g. they only read from static
# reference tables or the state dataset) that may be used as input to our Dataflow
# pipelines, or which may be referenced by other views.
REFERENCE_VIEWS_DATASET: str = "reference_views"

# Where the tables used to populate the COVID dashboard views live
COVID_DASHBOARD_REFERENCE_DATASET: str = "covid_public_data_reference_tables"

# Where the views for the public dashboard live
PUBLIC_DASHBOARD_VIEWS_DATASET: str = "public_dashboard_views"

# Where analyst datasets live
ANALYST_VIEWS_DATASET: str = "analyst_data"

# Where sessions datasets live
SESSIONS_DATASET: str = "sessions"

# Where sessions datasets live
SESSIONS_VALIDATION_DATASET: str = "sessions_validation"

# Where sentence sessions data lives
SENTENCE_SESSIONS_DATASET: str = "sentence_sessions"

# # Mirrored version of `sentence_sessions` only using v2 sentence entities
SENTENCE_SESSIONS_V2_ALL_DATASET: str = "sentence_sessions_v2_all"

# Where the population projection simulation views live
POPULATION_PROJECTION_DATASET: str = "population_projection_data"

# Where the population projection simulation output tables live
POPULATION_PROJECTION_OUTPUT_DATASET: str = "population_projection_output_data"

# Where the spark simulation output data tables live
SPARK_OUTPUT_DATASET: str = "spark_public_output_data"

# Where the PSI tools datasets live
SENTENCING_OUTPUT_DATASET: str = "sentencing_views"

# Where the Case Notes Prototype datasets live
CASE_NOTES_PROTOTYPE_DATASET: str = "case_notes_prototype_views"

# Contains views that select the most recent data for each spark simulation tag from spark_public_output_data
SPARK_OUTPUT_DATASET_MOST_RECENT: str = "spark_public_output_data_most_recent"

# Where the views for the vitals report live
VITALS_REPORT_DATASET: str = "vitals_report_views"

# Where Sendgrid datasets live
SENDGRID_EMAIL_DATA_DATASET: str = "sendgrid_email_data"

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

# Views that contain legacy events logged from Auth0 actions via Segment
AUTH0_EVENTS: str = "auth0_events"

# Views that contain aggregated metrics related to impact reports
IMPACT_REPORTS_DATASET_ID = "impact_reports"

# Views that are used to text JII
JII_TEXTING_DATASET_ID = "jii_texting"

# Views that contain observations and metrics related to org-wide transitions
TRANSITIONS_DATASET_ID = "transitions"

# Views that contain aggregated metrics about tool users
USER_METRICS_DATASET_ID = "user_metrics"
