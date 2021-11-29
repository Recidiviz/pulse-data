# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Queue names for shared Google Cloud Task queues."""

BIGQUERY_QUEUE_V2 = "bigquery-v2"
ADMIN_PANEL_DATA_DISCOVERY_QUEUE = "admin-panel-data-discovery"

# TODO(#6455) - This queue should ideally be renamed to "direct-ingest-jpp-scheduler-v2"
DIRECT_INGEST_SCHEDULER_QUEUE_V2 = "direct-ingest-scheduler-v2"

# TODO(#6455) - This queue should ideally be renamed to "direct-ingest-jpp-bq-import-export-v2"
DIRECT_INGEST_BQ_IMPORT_EXPORT_QUEUE_V2 = "direct-ingest-bq-import-export-v2"

# TODO(#6455) - This queue is no-longer used and can be safely deleted (once this code
#  is deployed).
DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2 = "direct-ingest-state-process-job-queue-v2"

DIRECT_INGEST_JAILS_PROCESS_JOB_QUEUE_V2 = "direct-ingest-jpp-process-job-queue-v2"

JOB_MONITOR_QUEUE_V2 = "job-monitor-v2"
SCRAPER_PHASE_QUEUE_V2 = "scraper-phase-v2"
