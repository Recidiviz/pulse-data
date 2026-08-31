# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

# Queue used to run tasks that monitor whether we can start a cloud SQL refresh job and
# schedule the tasks when appropriate.
resource "google_cloud_tasks_queue" "cloud-sql-to-bq-refresh-scheduler-queue" {
  name     = "cloud-sql-to-bq-refresh-scheduler"
  location = var.us_east_region

  rate_limits {
    max_dispatches_per_second = 25
    max_concurrent_dispatches = 25
  }

  retry_config {
    max_attempts = 5
    max_backoff  = "120s"
    min_backoff  = "5s"
  }

  stackdriver_logging_config {
    sampling_ratio = 1.0
  }
}

# Queue used to process tasks that mirror the data in our CloudSQL databases to BQ.
module "cloud-sql-to-bq-refresh-queue" {
  source = "./modules/base-task-queue"

  queue_name         = "cloud-sql-to-bq-refresh"
  region             = var.us_east_region
  max_retry_attempts = 1
}

# Queue used to process tasks that update views in BQ.
module "bq-view-update-queue" {
  source = "./modules/base-task-queue"

  queue_name         = "bq-view-update"
  region             = var.us_east_region
  max_retry_attempts = 1
}

# Queue used to process tasks that export the results of metric view queries to GCS.
module "metric-view-export-queue" {
  source = "./modules/base-task-queue"

  queue_name                = "metric-view-export"
  region                    = var.us_east_region
  max_retry_attempts        = 1
  max_concurrent_dispatches = 50
}

module "case-triage-db-operations-queue" {
  source = "./modules/base-task-queue"

  queue_name                = "case-triage-db-operations-queue"
  region                    = var.us_east_region
  max_dispatches_per_second = 100
}

# Queue used to process tasks that run our validations.
module "validations-queue" {
  source = "./modules/base-task-queue"

  queue_name                = "validations"
  region                    = var.us_east_region
  max_retry_attempts        = 1
  max_concurrent_dispatches = 50
}

# Queue used to process tasks that import data into the pathways DB.
module "pathways-db-import-queue" {
  source = "./modules/base-task-queue"

  queue_name = "pathways-db-import-v2"
  region     = var.us_east_region
  # Use the default of 1 concurrent dispatch because only one SQL operation can run on an instance
  # at a time.
}

# Queue used to process tasks that import data into the pathways DB.
module "public-pathways-db-import-queue" {
  source = "./modules/base-task-queue"

  queue_name = "public-pathways-db-import-v2"
  region     = var.us_east_region
  # Use the default of 1 concurrent dispatch because only one SQL operation can run on an instance
  # at a time.
}

# Queue used for tasks to update DBs backing workflows products.
module "workflows-etl-operations-queue" {
  source = "./modules/base-task-queue"

  queue_name                = "workflows-etl-operations-queue"
  region                    = var.us_east_region
  max_dispatches_per_second = 100
}

# Queue used to trigger Typesense backfills once a Workflows Firestore ETL has written a
# collection to Firestore.
#
# Separate from workflows-etl-operations-queue on purpose. A backfill runs for minutes and
# the backfill service refuses triggers it has no capacity for, so a refused trigger has
# to be retried on a timescale that would exceed the deadline of the ETL task that
# produced the data — and retrying that ETL task to get one more attempt at the trigger
# would redundantly rewrite everything it already wrote to Firestore.
#
# Declared directly rather than through the base-task-queue module because it needs retry
# backoff bounds, which that module does not expose.
resource "google_cloud_tasks_queue" "workflows-typesense-backfill-queue" {
  name     = "workflows-typesense-backfill-queue"
  location = var.us_east_region

  rate_limits {
    # The backfill service serves 4 backfills at once (Cloud Run maxScale 4, and a CFv2
    # function serves one request per instance) and rejects the rest outright. Overlapping
    # runs are safe there: imports are upserts, and the prune re-confirms each delete
    # candidate against Firestore after the export, so a document one run is mid-import on
    # is not deleted by another.
    #
    # Dispatch 2 of those 4 slots. A backfill that outlives the read timeout of the
    # request that started it keeps running on its instance after we have given up, so the
    # retry of that trigger — and the search project's own manual and scheduled runs —
    # still find a free instance instead of a 429. Keep this below that service's max
    # instance count.
    max_concurrent_dispatches = 2
    max_dispatches_per_second = 1
  }

  retry_config {
    max_attempts = 5
    # A backfill occupies one of the service's instances for minutes, so a trigger refused
    # because they were all busy is only worth retrying on that timescale. Cloud Tasks'
    # 100ms default would spend every attempt inside the run that caused the refusal.
    min_backoff = "60s"
    max_backoff = "600s"
  }

  stackdriver_logging_config {
    sampling_ratio = 1.0
  }
}

# Queue used for tasks to make external system requests related to Workflows
module "workflows-external-system-requests-queue" {
  source = "./modules/base-task-queue"

  queue_name                = "workflows-external-system-requests-queue"
  region                    = var.us_east_region
  max_dispatches_per_second = 100
  max_retry_attempts        = 1
}

# Queue used to process tasks that import data into the outliers DB.
module "outliers-db-import-queue" {
  source = "./modules/base-task-queue"

  queue_name = "outliers-db-import-v2"
  region     = var.us_east_region
  # Use the default of 1 concurrent dispatch because only one SQL operation can run on an instance
  # at a time.
}
