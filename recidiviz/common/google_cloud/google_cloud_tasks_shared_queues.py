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

CLOUD_SQL_TO_BQ_REFRESH_QUEUE = "cloud-sql-to-bq-refresh"
CLOUD_SQL_TO_BQ_REFRESH_SCHEDULER_QUEUE = "cloud-sql-to-bq-refresh-scheduler"

METRIC_VIEW_EXPORT_QUEUE = "metric-view-export"
