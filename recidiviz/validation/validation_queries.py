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

"""Utilities for performing validation-related queries against BigQuery."""

from google.cloud import bigquery


DATASET = 'validation_views'
_client = None


def client() -> bigquery.Client:
    global _client
    if not _client:
        _client = bigquery.Client()
    return _client


def run_query(validation_query: str) -> bigquery.job.QueryJob:
    """Returns the result set from querying the given validation query string."""

    return client().query(validation_query)
