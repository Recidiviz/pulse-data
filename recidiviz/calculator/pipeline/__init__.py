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
"""Pipeline-based calculations."""
import logging

from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery

def _start_job(self: BigQueryWrapper, request: bigquery.BigqueryJobsInsertRequest) -> bigquery.Job:
    """Inserts a BigQuery job.

    Raises an error if the insert fails, even if the job already exists.
    """
    logging.info("Using patched _start_job")
    try:
        response: bigquery.Job = self.client.jobs.Insert(request)
        logging.info(
            "Started BigQuery job: %s\n "
            "bq show -j --format=prettyjson --project_id=%s %s",
            response.jobReference,
            response.jobReference.projectId,
            response.jobReference.jobId)
        return response
    except Exception as exn:
        logging.error(
            "Failed to insert job %s: %s", request.job.jobReference, exn)
        raise

# Replaces the default implementation of _start_job with one that raises all errors.
# The default implementation suppresses 409s which can cause our dataflow pipelines to silently omit data. See
# #5152, as well as the default implementation:
# https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/gcp/bigquery_tools.py#L440
logging.info('Patching BigQueryWrapper._start_job')
setattr(BigQueryWrapper, "_start_job", _start_job)
