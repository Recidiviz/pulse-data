# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Classes for raw table validations."""
from typing import List

import attr
from google.api_core import retry

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_utils import bq_query_job_result_to_list_of_row_dicts
from recidiviz.cloud_resources.platform_resource_labels import (
    RawDataImportStepResourceLabel,
)
from recidiviz.common.retry_predicate import rate_limit_retry_predicate

DEFAULT_INITIAL_DELAY = 15.0  # 15 seconds
DEFAULT_MAXIMUM_DELAY = 60.0 * 2  # 2 minutes, in seconds
DEFAULT_TOTAL_TIMEOUT = 60.0 * 8  # 8 minutes, in seconds


@attr.define(auto_attribs=True)
class RawDataImportBlockingValidationQueryRunner:
    """Class to represent a query for raw data import blocking validation."""

    rate_limit_retry_policy = retry.Retry(
        initial=DEFAULT_INITIAL_DELAY,
        maximum=DEFAULT_MAXIMUM_DELAY,
        timeout=DEFAULT_TOTAL_TIMEOUT,
        predicate=rate_limit_retry_predicate,
    )
    bq_client: BigQueryClient

    @classmethod
    def _run_query(cls, client: BigQueryClient, query: str) -> List[dict]:
        """Run a query against BigQuery and return the results."""
        job = client.run_query_async(
            query_str=query,
            use_query_cache=True,
            job_labels=[
                RawDataImportStepResourceLabel.RAW_DATA_PRE_IMPORT_VALIDATIONS.value
            ],
        )
        result = job.result(retry=cls.rate_limit_retry_policy)

        return bq_query_job_result_to_list_of_row_dicts(result)

    def run_query(self, query: str) -> List[dict]:
        """Run a query against BigQuery and return the results."""
        return self._run_query(self.bq_client, query)

    def run_query_in_region(self, query: str, region: str) -> List[dict]:
        """Run a query against BigQuery in a specific region and return the results."""
        client = BigQueryClientImpl(region_override=region)
        return self._run_query(client, query)
