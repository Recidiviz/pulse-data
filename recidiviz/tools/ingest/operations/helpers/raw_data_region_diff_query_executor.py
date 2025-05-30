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
"""Executes raw data comparison queries for the given file tags in a region."""
import logging
from typing import Dict, List, Optional

import attr
from google.cloud import exceptions
from google.cloud.bigquery import QueryJob

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_utils import bq_query_job_result_to_list_of_row_dicts
from recidiviz.tools.ingest.operations.helpers.raw_table_diff_query_generator import (
    RawTableDiffQueryGenerator,
    RawTableDiffQueryResult,
)


@attr.define
class RawDataRegionQueryResult:
    """Result of running raw data comparison queries for a region.
    succeeded_tables: List of file tags for which the diff query returned no results.
    failed_table_results: Dict mapping file tags to the results of the diff query."""

    succeeded_tables: List[str]
    failed_table_results: Dict[str, RawTableDiffQueryResult]


@attr.define
class RawDataRegionDiffQueryExecutor:
    """Executes raw data comparison queries for the given file tags in a region.
    If no file tags are provided, all file tags in the region are used."""

    region_code: str = attr.ib()
    query_generator: RawTableDiffQueryGenerator = attr.ib()
    file_tags: List[str] = attr.ib()

    bq_client: BigQueryClientImpl = attr.ib()

    save_to_table: bool = attr.ib()
    dataset_id: Optional[str] = attr.ib()
    table_name_prefix: Optional[str] = attr.ib()

    @classmethod
    def build(
        cls,
        query_generator: RawTableDiffQueryGenerator,
        region_code: str,
        project_id: str,
        file_tags: List[str],
        save_to_table: bool = False,
        dataset_id: Optional[str] = None,
        table_name_prefix: Optional[str] = None,
    ) -> "RawDataRegionDiffQueryExecutor":

        if save_to_table and (not dataset_id or not table_name_prefix):
            raise ValueError(
                "Dataset ID and table name prefix must be provided when saving to a table"
            )
        return cls(
            region_code=region_code,
            query_generator=query_generator,
            bq_client=BigQueryClientImpl(project_id=project_id),
            file_tags=file_tags,
            save_to_table=save_to_table,
            dataset_id=dataset_id,
            table_name_prefix=table_name_prefix,
        )

    def _get_table_address(self, file_tag: str) -> BigQueryAddress:
        if not self.dataset_id or not self.table_name_prefix:
            raise ValueError(
                "Dataset ID and table name prefix must be provided when saving to a table"
            )
        return BigQueryAddress(
            dataset_id=self.dataset_id,
            table_id=f"{self.table_name_prefix}{file_tag}",
        )

    def _run_queries_async(self) -> Dict[str, QueryJob]:
        """Run queries asynchronously for all relevant file tags."""
        query_jobs = {}

        for file_tag in self.file_tags:
            query_str = self.query_generator.generate_query(file_tag)

            query_job = (
                self.bq_client.create_table_from_query_async(
                    address=self._get_table_address(file_tag),
                    query=query_str,
                    use_query_cache=False,
                )
                if self.save_to_table
                else self.bq_client.run_query_async(
                    query_str=query_str,
                    use_query_cache=False,
                )
            )

            query_jobs[file_tag] = query_job

        return query_jobs

    def _get_queries_results(
        self, query_jobs: Dict[str, QueryJob]
    ) -> RawDataRegionQueryResult:
        failed_table_results: Dict[str, RawTableDiffQueryResult] = {}
        succeeded_tables: List[str] = []

        for file_tag in sorted(query_jobs.keys()):
            query_job = query_jobs[file_tag]
            try:
                result = query_job.result()
            except exceptions.NotFound:
                logging.warning(
                    "Missing table %s",
                    file_tag,
                )
                continue

            results_list = bq_query_job_result_to_list_of_row_dicts(result)
            if not results_list:
                succeeded_tables.append(file_tag)
                if self.save_to_table:
                    self.bq_client.delete_table(self._get_table_address(file_tag))
            else:
                failed_table_results[
                    file_tag
                ] = self.query_generator.parse_query_result(results_list)

        return RawDataRegionQueryResult(succeeded_tables, failed_table_results)

    def run_queries(self) -> RawDataRegionQueryResult:
        """Run queries for all relevant file tags."""
        logging.info("Running queries for file tags: %s", self.file_tags)

        query_jobs = self._run_queries_async()

        return self._get_queries_results(query_jobs)
