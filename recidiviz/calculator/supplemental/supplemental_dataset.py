# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Generic Class for supplemental datasets that get generated."""
import abc
import logging
import uuid
from typing import Any, Callable, Dict, List, Optional

from google.cloud import bigquery
from more_itertools import one

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.view_update_manager import (
    rematerialize_views_for_view_builders,
)
from recidiviz.calculator.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS
from recidiviz.view_registry.deployed_views import deployed_view_builders

# TODO(#11312): Remove once dataflow pipeline is deployed.
TEMP_DATASET_ID = f"temp_{SUPPLEMENTAL_DATA_DATASET}"
QUERY_PAGE_SIZE = 50000
DEFAULT_TEMPORARY_TABLE_EXPIRATION = 4 * 60 * 60 * 1000  # 4 hours


class SupplementalDatasetTable(abc.ABC):
    """Represents a supplemental dataset table that is to be created in BigQuery."""

    def __init__(
        self,
        destination_table_id: str,
        dependent_views: List[BigQueryViewBuilder],
    ) -> None:
        self.dependent_views = dependent_views
        self.destination_dataset_id = SUPPLEMENTAL_DATA_DATASET
        self.destination_table_id = destination_table_id

    @abc.abstractmethod
    def query_from_dependent_views(self, project_id: str) -> str:
        """Generates the query string from which to query the dependent views in order
        to generate the supplemental table."""

    @abc.abstractmethod
    def process_row(self, row: bigquery.table.Row) -> Dict[str, Any]:
        """Defines custom business logic that updates the given row in place and returns
        it back in dictionary form to be loaded into BigQuery for supplemental table
        generation."""


class StateSpecificSupplementalDatasetGenerator:
    """Represents a state's specific supplemental datasets to generate."""

    def __init__(
        self, supplemental_dataset_tables: List[SupplementalDatasetTable]
    ) -> None:
        self.supplemental_dataset_tables = supplemental_dataset_tables
        self.bigquery_client = BigQueryClientImpl()

    def generate_dataset_table(
        self, table_id: str, dataset_override: Optional[str] = None
    ) -> None:
        """Generates a table for a region given a table_id and an optional dataset
        override. Because a supplemental dataset is likely to depend on certain BigQuery
        views, rematerializes those views first. Then, all supplemental tables follow
        the same process of extracting all of the rows from the dependent views, via the
        supplemental table query, to loading the processed rows into a temporary table,
        before inserting and overwriting the destination table with the updated data.
        """

        supplemental_dataset_table = one(
            [
                st
                for st in self.supplemental_dataset_tables
                if st.destination_table_id == table_id
            ]
        )

        # Rematerialize all of the dependent views for a supplemental table
        logging.info(
            "Rematerializing dependent views for %s",
            supplemental_dataset_table.destination_table_id,
        )
        rematerialize_views_for_view_builders(
            views_to_update_builders=supplemental_dataset_table.dependent_views,
            all_view_builders=deployed_view_builders(self.bigquery_client.project_id),
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
        )

        # Create a temporary dataset and a temporary table with no schema so that
        # it can be autodetected when the rows are loaded into it.
        temp_dataset_id = TEMP_DATASET_ID if not dataset_override else dataset_override
        temp_dataset_ref = self.bigquery_client.dataset_ref_for_id(temp_dataset_id)
        temp_table_id = f"temp_{table_id}_{uuid.uuid4()}"
        logging.info(
            "Creating temporary dataset: %s.%s", temp_dataset_id, temp_table_id
        )

        self.bigquery_client.create_dataset_if_necessary(
            dataset_ref=temp_dataset_ref,
            default_table_expiration_ms=DEFAULT_TEMPORARY_TABLE_EXPIRATION,
        )
        self.bigquery_client.create_table_with_schema(
            dataset_id=temp_dataset_id,
            table_id=temp_table_id,
            schema_fields=[],
        )

        logging.info("Starting to query dependent views to generate temporary table")
        query_reference_view_job = self.bigquery_client.run_query_async(
            query_str=supplemental_dataset_table.query_from_dependent_views(
                project_id=self.bigquery_client.project_id
            ),
        )
        self.bigquery_client.paged_read_and_process(
            query_job=query_reference_view_job,
            page_size=QUERY_PAGE_SIZE,
            process_page_fn=self._load_rows_to_temp_table(
                temp_dataset_ref=temp_dataset_ref,
                temp_table_id=temp_table_id,
                process_row_fn=supplemental_dataset_table.process_row,
            ),
        )

        temporary_table_final_schema = self.bigquery_client.get_table(
            dataset_ref=temp_dataset_ref,
            table_id=temp_table_id,
        ).schema

        # Create destination table if it doesn't exist, otherwise update its schema
        final_dataset_id = (
            supplemental_dataset_table.destination_dataset_id
            if not dataset_override
            else dataset_override
        )
        final_dataset_ref = self.bigquery_client.dataset_ref_for_id(final_dataset_id)
        if not self.bigquery_client.table_exists(
            dataset_ref=final_dataset_ref,
            table_id=supplemental_dataset_table.destination_table_id,
        ):
            logging.info(
                "Creating final destination table %s.%s",
                final_dataset_id,
                supplemental_dataset_table.destination_table_id,
            )
            self.bigquery_client.create_table_with_schema(
                dataset_id=final_dataset_id,
                table_id=supplemental_dataset_table.destination_table_id,
                schema_fields=temporary_table_final_schema,
            )
        else:
            logging.info(
                "Updating schema for final destination table %s.%s",
                final_dataset_id,
                supplemental_dataset_table.destination_table_id,
            )
            self.bigquery_client.update_schema(
                dataset_id=final_dataset_id,
                table_id=supplemental_dataset_table.destination_table_id,
                desired_schema_fields=temporary_table_final_schema,
            )

        # Insert into the final destination table
        logging.info(
            "Inserting data from temporary table %s.%s to final destination table %s.%s",
            temp_dataset_id,
            temp_table_id,
            final_dataset_id,
            supplemental_dataset_table.destination_table_id,
        )
        insert_job = self.bigquery_client.insert_into_table_from_table_async(
            source_dataset_id=temp_dataset_id,
            source_table_id=temp_table_id,
            destination_dataset_id=final_dataset_id,
            destination_table_id=supplemental_dataset_table.destination_table_id,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        _ = insert_job.result()

        logging.info("Deleting temporary table %s.%s", temp_dataset_id, temp_table_id)
        self.bigquery_client.delete_table(
            dataset_id=temp_dataset_id, table_id=temp_table_id
        )

    def _load_rows_to_temp_table(
        self,
        temp_dataset_ref: bigquery.DatasetReference,
        temp_table_id: str,
        process_row_fn: Callable[[bigquery.table.Row], Dict[str, Any]],
    ) -> Callable[[List[bigquery.table.Row]], None]:
        """Returns a function taht will be used to load a series of rows into the
        temporary table."""

        def _load_rows(rows: List[bigquery.table.Row]) -> None:
            load_row_job = self.bigquery_client.load_into_table_async(
                dataset_ref=temp_dataset_ref,
                table_id=temp_table_id,
                rows=[process_row_fn(row) for row in rows],
            )
            _ = load_row_job.result()

        return _load_rows
