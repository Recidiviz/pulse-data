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
"""Class that persists runtime of successful view rematerialization jobs to BQ."""
from typing import List

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view import BigQueryViewBuilder

#  Dataset with metadata about view update operations
VIEW_UPDATE_METADATA_DATASET = "view_update_metadata"

# Table that holds info about successful rematerialization jobs
REMATERIALIZATION_TRACKER_TABLE_ID = "rematerialization_tracker"

CLOUD_TASK_ID_COL = "cloud_task_id"
NUM_DEPLOYED_VIEWS_COL = "num_deployed_views"
NUM_MATERIALIZED_VIEWS_COL = "num_materialized_views"
REMATERIAIZATION_RUNTIME_SEC_COL = "rematerializaton_runtime_sec"


class RematerializationSuccessPersister:
    """Class that persists runtime of successful view rematerialization jobs to BQ."""

    def __init__(self, bq_client: BigQueryClient) -> None:
        self.bq_client = bq_client

    def record_success_in_bq(
        self,
        deployed_view_builders: List[BigQueryViewBuilder],
        rematerialization_runtime_sec: int,
        cloud_task_id: str,
    ) -> None:
        num_deployed_views = len(deployed_view_builders)
        num_views_materialized = len(
            [v for v in deployed_view_builders if v.materialized_address]
        )

        self._create_tracker_table_if_necessary()
        self.bq_client.stream_into_table(
            self.bq_client.dataset_ref_for_id(VIEW_UPDATE_METADATA_DATASET),
            REMATERIALIZATION_TRACKER_TABLE_ID,
            [
                {
                    CLOUD_TASK_ID_COL: cloud_task_id,
                    NUM_DEPLOYED_VIEWS_COL: num_deployed_views,
                    NUM_MATERIALIZED_VIEWS_COL: num_views_materialized,
                    REMATERIAIZATION_RUNTIME_SEC_COL: rematerialization_runtime_sec,
                }
            ],
        )

    def _create_tracker_table_if_necessary(self) -> None:
        dataset_ref = self.bq_client.dataset_ref_for_id(VIEW_UPDATE_METADATA_DATASET)
        self.bq_client.create_dataset_if_necessary(dataset_ref)
        if not self.bq_client.table_exists(
            dataset_ref, REMATERIALIZATION_TRACKER_TABLE_ID
        ):
            self.bq_client.create_table_with_schema(
                dataset_ref.dataset_id,
                REMATERIALIZATION_TRACKER_TABLE_ID,
                schema_fields=[
                    bigquery.SchemaField(
                        name=CLOUD_TASK_ID_COL,
                        field_type=bigquery.enums.SqlTypeNames.STRING.value,
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name=NUM_DEPLOYED_VIEWS_COL,
                        field_type=bigquery.enums.SqlTypeNames.INT64.value,
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name=NUM_MATERIALIZED_VIEWS_COL,
                        field_type=bigquery.enums.SqlTypeNames.INT64.value,
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name=REMATERIAIZATION_RUNTIME_SEC_COL,
                        field_type=bigquery.enums.SqlTypeNames.INT64.value,
                        mode="REQUIRED",
                    ),
                ],
            )
