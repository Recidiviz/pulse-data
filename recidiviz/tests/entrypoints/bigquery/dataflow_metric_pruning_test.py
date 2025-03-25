# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests the DataflowMetricPruningEntrypoint."""
import datetime

from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized import (
    most_recent_dataflow_metrics,
)
from recidiviz.entrypoints.bigquery.dataflow_metric_pruning_entrypoint import (
    DataflowMetricPruningEntrypoint,
)
from recidiviz.source_tables.dataflow_output_table_collector import (
    get_dataflow_output_source_table_collections,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

MOST_RECENT_DATAFLOW_METRICS_MODULE = most_recent_dataflow_metrics.__name__


def _fake_fields(fields: list[SchemaField], overrides: dict) -> dict:
    values = {}
    for field in fields:
        if field.name in overrides:
            values[field.name] = overrides[field.name]
        elif field.field_type == "STRING":
            values[field.name] = "string"
        elif field.field_type == "INTEGER":
            values[field.name] = 1
        elif field.field_type == "FLOAT":
            values[field.name] = 1.0
        elif field.field_type == "DATE":
            values[field.name] = datetime.date.today().isoformat()
    return values


_TABLE_TO_DECOMMISSION_ID = "table_to_decommission"


class DataflowMetricPruningEntrypointTest(BigQueryEmulatorTestCase):
    """Tests the DataflowMetricPruningEntrypoint."""

    wipe_emulator_data_on_teardown = False

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        dataflow_source_table_collection = (
            get_dataflow_output_source_table_collections()[0]
        )

        # This table should be decommissioned
        dataflow_source_table_collection.add_source_table(
            _TABLE_TO_DECOMMISSION_ID,
            [
                SchemaField("state_code", "STRING", "REQUIRED"),
            ],
        )

        return [
            dataflow_source_table_collection,
            SourceTableCollection(
                dataset_id="sessions",
                description="Sessions, needed by recidiviz.calculator.query.state.views.dataflow_metrics_materialized.make_most_recent_metric_view_builders",
                update_config=SourceTableCollectionUpdateConfig.protected(),
                source_tables_by_address={
                    source_table.address: source_table
                    for source_table in [
                        SourceTableConfig(
                            address=BigQueryAddress(
                                dataset_id="sessions",
                                table_id="person_demographics_materialized",
                            ),
                            description="Person Demographics",
                            schema_fields=[
                                SchemaField("state_code", "STRING", "REQUIRED"),
                                SchemaField("person_id", "INT64", "REQUIRED"),
                                SchemaField(
                                    "prioritized_race_or_ethnicity",
                                    "STRING",
                                    "REQUIRED",
                                ),
                            ],
                        ),
                    ]
                },
            ),
            SourceTableCollection(
                dataset_id="reference_views",
                description="Reference views, needed by recidiviz.calculator.query.state.views.dataflow_metrics_materialized.make_most_recent_metric_view_builders",
                update_config=SourceTableCollectionUpdateConfig.protected(),
                source_tables_by_address={
                    source_table.address: source_table
                    for source_table in [
                        SourceTableConfig(
                            address=BigQueryAddress(
                                dataset_id="reference_views",
                                table_id="supervision_location_ids_to_names_materialized",
                            ),
                            description="Person Demographics",
                            schema_fields=[
                                SchemaField("state_code", "STRING", "REQUIRED"),
                                SchemaField(
                                    "level_1_supervision_location_external_id",
                                    "STRING",
                                    "REQUIRED",
                                ),
                                SchemaField(
                                    "level_2_supervision_location_external_id",
                                    "STRING",
                                    "REQUIRED",
                                ),
                            ],
                        ),
                    ]
                },
            ),
        ]

    def test_entrypoint(
        self,
    ) -> None:
        dataflow_collection = get_dataflow_output_source_table_collections()[0]

        for table in dataflow_collection.source_tables:
            self.load_rows_into_table(
                table.address,
                [
                    _fake_fields(
                        table.schema_fields,
                        {"job_id": "2025-02-01", "created_on": "2025-02-01"},
                    ),
                    _fake_fields(
                        table.schema_fields,
                        {"job_id": "2025-01-01", "created_on": "2025-01-01"},
                    ),
                    _fake_fields(
                        table.schema_fields,
                        {"job_id": "2024-12-01", "created_on": "2024-12-01"},
                    ),
                ],
            )

        # Pre-condition that the soon-to-be decommissioned table exists
        decommission_address = BigQueryAddress(
            dataset_id=dataflow_collection.dataset_id,
            table_id=_TABLE_TO_DECOMMISSION_ID,
        )
        self.bq_client.get_table(decommission_address)

        # Act
        args = DataflowMetricPruningEntrypoint.get_parser().parse_args([])
        DataflowMetricPruningEntrypoint.run_entrypoint(args)

        for table in dataflow_collection.source_tables:
            # The main table contains rows for the latest two runs
            self.assertEqual(
                [
                    row["job_id"]
                    for row in self.bq_client.run_query_async(
                        query_str=f"SELECT DISTINCT job_id FROM {table.address.to_str()}",
                        use_query_cache=False,
                    ).result()
                ],
                ["2025-01-01", "2025-02-01"],
            )

        # This table no longer exists in DATAFLOW_TABLES_TO_METRIC_TYPES, so it gets deleted
        with self.assertRaises(NotFound):
            self.bq_client.get_table(decommission_address)
