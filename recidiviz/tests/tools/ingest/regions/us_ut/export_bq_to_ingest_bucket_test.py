# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for export_bq_to_ingest_bucket.py"""

import datetime
from unittest.mock import MagicMock, patch

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tools.ingest.regions.us_ut.bq_sync_constants import (
    US_UT_BQ_EXPORT_TRACKER,
)
from recidiviz.tools.ingest.regions.us_ut.export_bq_to_ingest_bucket import (
    BigQueryToIngestBucketExportManager,
)


class ExportBqToIngestBucketTest(BigQueryEmulatorTestCase):
    """Tests for export_bq_to_ingest_bucket.py"""

    def setUp(self) -> None:
        super().setUp()
        # Create the export tracker table with schema matching Terraform definition
        self.export_tracker_address = ProjectSpecificBigQueryAddress(
            project_id=self.project_id,
            dataset_id=US_UT_BQ_EXPORT_TRACKER.dataset_id,
            table_id=US_UT_BQ_EXPORT_TRACKER.table_id,
        )
        self.create_mock_table(
            address=self.export_tracker_address.to_project_agnostic_address(),
            schema=[
                bigquery.SchemaField("table_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("update_datetime", "DATETIME", mode="REQUIRED"),
                bigquery.SchemaField(
                    "destination_project_id", "STRING", mode="REQUIRED"
                ),
                bigquery.SchemaField("destination_instance", "STRING", mode="REQUIRED"),
                bigquery.SchemaField(
                    "export_start_datetime", "DATETIME", mode="REQUIRED"
                ),
                bigquery.SchemaField("export_status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("row_write_datetime", "DATETIME", mode="REQUIRED"),
            ],
        )

        # Patch US_UT_BQ_EXPORT_TRACKER to use the emulator's project ID
        self.export_tracker_patcher = patch(
            "recidiviz.tools.ingest.regions.us_ut.export_bq_to_ingest_bucket.US_UT_BQ_EXPORT_TRACKER",
            self.export_tracker_address,
        )
        self.export_tracker_patcher.start()

    def tearDown(self) -> None:
        self.export_tracker_patcher.stop()
        super().tearDown()

    def test_write_export_records_to_bq_successful(self) -> None:
        export_manager = BigQueryToIngestBucketExportManager(
            address_to_update_datetime={
                BigQueryAddress(
                    dataset_id="test_dataset", table_id="table_1"
                ): datetime.datetime(2026, 1, 15, 10, 30, 0, tzinfo=datetime.UTC),
                BigQueryAddress(
                    dataset_id="test_dataset", table_id="table_2"
                ): datetime.datetime(2026, 1, 15, 10, 35, 0, tzinfo=datetime.UTC),
            },
            state_code=StateCode.US_UT,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        with patch(
            "recidiviz.tools.ingest.regions.us_ut.export_bq_to_ingest_bucket.BigQueryTableToRawDataFileTagExportManager"
        ):
            export_manager.export(bq_client=self.bq_client, dry_run=False)

        results = self.query(
            f"SELECT * FROM `{self.export_tracker_address.to_str()}` ORDER BY table_id, row_write_datetime"
        )
        records = results.to_dict("records")

        # We expect 4 rows: 2 tables * 2 status updates (STARTED, then COMPLETED)
        self.assertEqual(4, len(records))

        self.assertEqual("table_1", records[0]["table_id"])
        self.assertEqual(self.project_id, records[0]["destination_project_id"])
        self.assertEqual("PRIMARY", records[0]["destination_instance"])
        self.assertEqual("STARTED", records[0]["export_status"])
        self.assertEqual("2026-01-15 10:30:00", str(records[0]["update_datetime"]))
        self.assertIsNotNone(records[0]["row_write_datetime"])

        self.assertEqual("table_1", records[1]["table_id"])
        self.assertEqual("COMPLETED", records[1]["export_status"])

        self.assertEqual("table_2", records[2]["table_id"])
        self.assertEqual("STARTED", records[2]["export_status"])
        self.assertEqual("2026-01-15 10:35:00", str(records[2]["update_datetime"]))

        self.assertEqual("table_2", records[3]["table_id"])
        self.assertEqual("COMPLETED", records[3]["export_status"])

    def test_write_export_records_to_bq_with_failure(self) -> None:
        export_manager = BigQueryToIngestBucketExportManager(
            address_to_update_datetime={
                BigQueryAddress(
                    dataset_id="test_dataset", table_id="failing_table"
                ): datetime.datetime(2026, 1, 15, 10, 30, 0, tzinfo=datetime.UTC),
            },
            state_code=StateCode.US_UT,
            raw_data_instance=DirectIngestInstance.SECONDARY,
        )

        with patch(
            "recidiviz.tools.ingest.regions.us_ut.export_bq_to_ingest_bucket.BigQueryTableToRawDataFileTagExportManager"
        ) as mock_export_manager:
            mock_instance = MagicMock()
            mock_export_manager.from_address_dir_and_datetime.return_value = (
                mock_instance
            )
            mock_instance.export.side_effect = Exception("Export failed!")

            with self.assertRaises(ExceptionGroup):
                export_manager.export(bq_client=self.bq_client, dry_run=False)

        results = self.query(
            f"SELECT * FROM `{self.export_tracker_address.to_str()}` ORDER BY row_write_datetime"
        )
        records = results.to_dict("records")

        # We expect 2 rows: STARTED, then FAILED
        self.assertEqual(2, len(records))

        self.assertEqual("failing_table", records[0]["table_id"])
        self.assertEqual("STARTED", records[0]["export_status"])
        self.assertEqual("SECONDARY", records[0]["destination_instance"])

        self.assertEqual("failing_table", records[1]["table_id"])
        self.assertEqual("FAILED", records[1]["export_status"])
        self.assertEqual("SECONDARY", records[1]["destination_instance"])

    def test_write_export_records_dry_run(self) -> None:
        export_manager = BigQueryToIngestBucketExportManager(
            address_to_update_datetime={
                BigQueryAddress(
                    dataset_id="test_dataset", table_id="dry_run_table"
                ): datetime.datetime(2026, 1, 15, tzinfo=datetime.UTC),
            },
            state_code=StateCode.US_UT,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        export_manager.export(bq_client=self.bq_client, dry_run=True)

        # Should not write any records to BigQuery in dry run mode
        results = self.query(f"SELECT * FROM `{self.export_tracker_address.to_str()}`")
        records = results.to_dict("records")
        self.assertEqual(0, len(records))
