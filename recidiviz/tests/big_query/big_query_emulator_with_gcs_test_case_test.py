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
"""Tests for BigQueryEmulatorWithGCSTestCase: the FakeGCSFileSystem-backed
load_table_from_cloud_storage stand-in reads the CSVs seeded into self.fs and
lands their rows in the emulator table."""

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.tests.big_query.big_query_emulator_with_gcs_test_case import (
    BigQueryEmulatorWithGCSTestCase,
)

_DATASET = "dataset_1"
_TABLE = "table_1"
_BUCKET = "test-bucket"
_UPLOAD_DIR = "uploads"

_SCHEMA = [
    bigquery.SchemaField("a", bigquery.enums.SqlTypeNames.STRING.value),
    bigquery.SchemaField("b", bigquery.enums.SqlTypeNames.STRING.value),
]


class BigQueryEmulatorWithGCSTestCaseTest(BigQueryEmulatorWithGCSTestCase):
    """Drives the wired-up load_table_from_cloud_storage against self.fs the way a
    real consumer does: seed CSVs into the fake filesystem, load a
    `gs://.../*.csv` glob, and read the rows back out of the emulator."""

    def _destination(self) -> BigQueryAddress:
        return BigQueryAddress(dataset_id=_DATASET, table_id=_TABLE)

    def _upload_dir(self) -> GcsfsDirectoryPath:
        return GcsfsDirectoryPath(bucket_name=_BUCKET, relative_path=_UPLOAD_DIR)

    def _seed_csv(self, file_name: str, contents: str) -> None:
        self.fs.upload_from_string(
            path=GcsfsFilePath.from_directory_and_file_name(
                self._upload_dir(), file_name
            ),
            contents=contents,
            content_type="text/csv",
        )

    def _load(self, source_uris: list[str]) -> None:
        self.bq_client.load_table_from_cloud_storage(
            source_uris=source_uris,
            destination_address=self._destination(),
            destination_table_schema=_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

    def _glob(self) -> str:
        return f"{self._upload_dir().uri()}/*.csv"

    def test_fs_is_wired(self) -> None:
        self.assertIsNotNone(self.fs)

    def test_load_reads_csvs_from_fake_fs(self) -> None:
        # Rows from every CSV under the glob land in the table; an empty cell reads
        # back as NULL.
        self.create_mock_table(self._destination(), schema=_SCHEMA)
        self._seed_csv("part_0.csv", "1,x\n2,\n")
        self._seed_csv("part_1.csv", "3,z\n")

        self._load([self._glob()])

        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{_DATASET}.{_TABLE}` ORDER BY a",
            expected_result=[
                {"a": "1", "b": "x"},
                {"a": "2", "b": None},
                {"a": "3", "b": "z"},
            ],
        )

    def test_load_ignores_non_csv_and_aggregates_source_uris(self) -> None:
        # Only *.csv files are read (a stray non-CSV under the same dir is skipped),
        # and multiple source_uris are aggregated into one load.
        self.create_mock_table(self._destination(), schema=_SCHEMA)
        self._seed_csv("part_0.csv", "1,x\n")
        self._seed_csv("_SUCCESS", "not,a,csv\n")

        self._load([self._glob(), self._glob()])

        # The single CSV row is picked up once per source_uri; the non-CSV file is
        # never read.
        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{_DATASET}.{_TABLE}`",
            expected_result=[{"a": "1", "b": "x"}, {"a": "1", "b": "x"}],
        )

    def test_load_with_no_matching_files_is_a_noop(self) -> None:
        self.create_mock_table(self._destination(), schema=_SCHEMA)

        self._load([self._glob()])

        self.run_query_test(
            f"SELECT a, b FROM `{self.project_id}.{_DATASET}.{_TABLE}`",
            expected_result=[],
        )
