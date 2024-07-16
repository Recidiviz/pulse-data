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
"""Tests for bq_load_tasks.py airflow tasks"""
import datetime
from typing import List
from unittest import TestCase
from unittest.mock import call, patch

import attr

from recidiviz.airflow.dags.raw_data.bq_load_tasks import (
    _filter_load_results_based_on_errors,
    append_to_raw_data_table_for_batch,
    generate_append_batches,
    load_and_prep_paths_for_batch,
)
from recidiviz.airflow.dags.raw_data.metadata import (
    APPEND_READY_FILE_BATCHES,
    SKIPPED_FILE_ERRORS,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    AppendReadyFile,
    AppendReadyFileBatch,
    AppendSummary,
    ImportReadyFile,
    RawDataAppendImportError,
    RawFileLoadAndPrepError,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.utils.airflow_types import BatchedTaskInstanceOutput


class LoadAndPrepForRegionTest(TestCase):
    """Unit tests for load_and_prep_paths_for_region airflow task"""

    def setUp(self) -> None:
        self.load_manager_patch = patch(
            "recidiviz.airflow.dags.raw_data.bq_load_tasks.DirectIngestRawFileLoadManager.load_and_prep_paths"
        )
        self.load_manager_mock = self.load_manager_patch.start()
        self.bq_patch = patch(
            "recidiviz.airflow.dags.raw_data.bq_load_tasks.BigQueryClientImpl"
        )
        self.bq_patch.start()
        self.fs_patch = patch(
            "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.GcsfsFactory.build",
        )
        self.fs_patch.start()

        self.mock_files = [
            ImportReadyFile(
                file_id=1,
                file_tag="singlePrimaryKey",
                file_paths=[],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            ImportReadyFile(
                file_id=2,
                file_tag="singlePrimaryKey",
                file_paths=[],
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            ImportReadyFile(
                file_id=3,
                file_tag="singlePrimaryKey",
                file_paths=[],
                update_datetime=datetime.datetime(
                    2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]

    def tearDown(self) -> None:
        self.load_manager_patch.stop()
        self.bq_patch.stop()
        self.fs_patch.stop()

    def test_no_files(self) -> None:
        result_str = load_and_prep_paths_for_batch.function(
            "US_XX", DirectIngestInstance.PRIMARY, [], fake_regions
        )

        result = BatchedTaskInstanceOutput.deserialize(
            result_str, result_cls=AppendReadyFile, error_cls=RawFileLoadAndPrepError
        )

        assert result.errors == []
        assert result.results == []

    @staticmethod
    def _sort(r: List[AppendReadyFile]) -> List[AppendReadyFile]:
        return list(sorted(r, key=lambda x: x.import_ready_file.file_id))

    def test_bad_input(self) -> None:
        with self.assertRaisesRegex(KeyError, r"file_id"):
            _ = load_and_prep_paths_for_batch.function(
                "US_XX",
                DirectIngestInstance.PRIMARY,
                ['{"this": "will-fail"}'],
                fake_regions,
            )

    def test_all_succeed(self) -> None:
        expected_results = []

        def return_success(irf: ImportReadyFile) -> AppendReadyFile:
            lps = AppendReadyFile(
                import_ready_file=irf,
                append_ready_table_address=BigQueryAddress(
                    dataset_id="fake", table_id="fake"
                ),
                raw_rows_count=10,
            )
            expected_results.append(lps)
            return lps

        self.load_manager_mock.side_effect = return_success

        result_str = load_and_prep_paths_for_batch.function(
            "US_XX",
            DirectIngestInstance.PRIMARY,
            [file.serialize() for file in self.mock_files],
            fake_regions,
        )

        result = BatchedTaskInstanceOutput.deserialize(
            result_str, result_cls=AppendReadyFile, error_cls=RawFileLoadAndPrepError
        )

        assert len(result.results) == len(expected_results)
        assert self._sort(result.results) == self._sort(expected_results)
        assert result.errors == []

    def test_mix_results(self) -> None:
        expected_results = []
        expected_errors = []

        def return_mixed(irf: ImportReadyFile) -> AppendReadyFile:
            if irf.file_id == 2:
                expected_errors.append(irf)
                raise ValueError("We hit an error")
            lps = AppendReadyFile(
                import_ready_file=irf,
                append_ready_table_address=BigQueryAddress(
                    dataset_id="fake", table_id="fake"
                ),
                raw_rows_count=10,
            )
            expected_results.append(lps)
            return lps

        self.load_manager_mock.side_effect = return_mixed

        result_str = load_and_prep_paths_for_batch.function(
            "US_XX",
            DirectIngestInstance.PRIMARY,
            [file.serialize() for file in self.mock_files],
            fake_regions,
        )

        result = BatchedTaskInstanceOutput.deserialize(
            result_str, result_cls=AppendReadyFile, error_cls=RawFileLoadAndPrepError
        )

        assert len(result.results) == len(expected_results)
        assert self._sort(result.results) == self._sort(expected_results)
        assert len(result.errors) == 1
        assert result.errors[0].update_datetime == expected_errors[0].update_datetime
        assert "We hit an error" in result.errors[0].error_msg


class GenerateAppendBatchesTest(TestCase):
    """Unit tests for generate_append_batches airflow task"""

    @staticmethod
    def get_summaries(file_tag: str) -> List[AppendReadyFile]:
        import_ready_file = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            file_paths=[],
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
        )
        return [
            AppendReadyFile(
                import_ready_file=attr.evolve(
                    import_ready_file,
                    file_id=1,
                    update_datetime=datetime.datetime(
                        2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                ),
                append_ready_table_address=BigQueryAddress(
                    dataset_id="fake", table_id="fake"
                ),
                raw_rows_count=10,
            ),
            AppendReadyFile(
                import_ready_file=attr.evolve(
                    import_ready_file,
                    file_id=3,
                    update_datetime=datetime.datetime(
                        2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                ),
                append_ready_table_address=BigQueryAddress(
                    dataset_id="fake", table_id="fake"
                ),
                raw_rows_count=10,
            ),
            AppendReadyFile(
                import_ready_file=attr.evolve(
                    import_ready_file,
                    file_id=4,
                    update_datetime=datetime.datetime(
                        2024, 1, 4, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                ),
                append_ready_table_address=BigQueryAddress(
                    dataset_id="fake", table_id="fake"
                ),
                raw_rows_count=10,
            ),
        ]

    def test_no_output(self) -> None:
        assert generate_append_batches.function([]) == (
            {SKIPPED_FILE_ERRORS: [], APPEND_READY_FILE_BATCHES: []}
        )

    def test_all_success(self) -> None:
        summaries = self.get_summaries("singlePrimaryKey")
        _in = BatchedTaskInstanceOutput[AppendReadyFile, RawFileLoadAndPrepError](
            errors=[], results=summaries
        )

        output_batch = AppendReadyFileBatch(
            append_ready_files_by_tag={"singlePrimaryKey": summaries}
        )

        assert generate_append_batches.function([_in.serialize()]) == (
            {
                SKIPPED_FILE_ERRORS: [],
                APPEND_READY_FILE_BATCHES: [output_batch.serialize()],
            }
        )

    def test_all_non_blocking(self) -> None:
        summaries = self.get_summaries("singlePrimaryKey")
        _in = BatchedTaskInstanceOutput[AppendReadyFile, RawFileLoadAndPrepError](
            errors=[
                RawFileLoadAndPrepError(
                    error_msg="Error!",
                    file_tag="tagFullHistoricalExport",
                    file_paths=[],
                    update_datetime=datetime.datetime(
                        2024, 1, 10, 1, 1, tzinfo=datetime.UTC
                    ),
                )
            ],
            results=summaries,
        )

        output_batch = AppendReadyFileBatch(
            append_ready_files_by_tag={"singlePrimaryKey": summaries}
        )

        assert generate_append_batches.function([_in.serialize()]) == (
            {
                SKIPPED_FILE_ERRORS: [],
                APPEND_READY_FILE_BATCHES: [output_batch.serialize()],
            }
        )

    def test_failures_blocking(self) -> None:
        summaries = self.get_summaries("tagFullHistoricalExport")
        f = RawFileLoadAndPrepError(
            error_msg="Error!",
            file_tag="tagFullHistoricalExport",
            file_paths=[],
            update_datetime=datetime.datetime(2024, 1, 2, 1, 1, tzinfo=datetime.UTC),
        )

        _in = BatchedTaskInstanceOutput[AppendReadyFile, RawFileLoadAndPrepError](
            errors=[f], results=summaries
        )
        output_batch = AppendReadyFileBatch(
            append_ready_files_by_tag={"tagFullHistoricalExport": summaries[:1]}
        )

        result = generate_append_batches.function([_in.serialize()])

        assert result[APPEND_READY_FILE_BATCHES] == [output_batch.serialize()]
        assert len(result[SKIPPED_FILE_ERRORS]) == 2
        _r = [
            RawFileLoadAndPrepError.deserialize(r) for r in result[SKIPPED_FILE_ERRORS]
        ]
        assert _r[0].update_datetime == summaries[1].import_ready_file.update_datetime
        assert _r[1].update_datetime == summaries[2].import_ready_file.update_datetime

    def test_filter_load_results_based_on_errors_none(self) -> None:
        assert _filter_load_results_based_on_errors([], []) == ([], [])

    def test_filter_load_results_based_on_errors_all_success(self) -> None:
        assert _filter_load_results_based_on_errors(
            self.get_summaries("tagFullHistoricalExport"), []
        ) == (self.get_summaries("tagFullHistoricalExport"), [])

    def test_filter_load_results_based_on_errors_non_blocking(self) -> None:
        assert _filter_load_results_based_on_errors(
            self.get_summaries("tagFullHistoricalExport"),
            [
                RawFileLoadAndPrepError(
                    error_msg="yikes!",
                    file_tag="tagFullHistoricalExport",
                    update_datetime=datetime.datetime(
                        2024, 1, 5, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_paths=[],
                )
            ],
        ) == (self.get_summaries("tagFullHistoricalExport"), [])

    def test_filter_load_results_based_on_errors_blocking(self) -> None:
        results_input = self.get_summaries("tagFullHistoricalExport")
        results, errors = _filter_load_results_based_on_errors(
            results_input,
            [
                RawFileLoadAndPrepError(
                    error_msg="yikes!",
                    file_tag="tagFullHistoricalExport",
                    update_datetime=datetime.datetime(
                        2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_paths=[],
                )
            ],
        )

        assert results == results_input[:1]
        assert len(errors) == 2
        assert {error.update_datetime for error in errors} == {
            result_input.import_ready_file.update_datetime
            for result_input in results_input[1:]
        }


class AppendToRawDataTableForRegionTest(TestCase):
    """Unit tests for append_to_raw_data_table_for_region airflow task"""

    def setUp(self) -> None:
        self.load_manager_patch = patch(
            "recidiviz.airflow.dags.raw_data.bq_load_tasks.DirectIngestRawFileLoadManager.append_to_raw_data_table"
        )
        self.load_manager_mock = self.load_manager_patch.start()
        self.bq_patch = patch(
            "recidiviz.airflow.dags.raw_data.bq_load_tasks.BigQueryClientImpl"
        )
        self.bq_patch.start()
        self.fs_patch = patch(
            "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.GcsfsFactory.build",
        )
        self.fs_patch.start()

    def tearDown(self) -> None:
        self.load_manager_patch.stop()
        self.bq_patch.stop()
        self.fs_patch.stop()

    @staticmethod
    def get_summaries(file_tag: str) -> List[AppendReadyFile]:
        import_ready_file = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            file_paths=[],
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
        )
        return [
            AppendReadyFile(
                import_ready_file=attr.evolve(
                    import_ready_file,
                    file_id=1,
                    update_datetime=datetime.datetime(
                        2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                ),
                append_ready_table_address=BigQueryAddress(
                    dataset_id="fake", table_id="fake_1"
                ),
                raw_rows_count=10,
            ),
            AppendReadyFile(
                import_ready_file=attr.evolve(
                    import_ready_file,
                    file_id=3,
                    update_datetime=datetime.datetime(
                        2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                ),
                append_ready_table_address=BigQueryAddress(
                    dataset_id="fake", table_id="fake_3"
                ),
                raw_rows_count=10,
            ),
            AppendReadyFile(
                import_ready_file=attr.evolve(
                    import_ready_file,
                    file_id=4,
                    update_datetime=datetime.datetime(
                        2024, 1, 4, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                ),
                append_ready_table_address=BigQueryAddress(
                    dataset_id="fake", table_id="fake_4"
                ),
                raw_rows_count=10,
            ),
        ]

    @staticmethod
    def _sort(r: List[AppendSummary]) -> List[AppendSummary]:
        return list(sorted(r, key=lambda x: x.file_id))

    def test_no_files(self) -> None:
        result_str = append_to_raw_data_table_for_batch.function(
            "US_XX", DirectIngestInstance.PRIMARY, "{}", fake_regions
        )

        result = BatchedTaskInstanceOutput.deserialize(
            result_str, result_cls=AppendReadyFile, error_cls=RawFileLoadAndPrepError
        )

        assert result.errors == []
        assert result.results == []

    def test_all_succeed(self) -> None:

        expected_results = []

        def return_success(lps: AppendReadyFile) -> AppendSummary:
            append = AppendSummary(
                file_id=lps.import_ready_file.file_id,
            )
            expected_results.append(append)
            return append

        self.load_manager_mock.side_effect = return_success

        result_str = append_to_raw_data_table_for_batch.function(
            "US_XX",
            DirectIngestInstance.PRIMARY,
            AppendReadyFileBatch(
                append_ready_files_by_tag={
                    "singlePrimaryKey": self.get_summaries("singlePrimaryKey"),
                    "tagFullHistoricalExport": self.get_summaries(
                        "tagFullHistoricalExport"
                    ),
                }
            ).serialize(),
            fake_regions,
        )

        result = BatchedTaskInstanceOutput.deserialize(
            result_str, result_cls=AppendSummary, error_cls=RawDataAppendImportError
        )

        assert result.errors == []
        assert self._sort(result.results) == self._sort(expected_results)

    def test_all_non_blocking(self) -> None:

        expected_results = []
        expected_fails = []

        def return_mix(lps: AppendReadyFile) -> AppendSummary:
            if (
                lps.import_ready_file.file_id == 4
                and lps.import_ready_file.file_tag == "tagFullHistoricalExport"
            ):
                expected_fails.append(lps.append_ready_table_address)
                raise ValueError("oops!")

            append = AppendSummary(
                file_id=lps.import_ready_file.file_id,
            )
            expected_results.append(append)
            return append

        self.load_manager_mock.side_effect = return_mix

        inputs = {
            "singlePrimaryKey": self.get_summaries("singlePrimaryKey"),
            "tagFullHistoricalExport": self.get_summaries("tagFullHistoricalExport"),
        }

        result_str = append_to_raw_data_table_for_batch.function(
            "US_XX",
            DirectIngestInstance.PRIMARY,
            AppendReadyFileBatch(append_ready_files_by_tag=inputs).serialize(),
            fake_regions,
        )

        result = BatchedTaskInstanceOutput.deserialize(
            result_str, result_cls=AppendSummary, error_cls=RawDataAppendImportError
        )
        self.load_manager_mock.assert_has_calls(
            calls=[call(file) for _, files in inputs.items() for file in files],
            any_order=True,
        )
        assert self._sort(result.results) == self._sort(expected_results)
        assert {e.raw_temp_table for e in result.errors} == {
            BigQueryAddress(dataset_id="fake", table_id="fake_4"),
        }

    def test_blocking_fail(self) -> None:
        expected_results = []
        expected_fails = []

        def return_mix(lps: AppendReadyFile) -> AppendSummary:
            if (
                lps.import_ready_file.file_id == 3
                and lps.import_ready_file.file_tag == "tagFullHistoricalExport"
            ):
                expected_fails.append(lps.append_ready_table_address)
                raise ValueError("oops!")

            append = AppendSummary(
                file_id=lps.import_ready_file.file_id,
            )
            expected_results.append(append)
            return append

        self.load_manager_mock.side_effect = return_mix

        inputs = {
            "singlePrimaryKey": self.get_summaries("singlePrimaryKey"),
            "tagFullHistoricalExport": self.get_summaries("tagFullHistoricalExport"),
        }

        result_str = append_to_raw_data_table_for_batch.function(
            "US_XX",
            DirectIngestInstance.PRIMARY,
            AppendReadyFileBatch(append_ready_files_by_tag=inputs).serialize(),
            fake_regions,
        )

        result = BatchedTaskInstanceOutput.deserialize(
            result_str, result_cls=AppendSummary, error_cls=RawDataAppendImportError
        )

        self.load_manager_mock.assert_has_calls(
            calls=[call(file) for file in inputs["singlePrimaryKey"]]
            + [call(file) for file in inputs["tagFullHistoricalExport"][:1]],
            any_order=True,
        )
        assert self._sort(result.results) == self._sort(expected_results)
        assert {e.raw_temp_table for e in result.errors} == {
            BigQueryAddress(dataset_id="fake", table_id="fake_3"),
            BigQueryAddress(dataset_id="fake", table_id="fake_4"),
        }
