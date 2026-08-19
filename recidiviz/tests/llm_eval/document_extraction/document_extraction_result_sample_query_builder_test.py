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
"""Tests for DocumentExtractionResultSampleQueryBuilder, run against the BQ emulator using
the fake US_XX extractor collection.
"""
import datetime
import json
from typing import Any

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.extraction_results_narrowing import (
    ExtractionResultsNarrowing,
)
from recidiviz.documents.extraction.llm_extraction_results_tables import (
    ExtractionValidatedResultsBQTable,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    ROW_CREATE_DATETIME_COLUMN_NAME,
)
from recidiviz.llm_eval.document_extraction.document_extraction_result_sample_query_builder import (
    DocumentExtractionResultSampleQueryBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents import fake_config

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"

_VERSION = "v_sampled"
_OTHER_VERSION = "v_superseded"
_FIRST_JOB = "job_first_pass"
_RETRY_JOB = "job_retry"

_EARLIER = datetime.datetime(2026, 3, 1, 12, 0, 0)
_LATER = datetime.datetime(2026, 3, 2, 12, 0, 0)

# Narrows to one version across every job it ran, which is the common case.
_VERSION_NARROWING = ExtractionResultsNarrowing(
    extractor_version_id=_VERSION, extraction_job_id=None
)


class DocumentExtractionResultSampleQueryBuilderTest(BigQueryEmulatorTestCase):
    """Runs the sample query against the emulator and asserts on the rows it selects."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor_config = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        )

    def _results_address(self, sandbox_prefix: str | None = None) -> BigQueryAddress:
        return ExtractionValidatedResultsBQTable.address(
            state_code=_STATE_CODE,
            collection_name=self.extractor_config.extractor_collection.name,
            sandbox_prefix=sandbox_prefix,
        )

    def _contents_address(self, sandbox_prefix: str | None = None) -> BigQueryAddress:
        return self.extractor_config.input_document_collection.document_contents_table_address(
            sandbox_dataset_prefix=sandbox_prefix
        )

    def _load_results(
        self, rows: list[dict[str, Any]], *, sandbox_prefix: str | None = None
    ) -> None:
        address = self._results_address(sandbox_prefix)
        self.create_mock_table(
            address=address, schema=ExtractionValidatedResultsBQTable.schema()
        )
        self.load_rows_into_table(address, rows)

    def _load_contents(
        self, rows: list[dict[str, Any]], *, sandbox_prefix: str | None = None
    ) -> None:
        address = self._contents_address(sandbox_prefix)
        self.create_mock_table(
            address=address,
            schema=self.extractor_config.input_document_collection.build_bq_document_contents_schema(),
        )
        self.load_rows_into_table(address, rows)

    @staticmethod
    def _result_row(
        *,
        document_contents_id: str,
        extractor_version_id: str = _VERSION,
        job_id: str = _FIRST_JOB,
        validated_at: datetime.datetime = _EARLIER,
        is_relevant: bool = True,
        extracted_status: str = "active",
    ) -> dict[str, Any]:
        return ExtractionValidatedResultsBQTable.to_row(
            state_code_str=_STATE_CODE.value,
            document_contents_id=document_contents_id,
            job_id=job_id,
            extractor_version_id=extractor_version_id,
            validation_config_version_id="thresholds_v1",
            validation_datetime_utc=validated_at,
            is_relevant=is_relevant,
            validated_output_json={"primary_status": extracted_status},
        )

    @staticmethod
    def _contents_row(
        *, document_contents_id: str, document_text: str | None
    ) -> dict[str, Any]:
        return {
            DOCUMENT_CONTENTS_ID_COLUMN_NAME: document_contents_id,
            DOCUMENT_TEXT_COLUMN_NAME: document_text,
            DOCUMENT_LENGTH_BYTES_COLUMN_NAME: len(document_text or ""),
            ROW_CREATE_DATETIME_COLUMN_NAME: _EARLIER,
        }

    def _run_query(
        self,
        *,
        sample_size: int = 10,
        narrowing: ExtractionResultsNarrowing = _VERSION_NARROWING,
        input_results_sandbox_dataset_prefix: str | None = None,
        input_documents_sandbox_dataset_prefix: str | None = None,
    ) -> list[dict[str, Any]]:
        query = DocumentExtractionResultSampleQueryBuilder(
            extractor_config=self.extractor_config,
            results_narrowing=narrowing,
            sample_size=sample_size,
            input_results_sandbox_dataset_prefix=input_results_sandbox_dataset_prefix,
            input_documents_sandbox_dataset_prefix=input_documents_sandbox_dataset_prefix,
        ).build_query(project_id=self.project_id)
        results: list[dict[str, Any]] = self.query(query).to_dict("records")
        return results

    def _sampled_document_ids(self, **kwargs: Any) -> list[str]:
        return sorted(
            row[DOCUMENT_CONTENTS_ID_COLUMN_NAME] for row in self._run_query(**kwargs)
        )

    def test_returns_one_row_per_document_with_its_text(self) -> None:
        self._load_results(
            [
                self._result_row(
                    document_contents_id="doc_a", extracted_status="active"
                ),
                self._result_row(
                    document_contents_id="doc_b", extracted_status="discharged"
                ),
            ]
        )
        self._load_contents(
            [
                self._contents_row(
                    document_contents_id="doc_a",
                    document_text="Client started at Walmart as a cashier.",
                ),
                self._contents_row(
                    document_contents_id="doc_b",
                    document_text="Client was discharged in March.",
                ),
            ]
        )
        self.assertEqual(
            [
                {
                    "state_code": "US_XX",
                    "document_contents_id": "doc_a",
                    "extractor_version_id": _VERSION,
                    "result_json": json.dumps({"primary_status": "active"}),
                    "document_text": "Client started at Walmart as a cashier.",
                },
                {
                    "state_code": "US_XX",
                    "document_contents_id": "doc_b",
                    "extractor_version_id": _VERSION,
                    "result_json": json.dumps({"primary_status": "discharged"}),
                    "document_text": "Client was discharged in March.",
                },
            ],
            sorted(
                self._run_query(),
                key=lambda row: row[DOCUMENT_CONTENTS_ID_COLUMN_NAME],
            ),
        )

    def test_narrows_to_the_named_extractor_version(self) -> None:
        self._load_results(
            [
                self._result_row(document_contents_id="doc_a"),
                self._result_row(
                    document_contents_id="doc_b", extractor_version_id=_OTHER_VERSION
                ),
            ]
        )
        self._load_contents(
            [
                self._contents_row(
                    document_contents_id="doc_a", document_text="Sampled note."
                ),
                self._contents_row(
                    document_contents_id="doc_b", document_text="Older version's note."
                ),
            ]
        )
        self.assertEqual(["doc_a"], self._sampled_document_ids())

    def test_narrows_to_one_job_when_given_one(self) -> None:
        self._load_results(
            [
                self._result_row(document_contents_id="doc_a", job_id=_FIRST_JOB),
                self._result_row(document_contents_id="doc_b", job_id=_RETRY_JOB),
            ]
        )
        self._load_contents(
            [
                self._contents_row(
                    document_contents_id="doc_a", document_text="First pass note."
                ),
                self._contents_row(
                    document_contents_id="doc_b", document_text="Retried note."
                ),
            ]
        )
        self.assertEqual(
            ["doc_b"],
            self._sampled_document_ids(
                narrowing=ExtractionResultsNarrowing(
                    extractor_version_id=_VERSION, extraction_job_id=_RETRY_JOB
                )
            ),
        )

    def test_keeps_every_job_when_none_given(self) -> None:
        self._load_results(
            [
                self._result_row(document_contents_id="doc_a", job_id=_FIRST_JOB),
                self._result_row(document_contents_id="doc_b", job_id=_RETRY_JOB),
            ]
        )
        self._load_contents(
            [
                self._contents_row(
                    document_contents_id="doc_a", document_text="First pass note."
                ),
                self._contents_row(
                    document_contents_id="doc_b", document_text="Retried note."
                ),
            ]
        )
        self.assertEqual(["doc_a", "doc_b"], self._sampled_document_ids())

    def test_latest_validation_wins_per_document(self) -> None:
        self._load_results(
            [
                self._result_row(
                    document_contents_id="doc_a",
                    validated_at=_EARLIER,
                    extracted_status="active",
                ),
                self._result_row(
                    document_contents_id="doc_a",
                    validated_at=_LATER,
                    extracted_status="discharged",
                ),
            ]
        )
        self._load_contents(
            [
                self._contents_row(
                    document_contents_id="doc_a", document_text="Revalidated note."
                )
            ]
        )
        rows = self._run_query()
        self.assertEqual(1, len(rows))
        self.assertEqual(
            json.dumps({"primary_status": "discharged"}), rows[0]["result_json"]
        )

    def test_document_whose_latest_result_is_irrelevant_is_dropped(self) -> None:
        """The relevance filter runs after the dedupe, so an earlier relevant result cannot
        put back a document the newest result calls irrelevant.
        """
        self._load_results(
            [
                self._result_row(
                    document_contents_id="doc_a",
                    validated_at=_EARLIER,
                    is_relevant=True,
                ),
                self._result_row(
                    document_contents_id="doc_a",
                    validated_at=_LATER,
                    is_relevant=False,
                ),
            ]
        )
        self._load_contents(
            [
                self._contents_row(
                    document_contents_id="doc_a", document_text="Now judged irrelevant."
                )
            ]
        )
        self.assertEqual([], self._sampled_document_ids())

    def test_document_with_null_text_is_dropped(self) -> None:
        self._load_results(
            [
                self._result_row(document_contents_id="doc_a"),
                self._result_row(document_contents_id="doc_scrubbed"),
            ]
        )
        self._load_contents(
            [
                self._contents_row(
                    document_contents_id="doc_a", document_text="Readable note."
                ),
                self._contents_row(
                    document_contents_id="doc_scrubbed", document_text=None
                ),
            ]
        )
        self.assertEqual(["doc_a"], self._sampled_document_ids())

    def test_sample_size_caps_documents(self) -> None:
        self._load_results(
            [self._result_row(document_contents_id=f"doc_{i}") for i in range(5)]
        )
        self._load_contents(
            [
                self._contents_row(
                    document_contents_id=f"doc_{i}", document_text=f"Note {i}."
                )
                for i in range(5)
            ]
        )
        self.assertEqual(2, len(self._run_query(sample_size=2)))

    def test_larger_sample_includes_the_smaller_one(self) -> None:
        """Sampling orders every document before taking the first sample_size of them, so
        raising the size adds documents rather than choosing a fresh set. Asserting the
        subset relation rather than which documents get picked keeps this independent of
        what FARM_FINGERPRINT hashes to.
        """
        self._load_results(
            [self._result_row(document_contents_id=f"doc_{i}") for i in range(5)]
        )
        self._load_contents(
            [
                self._contents_row(
                    document_contents_id=f"doc_{i}", document_text=f"Note {i}."
                )
                for i in range(5)
            ]
        )
        smaller = set(self._sampled_document_ids(sample_size=2))
        larger = set(self._sampled_document_ids(sample_size=3))
        self.assertEqual(2, len(smaller))
        self.assertEqual(3, len(larger))
        self.assertTrue(
            smaller < larger,
            f"Sample of 2 {sorted(smaller)} is not contained in the sample of 3 "
            f"{sorted(larger)}, so the sample is not ordered deterministically.",
        )

    def test_reads_sandbox_results_with_production_documents(self) -> None:
        """A sandbox extraction run writes its results to a sandbox dataset while still
        reading the production document store, so the two prefixes have to apply
        independently.
        """
        self._load_results(
            [self._result_row(document_contents_id="doc_a")],
            sandbox_prefix="my_prefix",
        )
        self._load_contents(
            [
                self._contents_row(
                    document_contents_id="doc_a", document_text="Production note."
                )
            ]
        )
        self.assertEqual(
            ["doc_a"],
            self._sampled_document_ids(
                input_results_sandbox_dataset_prefix="my_prefix",
                input_documents_sandbox_dataset_prefix=None,
            ),
        )
