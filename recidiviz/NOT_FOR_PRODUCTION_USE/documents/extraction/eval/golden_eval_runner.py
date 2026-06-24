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
"""Runs the golden eval pipeline for a single extractor.

Three steps:
1. Deploy the collection's eval source table (Google Sheets) to a sandbox BQ dataset.
2. Upload eval documents to the sandbox GCS bucket (skipping any already present),
   then run the extractor's LLM via LiteLLMClient.
3. Score the LLM outputs against the golden labels using score_document and
   print a human-readable accuracy report.
"""
import datetime
import json
import logging
import os
from typing import Any

import attr
from google.cloud import bigquery as bq_lib

import recidiviz.NOT_FOR_PRODUCTION_USE.source_tables as _nfpu_source_tables
from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_provider import (
    DOCUMENT_MIME_TYPE,
    GCSDocument,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.eval.eval_scorer import (
    DocumentScore,
    score_document,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.eval.golden_eval_config import (
    GoldenEvalConfig,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    ExtractionFieldMode,
    ExtractionFieldType,
    ExtractionInferredField,
    ExtractionOutputSchema,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.litellm_client import (
    LiteLLMClient,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.llm_client import (
    LLMExtractionBatchResult,
    LLMExtractionRequest,
    LLMExtractionResult,
    LLMExtractionStatus,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_metadata import (
    ExtractionJobMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_submitted_document_metadata import (
    ExtractionJobSubmittedDocumentMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_store_utils import (
    gcs_path_for_document,
)
from recidiviz.source_tables.collect_source_tables_from_yamls import (
    collect_source_tables_from_yamls_by_dataset,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.utils.metadata import project_id

_EVAL_DATASET_ID = "document_extraction_collection_golden_eval_data"

_LOG_FILE = os.path.join(
    os.path.dirname(__file__),
    "logs",
    "deploy_eval_source_table.log",
)

_EVAL_RESULTS_EXPIRY_HOURS = 24

_EVAL_RESULTS_SCHEMA: list[bq_lib.SchemaField] = [
    bq_lib.SchemaField("run_timestamp", "TIMESTAMP"),
    bq_lib.SchemaField("extractor_id", "STRING"),
    bq_lib.SchemaField("state_code", "STRING"),
    bq_lib.SchemaField("collection_name", "STRING"),
    bq_lib.SchemaField("llm_provider", "STRING"),
    bq_lib.SchemaField("model", "STRING"),
    bq_lib.SchemaField("thinking_budget", "INTEGER", mode="NULLABLE"),
    bq_lib.SchemaField("document_id", "STRING"),
    bq_lib.SchemaField("test_type", "STRING", mode="NULLABLE"),
    bq_lib.SchemaField("test_case", "STRING", mode="NULLABLE"),
    bq_lib.SchemaField("document_text", "STRING"),
    bq_lib.SchemaField("is_relevant_expected", "BOOL"),
    bq_lib.SchemaField("is_relevant_actual", "BOOL", mode="NULLABLE"),
    bq_lib.SchemaField("is_relevant_match", "BOOL", mode="NULLABLE"),
    bq_lib.SchemaField(
        "field_results",
        "RECORD",
        mode="REPEATED",
        fields=[
            bq_lib.SchemaField("field_name", "STRING"),
            bq_lib.SchemaField("expected", "STRING", mode="NULLABLE"),
            bq_lib.SchemaField("actual", "STRING", mode="NULLABLE"),
            bq_lib.SchemaField("is_match", "BOOL"),
        ],
    ),
    bq_lib.SchemaField("all_fields_match", "BOOL", mode="NULLABLE"),
    bq_lib.SchemaField("difficulty", "STRING", mode="NULLABLE"),
]


def _flatten_struct_element(
    elem: Any,
    struct_fields: tuple[ExtractionInferredField, ...],
) -> dict[str, Any]:
    flat_elem: dict[str, Any] = {}
    for sf in struct_fields:
        sf_raw = elem.get(sf.name) if isinstance(elem, dict) else None
        if sf.field_mode == ExtractionFieldMode.STRUCTURAL:
            flat_elem[sf.name] = sf_raw
        elif isinstance(sf_raw, dict):
            flat_elem[sf.name] = sf_raw.get("value")
        else:
            flat_elem[sf.name] = sf_raw
    return flat_elem


def _flatten_llm_extracted_data(
    extracted_data: dict[str, Any],
    output_schema: ExtractionOutputSchema,
) -> dict[str, Any]:
    """Converts the LLM envelope format to flat field values for scoring.

    LLM outputs wrap each INFERRED field as {value, confidence_score, null_reason,
    citations}. STRUCTURAL sub-fields within ARRAY_OF_STRUCT are plain values.
    This function extracts the flat value for each field so it can be passed
    directly to score_document.
    """
    flat: dict[str, Any] = {}
    for field in output_schema.inferred_fields:
        raw = extracted_data.get(field.name)
        if field.field_type == ExtractionFieldType.ARRAY_OF_STRUCT:
            if not isinstance(raw, list):
                flat[field.name] = []
            else:
                assert field.struct_fields is not None
                flat[field.name] = [
                    _flatten_struct_element(elem, field.struct_fields) for elem in raw
                ]
        elif field.field_mode == ExtractionFieldMode.STRUCTURAL:
            flat[field.name] = raw
        elif isinstance(raw, dict):
            flat[field.name] = raw.get("value")
        else:
            flat[field.name] = raw
    return flat


@attr.define
class GoldenEvalRunner:
    """Orchestrates the three-step golden eval pipeline for one extractor."""

    extractor: LLMPromptExtractorMetadata
    eval_config: GoldenEvalConfig
    sandbox_dataset_prefix: str
    sandbox_documents_bucket: str
    batch_size: int = 10

    def deploy_eval_table(self, bq_client: BigQueryClient) -> None:
        """Deploys the collection's eval source table to the sandbox BQ dataset."""
        table_id = self.extractor.collection_name.lower()

        nfpu_yamls_root = os.path.join(
            os.path.dirname(_nfpu_source_tables.__file__),
            "yaml_managed",
        )
        source_tables_by_dataset = collect_source_tables_from_yamls_by_dataset(
            yamls_root_path=nfpu_yamls_root
        )
        eval_source_tables = source_tables_by_dataset.get(_EVAL_DATASET_ID, [])

        target_table = next(
            (st for st in eval_source_tables if st.address.table_id == table_id),
            None,
        )
        if target_table is None:
            raise ValueError(
                f"No eval source table YAML found for collection '{table_id}' in "
                f"{_EVAL_DATASET_ID}. Run generate_eval_source_table_yamls.py first."
            )

        collection = SourceTableCollection(
            dataset_id=_EVAL_DATASET_ID,
            update_config=SourceTableCollectionUpdateConfig.regenerable(),
            description=f"Golden eval data for {table_id}",
            labels=[],
            source_tables_by_address={target_table.address: target_table},
        )
        sandbox_collection = collection.as_sandbox_collection(
            self.sandbox_dataset_prefix
        )

        os.makedirs(os.path.dirname(_LOG_FILE), exist_ok=True)
        update_manager = SourceTableUpdateManager(bq_client)
        update_manager.update_async(
            source_table_collections=[sandbox_collection],
            log_file=_LOG_FILE,
            log_output=False,
        )

    def read_eval_rows(self, bq_client: BigQueryClient) -> list[dict[str, Any]]:
        """Reads all eval rows for the extractor's state from the sandbox eval table."""
        sandbox_dataset = BigQueryAddressOverrides.format_sandbox_dataset(
            self.sandbox_dataset_prefix, _EVAL_DATASET_ID
        )
        table_id = self.extractor.collection_name.lower()
        query = f"""
            SELECT *
            FROM `{bq_client.project_id}.{sandbox_dataset}.{table_id}`
            WHERE state_code = '{self.extractor.state_code.value}'
        """
        results = bq_client.run_query_async(query_str=query, use_query_cache=False)
        return [dict(row.items()) for row in results]

    def upload_eval_documents_to_gcs(
        self, eval_rows: list[dict[str, Any]]
    ) -> list[GCSDocument]:
        """Uploads eval documents to the sandbox GCS bucket, skipping any already present.

        Returns the full list of GCSDocument objects (uploaded + pre-existing).
        """
        gcs_fs = GcsfsFactory.build()
        documents: list[GCSDocument] = []
        n_uploaded = 0
        n_skipped = 0
        for row in eval_rows:
            document_id = str(row["document_id"])
            gcs_path = gcs_path_for_document(
                project_id=project_id(),
                state_code=self.extractor.state_code,
                document_id=document_id,
                sandbox_bucket=self.sandbox_documents_bucket,
            )
            if not gcs_fs.exists(gcs_path):
                gcs_fs.upload_from_string(
                    path=gcs_path,
                    contents=row["document_text"],
                    content_type=DOCUMENT_MIME_TYPE,
                )
                n_uploaded += 1
            else:
                n_skipped += 1
            documents.append(
                GCSDocument(
                    document_id=document_id,
                    gcs_file_path=gcs_path,
                    mime_type=DOCUMENT_MIME_TYPE,
                )
            )
        logging.info(
            "  Uploaded %d documents, skipped %d already in GCS.",
            n_uploaded,
            n_skipped,
        )
        return documents

    def run_llm(self, documents: list[GCSDocument]) -> list[LLMExtractionResult]:
        """Runs the LLM via LiteLLMClient for all eval documents and returns results."""
        litellm_client = LiteLLMClient(
            batches_gcs_bucket=self.sandbox_documents_bucket,
            batch_size=self.batch_size,
        )
        requests = [
            LLMExtractionRequest(extractor=self.extractor, document=doc)
            for doc in documents
        ]
        job_id = litellm_client.submit_batch(requests)

        now = datetime.datetime.now(datetime.timezone.utc)
        job_metadata = ExtractionJobMetadata(
            job_id=job_id,
            start_datetime=now,
            extractor_id=self.extractor.extractor_id,
            extractor_version_id=self.extractor.extractor_version_id(),
            state_code=self.extractor.state_code,
            num_documents_submitted=len(documents),
        )
        submitted_doc_metadatas = [
            ExtractionJobSubmittedDocumentMetadata(
                job_id=job_id,
                document_id=doc.document_id,
                submitted_datetime=now,
                job_index=idx,
                gcs_uri=doc.gcs_file_path.uri(),
                mime_type=doc.mime_type,
            )
            for idx, doc in enumerate(documents)
        ]

        while True:
            batch_status = litellm_client.get_results(
                job_metadata, submitted_doc_metadatas
            )
            if isinstance(batch_status, LLMExtractionBatchResult):
                if batch_status.results is None:
                    raise RuntimeError(
                        f"Batch failed: {batch_status.batch_error_message}"
                    )
                return batch_status.results
            logging.info(
                "  LLM progress: %d/%d documents completed.",
                batch_status.completed_count,
                batch_status.total_count,
            )

    def score_results(
        self,
        eval_rows: list[dict[str, Any]],
        results: list[LLMExtractionResult],
    ) -> tuple[list[DocumentScore], list[str]]:
        """Scores LLM results against expected values.

        Returns (doc_scores, llm_failure_messages). doc_scores contains one
        DocumentScore per eval row; each DocumentScore.field_scores[field].expected
        and .actual hold the raw values for per-document inspection.
        """
        assert self.eval_config.output_schema is not None
        output_schema = self.eval_config.output_schema

        result_by_doc_id = {r.document_id: r for r in results}

        doc_scores: list[DocumentScore] = []
        llm_failures: list[str] = []

        for expected_row in eval_rows:
            doc_id = str(expected_row["document_id"])
            llm_result = result_by_doc_id.get(doc_id)

            if llm_result is None or llm_result.status != LLMExtractionStatus.SUCCESS:
                doc_scores.append(score_document(expected_row, None, self.eval_config))
                if llm_result is not None:
                    llm_failures.append(f"  {doc_id}: {llm_result.error_message}")
            else:
                assert llm_result.extracted_data is not None
                flat_actual = _flatten_llm_extracted_data(
                    llm_result.extracted_data, output_schema
                )
                doc_scores.append(
                    score_document(expected_row, flat_actual, self.eval_config)
                )

        return doc_scores, llm_failures

    @staticmethod
    def _unit_test_passed(ds: DocumentScore) -> bool:
        if ds.is_relevant_actual is None:
            return False
        if ds.is_relevant_expected != ds.is_relevant_actual:
            return False
        return all(fs.is_match for fs in ds.field_scores.values())

    def format_report(
        self,
        doc_scores: list[DocumentScore],
        llm_failures: list[str],
        eval_rows: list[dict[str, Any]],
    ) -> str:
        """Formats a human-readable accuracy report split by test_type.

        Unit-test rows are shown as named pass/fail cases. Sample rows are
        aggregated into per-field accuracy percentages. Relevance is reported
        separately across all rows.
        """
        assert self.eval_config.output_schema is not None
        output_schema = self.eval_config.output_schema

        row_by_doc_id = {str(r["document_id"]): r for r in eval_rows}

        unit_scores = [
            ds
            for ds in doc_scores
            if row_by_doc_id.get(ds.document_id, {}).get("test_type") == "unit"
        ]
        sample_scores = [
            ds
            for ds in doc_scores
            if row_by_doc_id.get(ds.document_id, {}).get("test_type") != "unit"
        ]

        field_names = [
            f.name for f in output_schema.inferred_fields if f.name != "is_relevant"
        ]

        _GREEN = "\033[32m"
        _RED = "\033[31m"
        _RESET = "\033[0m"

        def _pct(num: int, denom: int) -> str:
            if denom == 0:
                return "  n/a"
            return f"{100.0 * num / denom:5.1f}%"

        def _hr(label: str, width: int = 65) -> str:
            return f"{'━' * 3} {label} {'━' * max(0, width - len(label) - 5)}"

        def _mismatch_lines(ds: DocumentScore) -> list[str]:
            detail: list[str] = []
            if ds.is_relevant_actual is None and ds.is_relevant_expected:
                detail.append("      [no LLM result]")
            elif ds.is_relevant_actual is not None and (
                ds.is_relevant_expected != ds.is_relevant_actual
            ):
                detail.append(
                    f"      is_relevant   expected={ds.is_relevant_expected!r}  "
                    f"actual={ds.is_relevant_actual!r}"
                )
            for fs in ds.field_scores.values():
                if not fs.is_match:
                    detail.append(
                        f"      {fs.field_name:<28s} expected={fs.expected!r}  "
                        f"actual={fs.actual!r}"
                    )
            return detail

        n_total = len(doc_scores)
        n_unit = len(unit_scores)
        n_sample = len(sample_scores)

        lines = [
            "",
            "=" * 65,
            f"Golden Eval Results: {self.extractor.extractor_id}",
            "=" * 65,
            f"State: {self.extractor.state_code.value}  |  Collection: {self.extractor.collection_name}",
            f"Total rows: {n_total}  ({n_unit} unit · {n_sample} sample)",
        ]

        # ── Unit tests ──────────────────────────────────────────────────────
        if unit_scores:
            lines += ["", _hr("Unit Tests")]
            n_passed = 0
            for ds in unit_scores:
                row = row_by_doc_id.get(ds.document_id, {})
                test_case = str(row.get("test_case") or ds.document_id)
                passed = self._unit_test_passed(ds)
                if passed:
                    n_passed += 1
                status = f"{_GREEN}PASS{_RESET}" if passed else f"{_RED}FAIL{_RESET}"
                lines.append(f"  {status}  {test_case:<30s}  {ds.document_id}")
                if not passed:
                    lines.extend(_mismatch_lines(ds))
            n_failed = n_unit - n_passed
            lines += ["", f"  {n_unit} total · {n_passed} passed · {n_failed} failed"]

        # ── Sample accuracy ──────────────────────────────────────────────────
        scored_samples = [
            ds for ds in sample_scores if ds.is_relevant_expected and ds.field_scores
        ]
        n_scored = len(scored_samples)
        if scored_samples:
            per_field_matches: dict[str, int] = {name: 0 for name in field_names}
            n_all_correct = 0
            for ds in scored_samples:
                all_correct = True
                for fname in field_names:
                    fs = ds.field_scores.get(fname)
                    if fs is not None and fs.is_match:
                        per_field_matches[fname] += 1
                    else:
                        all_correct = False
                if all_correct:
                    n_all_correct += 1

            lines += ["", _hr(f"Sample Accuracy  (n={n_scored})")]
            col = max(len(n) for n in field_names) + 2
            for fname in field_names:
                n_match = per_field_matches[fname]
                lines.append(
                    f"  {fname:<{col}s}  {n_match:3d}/{n_scored}   {_pct(n_match, n_scored)}"
                )
            lines += [
                "  " + "─" * (col + 18),
                f"  {'All fields correct':<{col}s}  {n_all_correct:3d}/{n_scored}   {_pct(n_all_correct, n_scored)}",
            ]

        # ── Relevance (all rows) ─────────────────────────────────────────────
        n_expected_relevant = sum(1 for ds in doc_scores if ds.is_relevant_expected)
        n_predicted_relevant = sum(
            1
            for ds in doc_scores
            if ds.is_relevant_actual is not None and ds.is_relevant_actual
        )
        n_relevance_correct = sum(
            1
            for ds in doc_scores
            if ds.is_relevant_actual is not None
            and ds.is_relevant_expected == ds.is_relevant_actual
        )
        lines += [
            "",
            _hr(f"Relevance  (all rows, n={n_total})"),
            f"  Expected relevant:   {n_expected_relevant:3d}/{n_total}   {_pct(n_expected_relevant, n_total)}",
            f"  Predicted relevant:  {n_predicted_relevant:3d}/{n_total}   {_pct(n_predicted_relevant, n_total)}",
            f"  Relevance accuracy:  {n_relevance_correct:3d}/{n_total}   {_pct(n_relevance_correct, n_total)}",
        ]

        if llm_failures:
            lines += ["", f"LLM FAILURES ({len(llm_failures)})", *llm_failures]

        lines.append("")
        return "\n".join(lines)

    def format_mismatches(
        self,
        doc_scores: list[DocumentScore],
        eval_rows: list[dict[str, Any]],
    ) -> str:
        """Returns a detailed listing of every per-field mismatch across all documents."""
        row_by_doc_id = {str(r["document_id"]): r for r in eval_rows}

        lines: list[str] = ["", "━━━ Mismatches " + "━" * 50]
        found_any = False

        for ds in doc_scores:
            doc_lines: list[str] = []

            if ds.is_relevant_actual is not None and (
                ds.is_relevant_expected != ds.is_relevant_actual
            ):
                doc_lines.append(
                    f"  is_relevant   expected={ds.is_relevant_expected!r}  "
                    f"actual={ds.is_relevant_actual!r}"
                )

            if ds.is_relevant_actual is None and ds.is_relevant_expected:
                doc_lines.append("  [no LLM result]")

            for fs in ds.field_scores.values():
                if not fs.is_match:
                    doc_lines.append(
                        f"  {fs.field_name:<30s} expected={fs.expected!r}  "
                        f"actual={fs.actual!r}"
                    )

            if doc_lines:
                found_any = True
                row = row_by_doc_id.get(ds.document_id, {})
                test_type = str(row.get("test_type") or "sample")
                if test_type == "unit":
                    test_case = str(row.get("test_case") or ds.document_id)
                    header = f"[unit] {test_case}  ({ds.document_id})"
                else:
                    header = f"[sample] {ds.document_id}"
                lines.append(f"\n{header}")
                lines.extend(doc_lines)

        if not found_any:
            lines.append("  (none)")
        lines.append("")
        return "\n".join(lines)

    def score_and_report(
        self,
        eval_rows: list[dict[str, Any]],
        results: list[LLMExtractionResult],
    ) -> str:
        """Convenience wrapper: scores results and returns the formatted report string."""
        doc_scores, llm_failures = self.score_results(eval_rows, results)
        return self.format_report(doc_scores, llm_failures, eval_rows)

    def write_results_table(
        self,
        bq_client: BigQueryClient,
        eval_rows: list[dict[str, Any]],
        doc_scores: list[DocumentScore],
        run_timestamp: datetime.datetime,
    ) -> BigQueryAddress:
        """Writes per-document eval results to a sandbox BQ table, replacing any
        previous run. The table expires 24 hours after this write.

        Returns the address of the written table.
        """
        sandbox_dataset = BigQueryAddressOverrides.format_sandbox_dataset(
            self.sandbox_dataset_prefix, _EVAL_DATASET_ID
        )
        table_id = f"eval_results_{self.extractor.extractor_id.lower()}"
        address = BigQueryAddress(dataset_id=sandbox_dataset, table_id=table_id)

        row_by_doc_id = {str(r["document_id"]): r for r in eval_rows}

        rows: list[dict[str, Any]] = []
        for ds in doc_scores:
            eval_row = row_by_doc_id.get(ds.document_id, {})

            is_relevant_match: bool | None = (
                ds.is_relevant_expected == ds.is_relevant_actual
                if ds.is_relevant_actual is not None
                else None
            )

            field_results = [
                {
                    "field_name": fs.field_name,
                    "expected": json.dumps(fs.expected)
                    if fs.expected is not None
                    else None,
                    "actual": json.dumps(fs.actual) if fs.actual is not None else None,
                    "is_match": fs.is_match,
                }
                for fs in ds.field_scores.values()
            ]

            all_fields_match: bool | None = (
                all(fs.is_match for fs in ds.field_scores.values())
                if ds.field_scores
                else None
            )

            rows.append(
                {
                    "run_timestamp": run_timestamp.isoformat(),
                    "extractor_id": self.extractor.extractor_id,
                    "state_code": self.extractor.state_code.value,
                    "collection_name": self.extractor.collection_name,
                    "llm_provider": self.extractor.llm_provider,
                    "model": self.extractor.model,
                    "thinking_budget": self.extractor.thinking_budget,
                    "document_id": ds.document_id,
                    "test_type": eval_row.get("test_type") or None,
                    "test_case": eval_row.get("test_case") or None,
                    "document_text": eval_row.get("document_text", ""),
                    "is_relevant_expected": ds.is_relevant_expected,
                    "is_relevant_actual": ds.is_relevant_actual,
                    "is_relevant_match": is_relevant_match,
                    "field_results": field_results,
                    "all_fields_match": all_fields_match,
                    "difficulty": eval_row.get("difficulty") or None,
                }
            )

        bq_client.create_dataset_if_necessary(sandbox_dataset)
        bq_client.delete_table(address, not_found_ok=True)
        bq_client.create_table_with_schema(
            address=address,
            schema_fields=_EVAL_RESULTS_SCHEMA,
        )
        expiry = run_timestamp + datetime.timedelta(hours=_EVAL_RESULTS_EXPIRY_HOURS)
        bq_client.set_table_expiration(address, expiry)
        bq_client.load_into_table_async(address=address, rows=rows).result()

        return address

    def run(
        self, bq_client: BigQueryClient, show_mismatches: bool = False
    ) -> list[DocumentScore]:
        """Runs the full eval pipeline: deploy → read → LLM → score → report.

        Returns the per-document DocumentScore list for further inspection.
        Each DocumentScore.field_scores[field_name].expected and .actual hold
        the raw values that went into each comparison.
        """
        logging.info(
            "Step 1: Deploying eval source table for '%s' to sandbox prefix '%s'...",
            self.extractor.collection_name.lower(),
            self.sandbox_dataset_prefix,
        )
        self.deploy_eval_table(bq_client)

        logging.info("Step 2: Reading eval rows from sandbox BQ table...")
        eval_rows = self.read_eval_rows(bq_client)
        logging.info(
            "  Found %d eval rows for state %s.",
            len(eval_rows),
            self.extractor.state_code.value,
        )
        if not eval_rows:
            logging.warning(
                "No eval rows found for state %s in collection %s. "
                "Check that the eval sheet has rows with state_code='%s'.",
                self.extractor.state_code.value,
                self.extractor.collection_name,
                self.extractor.state_code.value,
            )
            return []

        logging.info(
            "Step 3: Uploading eval documents to sandbox bucket '%s'...",
            self.sandbox_documents_bucket,
        )
        documents = self.upload_eval_documents_to_gcs(eval_rows)

        logging.info(
            "Step 4: Running LLM (%s/%s) via LiteLLMClient for %d documents...",
            self.extractor.llm_provider,
            self.extractor.model,
            len(documents),
        )
        results = self.run_llm(documents)
        n_success = sum(1 for r in results if r.status == LLMExtractionStatus.SUCCESS)
        logging.info("  %d/%d LLM calls succeeded.", n_success, len(results))

        logging.info("Step 5: Scoring results...")
        run_timestamp = datetime.datetime.now(datetime.timezone.utc)
        doc_scores, llm_failures = self.score_results(eval_rows, results)

        logging.info("Step 6: Writing results table to BQ sandbox...")
        table_address = self.write_results_table(
            bq_client, eval_rows, doc_scores, run_timestamp
        )
        logging.info(
            "  Results table: %s.%s (expires in %dh)",
            table_address.dataset_id,
            table_address.table_id,
            _EVAL_RESULTS_EXPIRY_HOURS,
        )

        logging.info("Step 7: Aggregating and printing results...")
        print(self.format_report(doc_scores, llm_failures, eval_rows))
        if show_mismatches:
            print(self.format_mismatches(doc_scores, eval_rows))

        print(
            f"BQ query to inspect results:\n\n"
            f"  SELECT * FROM `{bq_client.project_id}.{table_address.dataset_id}"
            f".{table_address.table_id}`\n"
            f"  ORDER BY all_fields_match ASC, test_type DESC;\n"
        )

        return doc_scores
