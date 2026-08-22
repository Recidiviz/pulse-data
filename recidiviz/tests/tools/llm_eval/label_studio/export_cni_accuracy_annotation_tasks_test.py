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
"""Tests for export_cni_accuracy_annotation_tasks.py."""
import json
import random
from typing import Any
from unittest import TestCase
from unittest.mock import patch

import attr

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.extraction_results_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    EXTRACTOR_VERSION_ID_COLUMN_NAME,
    RESULT_JSON_COLUMN_NAME,
    STATE_CODE_COLUMN_NAME,
)
from recidiviz.documents.extraction.extraction_results_narrowing import (
    ExtractionResultsNarrowing,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
    LLMOutputFieldType,
    PrimitiveScalarLLMRequestOutputSchemaField,
)
from recidiviz.documents.store.document_store_columns import DOCUMENT_TEXT_COLUMN_NAME
from recidiviz.llm_eval.document_extraction.document_extraction_annotatable_record import (
    DocumentExtractionAnnotatableRecord,
)
from recidiviz.llm_eval.document_extraction.document_extraction_field_value import (
    DocumentExtractionFieldValue,
)
from recidiviz.llm_eval.label_studio.models.document_extraction.cni_accuracy_per_field_task_data import (
    NULL_VALUE_DISPLAY_TEXT,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_extractor_irrelevant_result_content,
    build_fake_extractor_result_content,
    wrap_in_result_key,
)
from recidiviz.tools.llm_eval.label_studio.document_extraction_annotation_batch_position import (
    DocumentExtractionAnnotationBatchPosition,
)
from recidiviz.tools.llm_eval.label_studio.export_cni_accuracy_annotation_tasks import (
    build_task_data_for_field_value,
    export_accuracy_tasks,
    order_field_values_by_document,
    select_annotatable_record_sample,
    task_output_path,
)
from recidiviz.utils.types import assert_type

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_DOCUMENT_ID = "doc_a"
_DOCUMENT_TEXT = "Client is on dish duty at $12.50 an hour."
_VERSION = "v_sampled"
_PROJECT_ID = "recidiviz-staging"
_SEED = 7

_MODULE_UNDER_TEST = (
    "recidiviz.tools.llm_eval.label_studio.export_cni_accuracy_annotation_tasks"
)


def _field_value(
    *,
    field_name: str = "location",
    extracted_value: Any = "Kitchen",
    confidence_level: ConfidenceLevel | None = ConfidenceLevel.EXPLICIT,
    array_field_name: str | None = None,
    source_array_index: int | None = None,
    array_element_json: str | None = None,
    document_contents_id: str = _DOCUMENT_ID,
) -> DocumentExtractionFieldValue:
    return DocumentExtractionFieldValue(
        state_code=_STATE_CODE,
        document_contents_id=document_contents_id,
        document_text=_DOCUMENT_TEXT,
        extractor_version_id=_VERSION,
        field_name=field_name,
        array_field_name=array_field_name,
        source_array_index=source_array_index,
        extracted_value=extracted_value,
        confidence_level=confidence_level,
        array_element_json=array_element_json,
    )


def _top_level_record(
    *,
    document_contents_id: str,
    extracted_value: Any = "Kitchen",
    confidence_level: ConfidenceLevel | None = ConfidenceLevel.EXPLICIT,
) -> DocumentExtractionAnnotatableRecord:
    return DocumentExtractionAnnotatableRecord(
        field_values=[
            _field_value(
                document_contents_id=document_contents_id,
                extracted_value=extracted_value,
                confidence_level=confidence_level,
            )
        ]
    )


def _array_element_record(
    *, document_contents_id: str, source_array_index: int
) -> DocumentExtractionAnnotatableRecord:
    return DocumentExtractionAnnotatableRecord(
        field_values=[
            _field_value(
                field_name="assignment_name",
                extracted_value="Dish duty",
                array_field_name="assignments",
                source_array_index=source_array_index,
                array_element_json='{"assignment_name": "Dish duty"}',
                document_contents_id=document_contents_id,
            )
        ]
    )


def _inferred_records(count: int) -> list[DocumentExtractionAnnotatableRecord]:
    return [
        _top_level_record(
            document_contents_id=f"inferred_{i}",
            confidence_level=ConfidenceLevel.INFERRED,
        )
        for i in range(count)
    ]


def _non_inferred_records(count: int) -> list[DocumentExtractionAnnotatableRecord]:
    return [
        _top_level_record(
            document_contents_id=f"explicit_{i}",
            confidence_level=ConfidenceLevel.EXPLICIT,
        )
        for i in range(count)
    ]


def _null_records(count: int) -> list[DocumentExtractionAnnotatableRecord]:
    return [
        _top_level_record(document_contents_id=f"null_{i}", extracted_value=None)
        for i in range(count)
    ]


class SelectAnnotatableRecordSampleTest(TestCase):
    """Tests the composition of the sample the export writes."""

    def _select(
        self,
        records: list[DocumentExtractionAnnotatableRecord],
        *,
        non_null_percent: int = 100 - 1,
        inferred_percent: int = 30,
    ) -> list[DocumentExtractionAnnotatableRecord]:
        return select_annotatable_record_sample(
            records,
            non_null_percent=non_null_percent,
            inferred_percent=inferred_percent,
            rng=random.Random(_SEED),
        )

    @staticmethod
    def _counts(
        selected: list[DocumentExtractionAnnotatableRecord],
    ) -> tuple[int, int, int, int]:
        """Returns the (inferred, non-inferred, null, array element) counts of a sample."""
        top_level = [record for record in selected if not record.is_array_element]
        non_null_top_level = [record for record in top_level if not record.is_null]
        return (
            sum(1 for r in non_null_top_level if r.has_inferred_confidence),
            sum(1 for r in non_null_top_level if not r.has_inferred_confidence),
            sum(1 for r in top_level if r.is_null),
            sum(1 for r in selected if r.is_array_element),
        )

    def test_keeps_scarce_inferred_pile_whole_and_caps_the_other(self) -> None:
        # 20 of 220 non-null records are inferred, which is under the 30% target, so all 20
        # stay and the non-inferred pile is cut to the 47 that make them 30% of the result.
        selected = self._select(
            _inferred_records(20) + _non_inferred_records(200), inferred_percent=30
        )
        self.assertEqual((20, 47, 0, 0), self._counts(selected))

    def test_keeps_scarce_non_inferred_pile_whole(self) -> None:
        # The other way round: only 20 non-inferred records exist, so all of them stay and
        # the inferred pile is cut to 9.
        selected = self._select(
            _inferred_records(200) + _non_inferred_records(20), inferred_percent=30
        )
        self.assertEqual((9, 20, 0, 0), self._counts(selected))

    def test_only_inferred_records_available(self) -> None:
        selected = self._select(_inferred_records(5), inferred_percent=30)
        self.assertEqual((5, 0, 0, 0), self._counts(selected))

    def test_only_non_inferred_records_available(self) -> None:
        selected = self._select(_non_inferred_records(5), inferred_percent=30)
        self.assertEqual((0, 5, 0, 0), self._counts(selected))

    def test_array_element_records_are_always_kept(self) -> None:
        """An array element's values have to be exported together, so elements sit out the
        inferred pass rather than being trimmed by it.
        """
        array_elements = [
            _array_element_record(document_contents_id="doc_a", source_array_index=i)
            for i in range(3)
        ]
        selected = self._select(
            array_elements + _inferred_records(2) + _non_inferred_records(2),
            inferred_percent=50,
        )
        self.assertEqual((2, 2, 0, 3), self._counts(selected))

    def test_null_records_admitted_up_to_their_share(self) -> None:
        # 8 non-null records at a 80% target leaves room for 2 nulls.
        selected = self._select(
            _inferred_records(4) + _non_inferred_records(4) + _null_records(5),
            non_null_percent=80,
            inferred_percent=50,
        )
        self.assertEqual((4, 4, 2, 0), self._counts(selected))

    def test_records_are_never_duplicated(self) -> None:
        records = _inferred_records(20) + _non_inferred_records(200)
        selected = self._select(records, inferred_percent=30)
        selected_ids = [record.document_contents_id for record in selected]
        self.assertEqual(len(selected_ids), len(set(selected_ids)))

    def test_same_seed_gives_the_same_sample(self) -> None:
        records = _inferred_records(20) + _non_inferred_records(200) + _null_records(20)
        self.assertEqual(
            [
                record.document_contents_id
                for record in self._select(records, non_null_percent=80)
            ],
            [
                record.document_contents_id
                for record in self._select(records, non_null_percent=80)
            ],
        )

    def test_empty_pool_yields_an_empty_sample(self) -> None:
        self.assertEqual([], self._select([]))

    def test_non_null_percent_out_of_range_raises(self) -> None:
        for out_of_range in (0, 100):
            with self.subTest(non_null_percent=out_of_range):
                with self.assertRaisesRegex(
                    ValueError,
                    r"^--non_null_percent must be between 1 and 99, got "
                    rf"\[{out_of_range}\]$",
                ):
                    self._select(
                        _non_inferred_records(1), non_null_percent=out_of_range
                    )

    def test_inferred_percent_out_of_range_raises(self) -> None:
        for out_of_range in (0, 100):
            with self.subTest(inferred_percent=out_of_range):
                with self.assertRaisesRegex(
                    ValueError,
                    r"^--inferred_percent must be between 1 and 99, got "
                    rf"\[{out_of_range}\]$",
                ):
                    self._select(
                        _non_inferred_records(1), inferred_percent=out_of_range
                    )


class OrderFieldValuesByDocumentTest(TestCase):
    """Tests the order field values are written in."""

    def test_pinned_fields_come_first_within_each_document(self) -> None:
        ordered = order_field_values_by_document(
            pinned_field_values=[
                _field_value(field_name="primary_status", document_contents_id="doc_b"),
                _field_value(field_name="primary_status", document_contents_id="doc_a"),
            ],
            selected_records=[
                _top_level_record(document_contents_id="doc_a"),
                _top_level_record(document_contents_id="doc_b"),
            ],
        )
        # doc_b leads because it is the first document seen among the pinned values.
        self.assertEqual(
            [
                ("doc_b", "primary_status"),
                ("doc_b", "location"),
                ("doc_a", "primary_status"),
                ("doc_a", "location"),
            ],
            [
                (field_value.document_contents_id, field_value.field_name)
                for field_value in ordered
            ],
        )

    def test_a_records_values_stay_together(self) -> None:
        record = DocumentExtractionAnnotatableRecord(
            field_values=[
                _field_value(
                    field_name="assignment_name",
                    extracted_value="Dish duty",
                    array_field_name="assignments",
                    source_array_index=0,
                    array_element_json="{}",
                ),
                _field_value(
                    field_name="rate_amount",
                    extracted_value=12.5,
                    array_field_name="assignments",
                    source_array_index=0,
                    array_element_json="{}",
                ),
            ]
        )
        ordered = order_field_values_by_document(
            pinned_field_values=[], selected_records=[record]
        )
        self.assertEqual(
            ["assignment_name", "rate_amount"],
            [field_value.field_name for field_value in ordered],
        )

    def test_documents_only_in_records_are_included(self) -> None:
        ordered = order_field_values_by_document(
            pinned_field_values=[
                _field_value(field_name="primary_status", document_contents_id="doc_a")
            ],
            selected_records=[_top_level_record(document_contents_id="doc_c")],
        )
        self.assertEqual(
            ["doc_a", "doc_c"],
            [field_value.document_contents_id for field_value in ordered],
        )


class BuildTaskDataForFieldValueTest(TestCase):
    """Tests the payload built for one annotated value."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema
        self.position = DocumentExtractionAnnotationBatchPosition(
            doc_index=1, field_index=2, total_fields=3, task_order=2
        )

    def _task_data(self, field_value: DocumentExtractionFieldValue) -> dict[str, Any]:
        return build_task_data_for_field_value(
            field_value=field_value,
            output_schema=self.output_schema,
            prompt_description="Pull assignment details out of case notes.",
            position=self.position,
        ).task_data

    def test_builds_the_whole_payload(self) -> None:
        self.assertEqual(
            {
                "state_code": "US_XX",
                "document_id": "doc_a",
                "document_text": _DOCUMENT_TEXT,
                "prompt_description": "Pull assignment details out of case notes.",
                "field_name": "location",
                "field_description": "The location associated with the record.",
                "group": "",
                "extracted_value": "Kitchen",
                "confidence_level": "explicit",
                "array_element_json": None,
                "extractor_version_id": _VERSION,
                "doc_index": 1,
                "field_index": 2,
                "total_fields": 3,
                "task_order": 2,
            },
            self._task_data(_field_value()),
        )

    def test_array_element_value_carries_its_group_and_element_json(self) -> None:
        task_data = self._task_data(
            _field_value(
                field_name="assignment_name",
                extracted_value="Dish duty",
                array_field_name="assignments",
                source_array_index=1,
                array_element_json='{"assignment_name": "Dish duty"}',
            )
        )
        self.assertEqual("assignments[1]", task_data["group"])
        self.assertEqual(
            '{"assignment_name": "Dish duty"}', task_data["array_element_json"]
        )
        self.assertEqual("Name of the assignment.", task_data["field_description"])

    def test_null_value_is_shown_as_the_display_text(self) -> None:
        """The annotator has to be able to read the claim that the document says nothing
        about this field in order to disagree with it.
        """
        self.assertEqual(
            NULL_VALUE_DISPLAY_TEXT,
            self._task_data(_field_value(extracted_value=None))["extracted_value"],
        )

    def test_missing_confidence_level_stays_none(self) -> None:
        self.assertIsNone(
            self._task_data(_field_value(confidence_level=None))["confidence_level"]
        )

    def test_non_string_value_is_rendered_for_reading(self) -> None:
        self.assertEqual(
            "12.5",
            self._task_data(
                _field_value(
                    field_name="rate_amount",
                    extracted_value=12.5,
                    array_field_name="assignments",
                    source_array_index=0,
                    array_element_json="{}",
                )
            )["extracted_value"],
        )


class TaskOutputPathTest(TestCase):
    """Tests where one task's JSON is written."""

    def test_path_shape(self) -> None:
        self.assertEqual(
            "gs://my-bucket/cni-labeling/accuracy_per_field/v_sampled/"
            "doc_a__007__assignments_1__rate_amount.json",
            task_output_path(
                target_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://my-bucket/cni-labeling"
                ),
                field_value=_field_value(
                    field_name="rate_amount",
                    extracted_value=12.5,
                    array_field_name="assignments",
                    source_array_index=1,
                    array_element_json="{}",
                ),
                position=DocumentExtractionAnnotationBatchPosition(
                    doc_index=1, field_index=7, total_fields=9, task_order=7
                ),
            ).uri(),
        )

    def test_field_index_is_zero_padded_so_a_listing_sorts_by_work_order(self) -> None:
        paths = [
            task_output_path(
                target_directory=GcsfsDirectoryPath.from_absolute_path(
                    "gs://my-bucket/cni-labeling"
                ),
                field_value=_field_value(field_name="location"),
                position=DocumentExtractionAnnotationBatchPosition(
                    doc_index=1, field_index=field_index, total_fields=10, task_order=1
                ),
            ).uri()
            for field_index in (2, 10)
        ]
        self.assertEqual(sorted(paths), paths)


class ExportAccuracyTasksTest(TestCase):
    """Tests the export end to end, with the query results and GCS stubbed out."""

    def setUp(self) -> None:
        self.extractor_config = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        )
        self.fs = FakeGCSFileSystem()
        gcsfs_patcher = patch(f"{_MODULE_UNDER_TEST}.GcsfsFactory")
        self.addCleanup(gcsfs_patcher.stop)
        gcsfs_patcher.start().build.return_value = self.fs

        bq_patcher = patch(f"{_MODULE_UNDER_TEST}.BigQueryClientImpl")
        self.addCleanup(bq_patcher.stop)
        self.mock_bq_client = bq_patcher.start()

    def _set_query_results(self, rows: list[dict[str, Any]]) -> None:
        self.mock_bq_client.return_value.run_query_async.return_value.result.return_value = (
            rows
        )

    @staticmethod
    def _result_row(
        *, document_contents_id: str, result_content: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            STATE_CODE_COLUMN_NAME: _STATE_CODE.value,
            DOCUMENT_CONTENTS_ID_COLUMN_NAME: document_contents_id,
            EXTRACTOR_VERSION_ID_COLUMN_NAME: _VERSION,
            RESULT_JSON_COLUMN_NAME: json.dumps(wrap_in_result_key(result_content)),
            DOCUMENT_TEXT_COLUMN_NAME: _DOCUMENT_TEXT,
        }

    @staticmethod
    def _result_content_with_empty_assignments() -> dict[str, Any]:
        """Returns a result naming a status but no location, with assignments present and
        empty, so the document yields three annotatable values. An empty array claims the
        document names no assignments, and that claim can be wrong.
        """
        return build_fake_extractor_result_content(
            primary_status="active",
            status_note="Currently on dish duty.",
            location=None,
            assignments=[],
        )

    def _export(
        self,
        *,
        required_fields: list[str] | None = None,
        extractor_config: Any = None,
    ) -> None:
        export_accuracy_tasks(
            project_id=_PROJECT_ID,
            extractor_config=extractor_config or self.extractor_config,
            results_narrowing=ExtractionResultsNarrowing(
                extractor_version_id=_VERSION, extraction_job_id=None
            ),
            sandbox_dataset_prefix=None,
            target_directory=GcsfsDirectoryPath.from_absolute_path(
                "gs://my-bucket/cni-labeling"
            ),
            sample_size=10,
            non_null_percent=50,
            inferred_percent=30,
            required_fields=required_fields,
            random_seed=_SEED,
        )

    def test_empty_array_is_annotated_on_the_array_field(self) -> None:
        """An empty assignments array claims the document names no assignments, so it gets
        a task of its own alongside the two scalar fields. One file per annotated value.
        """
        self._set_query_results(
            [
                self._result_row(
                    document_contents_id=_DOCUMENT_ID,
                    result_content=self._result_content_with_empty_assignments(),
                )
            ]
        )
        # location came back null, so pinning it keeps it out of the sampled pool. That
        # leaves primary_status to anchor the non-null share and admit the null assignments
        # record, so all three of the document's values are exported.
        self._export(required_fields=["location"])
        self.assertEqual(
            [
                "gs://my-bucket/cni-labeling/accuracy_per_field/v_sampled/"
                "doc_a__001__location.json",
                "gs://my-bucket/cni-labeling/accuracy_per_field/v_sampled/"
                "doc_a__002__primary_status.json",
                "gs://my-bucket/cni-labeling/accuracy_per_field/v_sampled/"
                "doc_a__003__assignments.json",
            ],
            sorted(path.uri() for path in self.fs.all_paths),
        )
        [assignments_path] = [
            assert_type(path, GcsfsFilePath)
            for path in self.fs.all_paths
            if "assignments" in path.abs_path()
        ]
        self.assertEqual(
            [
                {
                    "data": {
                        "state_code": "US_XX",
                        "document_id": "doc_a",
                        "document_text": _DOCUMENT_TEXT,
                        "prompt_description": (
                            "Extract fictional assignment information to exercise the "
                            "parser."
                        ),
                        "field_name": "assignments",
                        "field_description": "Assignments mentioned in the document.",
                        "group": "",
                        "extracted_value": NULL_VALUE_DISPLAY_TEXT,
                        # An ARRAY_OF_STRUCT field carries no companion-metadata wrapper of
                        # its own, so it reports no confidence.
                        "confidence_level": None,
                        "array_element_json": None,
                        "extractor_version_id": _VERSION,
                        "doc_index": 1,
                        "field_index": 3,
                        "total_fields": 3,
                        "task_order": 3,
                    }
                }
            ],
            json.loads(self.fs.download_as_string(assignments_path)),
        )

    def test_written_file_holds_the_task_payload(self) -> None:
        self._set_query_results(
            [
                self._result_row(
                    document_contents_id=_DOCUMENT_ID,
                    result_content=self._result_content_with_empty_assignments(),
                )
            ]
        )
        self._export(required_fields=["location"])
        [location_path] = [
            assert_type(path, GcsfsFilePath)
            for path in self.fs.all_paths
            if "location" in path.abs_path()
        ]
        self.assertEqual(
            [
                {
                    "data": {
                        "state_code": "US_XX",
                        "document_id": "doc_a",
                        "document_text": _DOCUMENT_TEXT,
                        "prompt_description": (
                            "Extract fictional assignment information to exercise the "
                            "parser."
                        ),
                        "field_name": "location",
                        "field_description": (
                            "The location associated with the record."
                        ),
                        "group": "",
                        "extracted_value": NULL_VALUE_DISPLAY_TEXT,
                        "confidence_level": "explicit",
                        "array_element_json": None,
                        "extractor_version_id": _VERSION,
                        "doc_index": 1,
                        "field_index": 1,
                        "total_fields": 3,
                        "task_order": 1,
                    }
                }
            ],
            json.loads(self.fs.download_as_string(location_path)),
        )

    def test_no_results_writes_nothing(self) -> None:
        self._set_query_results([])
        self._export()
        self.assertEqual([], self.fs.all_paths)

    def test_irrelevant_document_writes_nothing(self) -> None:
        """An irrelevant result is exactly `{"is_relevant": false}`. It carries none of the
        fields the schema declares, so it holds no extracted value to annotate. is_relevant
        itself is STRUCTURAL, so no task asks whether the relevance call was right either.
        """
        self._set_query_results(
            [
                self._result_row(
                    document_contents_id=_DOCUMENT_ID,
                    result_content=build_fake_extractor_irrelevant_result_content(),
                )
            ]
        )
        self._export()
        self.assertEqual([], self.fs.all_paths)

    def test_unknown_required_field_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^--required_fields names field\(s\) \['not_a_field'\] that extractor "
            r"\[US_XX_FAKE_EXTRACTOR_COLLECTION\] does not annotate\. Annotatable "
            r"fields: \[.*'primary_status'.*\]\.$",
        ):
            self._export(required_fields=["not_a_field"])

    def test_required_array_sub_field_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^--required_fields names array field\(s\) or array sub-field\(s\) "
            r"\['rate_amount'\], which cannot be pinned\. .*Pinnable fields: "
            r"\['location', 'primary_status'\]\.$",
        ):
            self._export(required_fields=["rate_amount"])

    def test_required_array_field_raises(self) -> None:
        """An array field itself is annotatable, but only for a document whose array came
        back empty, so it cannot be pinned for every sampled document.
        """
        with self.assertRaisesRegex(
            ValueError,
            r"^--required_fields names array field\(s\) or array sub-field\(s\) "
            r"\['assignments'\], which cannot be pinned\. .*Pinnable fields: "
            r"\['location', 'primary_status'\]\.$",
        ):
            self._export(required_fields=["assignments"])

    def test_extractor_with_no_inferred_fields_raises(self) -> None:
        structural_only_schema = LLMRequestOutputSchema(
            full_batch_description="Batch of parsed fake documents for testing.",
            result_level_description="One parsed fake document for testing.",
            # Has to match the collection's, which the config validates against.
            relevance_criteria=(
                self.extractor_config.extractor_collection.relevance_criteria
            ),
            user_defined_fields=[
                PrimitiveScalarLLMRequestOutputSchemaField(
                    name="status_note",
                    description="A bare note about the record's status.",
                    required=True,
                    inferred_field_config=None,
                    scalar_type=LLMOutputFieldType.STRING,
                )
            ],
        )
        with self.assertRaisesRegex(
            ValueError,
            r"^Extractor \[US_XX_FAKE_EXTRACTOR_COLLECTION\] declares no INFERRED "
            r"fields, so it has no extracted values to annotate\.$",
        ):
            self._export(
                extractor_config=attr.evolve(
                    self.extractor_config,
                    extractor_collection=attr.evolve(
                        self.extractor_config.extractor_collection,
                        output_schema=structural_only_schema,
                    ),
                )
            )
