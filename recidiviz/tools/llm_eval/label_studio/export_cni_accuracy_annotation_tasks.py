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
"""Exports CNI per-field accuracy annotation tasks to GCS for Label Studio import.

Sampling strategy:
  1. Sample --sample_size documents from a single extraction run via
     DocumentExtractionResultSampleQueryBuilder, then read each result through
     LLMRequestOutputValues to get its INFERRED field values.
  2. Group those values into annotatable records, one per top-level field and one per
     array element, since an element's values only make sense read together. Then select
     which records to export:
       - At least --non_null_percent of exported records are non-null (default: 70%).
         Null records are included so annotators can check for false negatives.
       - Among non-null top-level records, --inferred_percent have
         confidence_level='inferred' (default: 30%).

Parameter enforcement:
  Strictly enforced, as hard filters the sample cannot violate:
    --state_code, --collection_name: Select the extractor config the whole run is
      built from.
    --extractor_version_id, --extraction_job_id: Narrow the extraction results read
      to one run; only that run's results are eligible.
    --sample_size: Hard LIMIT in SQL; you get at most this many documents.
    --required_fields: Named fields are always written for every sampled document,
      regardless of the null/inferred mix. Only scalar top-level fields can be named,
      since only those hold exactly one value in every document.

  Best-effort targets, honored when the data allows:
    --non_null_percent: The script caps how many null records it admits so that
      non-null records make up at least this share. If the run has too few non-null
      records to anchor the ratio, the null share will exceed the target.
    --inferred_percent: Among non-null top-level records, the script keeps every record
      from whichever pile (inferred or non-inferred) is scarcer, then caps the other to
      hit the ratio. Array element records are exempt and always kept, since an
      element's values have to be exported together.

      Both are ratios on the result, hit by discarding records, so the export is only as
      large as the scarcest pile allows. That can be far smaller than --sample_size
      suggests.
    --random_seed: Controls Python-side shuffles only. The SQL document sample
      (ORDER BY FARM_FINGERPRINT) is deterministic on document ID regardless.

Output path (one JSON file per document-field pair):
  {target_path}/accuracy_per_field/{extractor_version_id}/{document_id}__{field_index_str}__{field_id}.json

Usage:
    python -m recidiviz.tools.llm_eval.label_studio.export_cni_accuracy_annotation_tasks \\
        --project_id recidiviz-staging \\
        --state_code US_CO \\
        --collection_name CASE_NOTE_EMPLOYMENT_INFO \\
        --extractor_version_id v1 \\
        --target_bucket recidiviz-staging-my-scratch \\
        --target_path label_studio/accuracy/co_round1 \\
        [--sandbox_dataset_prefix my_prefix] \\
        [--extraction_job_id abc123] \\
        [--sample_size 75] \\
        [--non_null_percent 70] \\
        [--inferred_percent 30] \\
        [--required_fields primary_status] \\
        [--random_seed 42]
"""
import argparse
import json
import logging
import random
import sys
from collections import defaultdict

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
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
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.store.document_store_columns import DOCUMENT_TEXT_COLUMN_NAME
from recidiviz.llm_eval.document_extraction.document_extraction_annotatable_field_utils import (
    annotatable_field_description,
    annotatable_field_names,
    scalar_top_level_annotatable_field_names,
)
from recidiviz.llm_eval.document_extraction.document_extraction_annotatable_record import (
    DocumentExtractionAnnotatableRecord,
)
from recidiviz.llm_eval.document_extraction.document_extraction_field_value import (
    DocumentExtractionFieldValue,
)
from recidiviz.llm_eval.document_extraction.document_extraction_result_sample_query_builder import (
    DocumentExtractionResultSampleQueryBuilder,
)
from recidiviz.llm_eval.label_studio.models.document_extraction.cni_accuracy_per_field_task_data import (
    NULL_VALUE_DISPLAY_TEXT,
    CNIAccuracyPerFieldTaskData,
)
from recidiviz.tools.llm_eval.label_studio.document_extraction_annotation_batch_position import (
    DocumentExtractionAnnotationBatchPosition,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Subdirectory of the target directory the per-field tasks are written under, keeping this
# export's files apart from any other annotation export sharing the same target.
_TASK_SUBDIRECTORY = "accuracy_per_field"


def select_annotatable_record_sample(
    records: list[DocumentExtractionAnnotatableRecord],
    *,
    non_null_percent: int,
    inferred_percent: int,
    rng: random.Random,
) -> list[DocumentExtractionAnnotatableRecord]:
    """Returns the subset of |records| whose mix matches the requested composition.

    Records are only ever discarded, never duplicated, so the result is as large as the
    requested ratios allow and no larger. Two passes, each hitting one target:

      1. --inferred_percent, over the non-null top-level records. Landing on the target
         ratio means trimming one of the two piles, and this keeps the scarcer pile whole
         and cuts the other down to match. Given 20 inferred and 200 non-inferred against
         a 30% target it keeps all 20 and takes 47 non-inferred; given 200 and 20 it keeps
         all 20 non-inferred and takes 9 inferred.

      2. --non_null_percent, over everything. Null records are admitted up to the share
         that leaves non-null records at the requested percentage.

    Array element records sit out pass 1 and are always kept, since an element's values
    have to be exported together, but they do count toward pass 2's non-null share.

    Because each pass keeps the scarcer pile whole, the size of the result is set by the
    scarcest pile rather than by --sample_size: a run that produced few inferred values
    yields a small export even with thousands of other values available. The logged summary
    reports what the sample actually came out as.
    """
    if not 0 < non_null_percent < 100:
        raise ValueError(
            f"--non_null_percent must be between 1 and 99, got [{non_null_percent}]"
        )
    if not 0 < inferred_percent < 100:
        raise ValueError(
            f"--inferred_percent must be between 1 and 99, got [{inferred_percent}]"
        )

    # Split on is_array_element, not on how many values a record holds: an element that
    # populated a single sub-field is still an array element.
    array_element_records = [r for r in records if r.is_array_element]
    top_level_records = [r for r in records if not r.is_array_element]
    null_records = [r for r in top_level_records if r.is_null]
    inferred_records = [
        r for r in top_level_records if not r.is_null and r.has_inferred_confidence
    ]
    non_inferred_records = [
        r for r in top_level_records if not r.is_null and not r.has_inferred_confidence
    ]

    logging.info(
        "Record pool: %d null top-level, %d inferred top-level, %d non-inferred "
        "top-level, %d array element.",
        len(null_records),
        len(inferred_records),
        len(non_inferred_records),
        len(array_element_records),
    )

    inferred_ratio = inferred_percent / 100
    non_inferred_ratio = 1.0 - inferred_ratio

    # Pass 1: keep whichever pile is scarcer whole, and cap the other to hit the ratio. If
    # either pile is empty there is no ratio to hit, so take whatever exists.
    if not inferred_records:
        selected_non_null_top_level = list(non_inferred_records)
    elif not non_inferred_records:
        selected_non_null_top_level = list(inferred_records)
    elif (
        len(inferred_records) / (len(inferred_records) + len(non_inferred_records))
        <= inferred_ratio
    ):
        # Inferred is the scarcer pile, so keep all of it and cap non-inferred.
        n_non_inferred = min(
            round(len(inferred_records) * non_inferred_ratio / inferred_ratio),
            len(non_inferred_records),
        )
        rng.shuffle(non_inferred_records)
        selected_non_null_top_level = (
            list(inferred_records) + non_inferred_records[:n_non_inferred]
        )
    else:
        # Non-inferred is the scarcer pile, so keep all of it and cap inferred.
        n_inferred = min(
            round(len(non_inferred_records) * inferred_ratio / non_inferred_ratio),
            len(inferred_records),
        )
        rng.shuffle(inferred_records)
        selected_non_null_top_level = inferred_records[:n_inferred] + list(
            non_inferred_records
        )

    # Pass 2: admit null records up to the share that leaves non-null at the target.
    selected_non_null = selected_non_null_top_level + array_element_records
    max_null = round(
        len(selected_non_null) * (100 - non_null_percent) / non_null_percent
    )
    rng.shuffle(null_records)
    selected = selected_non_null + null_records[: min(len(null_records), max_null)]
    rng.shuffle(selected)

    selected_non_null_top_level_final = [
        r for r in selected if not r.is_null and not r.is_array_element
    ]
    n_inferred_final = sum(
        1 for r in selected_non_null_top_level_final if r.has_inferred_confidence
    )
    logging.info(
        "Selected sample: %d records (%d%% non-null; %d%% inferred of non-null "
        "top-level).",
        len(selected),
        (
            round(100 * sum(1 for r in selected if not r.is_null) / len(selected))
            if selected
            else 0
        ),
        (
            round(100 * n_inferred_final / len(selected_non_null_top_level_final))
            if selected_non_null_top_level_final
            else 0
        ),
    )
    return selected


def order_field_values_by_document(
    *,
    pinned_field_values: list[DocumentExtractionFieldValue],
    selected_records: list[DocumentExtractionAnnotatableRecord],
) -> list[DocumentExtractionFieldValue]:
    """Returns every field value to export, ordered so an annotator works a document at a
    time: all of a document's pinned fields, then its surviving records, before moving on
    to the next document.

    Documents come in the order they are first seen, pinned values first and then the
    selected records, which is what fixes the batch's document numbering.
    """
    pinned_by_document: dict[str, list[DocumentExtractionFieldValue]] = defaultdict(
        list
    )
    for field_value in pinned_field_values:
        pinned_by_document[field_value.document_contents_id].append(field_value)

    records_by_document: dict[
        str, list[DocumentExtractionAnnotatableRecord]
    ] = defaultdict(list)
    for record in selected_records:
        records_by_document[record.document_contents_id].append(record)

    document_ids = dict.fromkeys(
        [field_value.document_contents_id for field_value in pinned_field_values]
        + [record.document_contents_id for record in selected_records]
    )
    ordered_field_values = []
    for document_id in document_ids:
        ordered_field_values.extend(pinned_by_document[document_id])
        for record in records_by_document[document_id]:
            ordered_field_values.extend(record.field_values)
    return ordered_field_values


def build_task_data_for_field_value(
    *,
    field_value: DocumentExtractionFieldValue,
    output_schema: LLMRequestOutputSchema,
    prompt_description: str,
    position: DocumentExtractionAnnotationBatchPosition,
) -> CNIAccuracyPerFieldTaskData:
    """Returns the task payload asking an annotator whether one extracted value is correct.

    Args:
        field_value: The extracted value to ask about.
        output_schema: Schema declaring the field, which supplies what it is defined to
            capture.
        prompt_description: What the extractor as a whole was asked to pull out of documents
            like this one.
        position: Where this task sits in the batch being exported.
    """
    return CNIAccuracyPerFieldTaskData(
        state_code=field_value.state_code.value,
        document_id=field_value.document_contents_id,
        document_text=field_value.document_text,
        prompt_description=prompt_description,
        field_name=field_value.field_name,
        field_description=annotatable_field_description(
            output_schema,
            field_name=field_value.field_name,
            array_field_name=field_value.array_field_name,
        ),
        group=field_value.display_group,
        extracted_value=field_value.display_value(
            null_sentinel=NULL_VALUE_DISPLAY_TEXT
        ),
        confidence_level=(
            None
            if field_value.confidence_level is None
            else field_value.confidence_level.value
        ),
        array_element_json=field_value.array_element_json,
        extractor_version_id=field_value.extractor_version_id,
        doc_index=position.doc_index,
        field_index=position.field_index,
        total_fields=position.total_fields,
        task_order=position.task_order,
    )


def task_output_path(
    *,
    target_directory: GcsfsDirectoryPath,
    field_value: DocumentExtractionFieldValue,
    position: DocumentExtractionAnnotationBatchPosition,
) -> GcsfsFilePath:
    """Returns where one task's JSON is written, one file per annotated field value:

        {target_directory}/accuracy_per_field/{extractor_version_id}/
            {document_id}__{field_index}__{field_id}.json

    The zero-padded field index leads the field id so a listing sorts into the order an
    annotator works through a document, rather than alphabetically by field name.
    """
    file_name = (
        f"{field_value.document_contents_id}"
        f"__{str(position.field_index).zfill(3)}"
        f"__{field_value.file_safe_field_id}.json"
    )
    return GcsfsFilePath.from_directory_and_file_name(
        GcsfsDirectoryPath.from_dir_and_subdir(
            target_directory,
            f"{_TASK_SUBDIRECTORY}/{field_value.extractor_version_id}",
        ),
        file_name,
    )


def export_accuracy_tasks(
    *,
    project_id: str,
    extractor_config: LLMExtractorConfig,
    results_narrowing: ExtractionResultsNarrowing,
    sandbox_dataset_prefix: str | None,
    target_directory: GcsfsDirectoryPath,
    sample_size: int,
    non_null_percent: int,
    inferred_percent: int,
    required_fields: list[str] | None,
    random_seed: int,
) -> None:
    """Exports per-field accuracy annotation tasks to GCS.

    Fields named in required_fields are always written for every sampled document,
    regardless of the null/inferred mix, which is then applied to the remaining
    fields. They have to be scalar top-level fields, since pinning pulls a value out of
    the record grouping and only a scalar field holds one value in every document.

    Writes one JSON file per document-field pair to:
        {target_path}/accuracy_per_field/{extractor_version_id}/{document_id}__{field_index_str}__{field_id}.json
    """
    output_schema = extractor_config.extractor_collection.output_schema
    annotatable_names = annotatable_field_names(output_schema)
    if not annotatable_names:
        raise ValueError(
            f"Extractor [{extractor_config.extractor_id}] declares no INFERRED fields, so "
            f"it has no extracted values to annotate."
        )
    pinned: frozenset[str] = (
        frozenset(required_fields) if required_fields else frozenset()
    )
    if unknown_pinned := sorted(pinned - annotatable_names):
        raise ValueError(
            f"--required_fields names field(s) {unknown_pinned} that extractor "
            f"[{extractor_config.extractor_id}] does not annotate. Annotatable fields: "
            f"{sorted(annotatable_names)}."
        )
    pinnable_names = scalar_top_level_annotatable_field_names(output_schema)
    if array_pinned := sorted(pinned - pinnable_names):
        raise ValueError(
            f"--required_fields names array field(s) or array sub-field(s) {array_pinned}, "
            f"which cannot be pinned. A pinned field is exported on its own for every "
            f"sampled document, and only a scalar field holds one value in every document. "
            f"An array sub-field holds one per element, so pinning it would split an "
            f"element's values apart and export the null ones, and an element's values only "
            f"make sense read together. An array field itself holds a value only for a "
            f"document whose array came back empty. Pinnable fields: "
            f"{sorted(pinnable_names)}."
        )

    query_builder = DocumentExtractionResultSampleQueryBuilder(
        extractor_config=extractor_config,
        results_narrowing=results_narrowing,
        sample_size=sample_size,
        input_results_sandbox_dataset_prefix=sandbox_dataset_prefix,
        # Documents always come from production: --sandbox_dataset_prefix names a sandbox
        # copy of the extraction *results*, not of the document store.
        input_documents_sandbox_dataset_prefix=None,
    )

    bq_client = BigQueryClientImpl(project_id=project_id)
    fs = GcsfsFactory.build()

    logging.info("Running accuracy query...")
    sampled_result_rows = list(
        bq_client.run_query_async(
            query_str=query_builder.build_query(project_id=project_id),
            use_query_cache=True,
        ).result()
    )
    field_values = [
        field_value
        for row in sampled_result_rows
        for field_value in DocumentExtractionFieldValue.from_extraction_result(
            output_values=LLMRequestOutputValues(
                output_schema=output_schema,
                output_json=json.loads(row[RESULT_JSON_COLUMN_NAME]),
            ),
            state_code=StateCode(row[STATE_CODE_COLUMN_NAME]),
            document_contents_id=row[DOCUMENT_CONTENTS_ID_COLUMN_NAME],
            document_text=row[DOCUMENT_TEXT_COLUMN_NAME],
            extractor_version_id=row[EXTRACTOR_VERSION_ID_COLUMN_NAME],
        )
    ]
    if not field_values:
        logging.warning("No results found. Exiting.")
        return

    logging.info(
        "Found %d field values across %d documents.",
        len(field_values),
        len({field_value.document_contents_id for field_value in field_values}),
    )

    pinned_field_values = [
        field_value for field_value in field_values if field_value.field_name in pinned
    ]
    remaining_field_values = [
        field_value
        for field_value in field_values
        if field_value.field_name not in pinned
    ]
    logging.info(
        "Collected %d pinned + %d remaining field values.",
        len(pinned_field_values),
        len(remaining_field_values),
    )

    rng = random.Random(random_seed)
    selected_records = select_annotatable_record_sample(
        DocumentExtractionAnnotatableRecord.from_field_values(remaining_field_values),
        non_null_percent=non_null_percent,
        inferred_percent=inferred_percent,
        rng=rng,
    )

    selected = order_field_values_by_document(
        pinned_field_values=pinned_field_values, selected_records=selected_records
    )
    positions = DocumentExtractionAnnotationBatchPosition.for_document_id_sequence(
        [field_value.document_contents_id for field_value in selected]
    )

    logging.info("Writing %d task files to GCS...", len(selected))
    for field_value, position in zip(selected, positions):
        task_data = build_task_data_for_field_value(
            field_value=field_value,
            output_schema=output_schema,
            prompt_description=extractor_config.extractor_collection.description,
            position=position,
        )
        fs.upload_from_string(
            task_output_path(
                target_directory=target_directory,
                field_value=field_value,
                position=position,
            ),
            task_data.to_import_json(),
            content_type="application/json",
        )
    logging.info(
        "Export complete. Wrote %d task files to %s/%s/.",
        len(selected),
        target_directory.uri(),
        _TASK_SUBDIRECTORY,
    )


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Export CNI per-field accuracy annotation tasks, sampling documents from one "
            "extraction run and selecting which of their field values to annotate."
        )
    )
    parser.add_argument(
        "--project_id",
        type=str,
        default=GCP_PROJECT_STAGING,
        help="GCP project ID.",
    )
    parser.add_argument(
        "--state_code",
        type=str,
        required=True,
        help="State code (e.g. US_CO).",
    )
    parser.add_argument(
        "--collection_name",
        type=str,
        required=True,
        help="Extractor collection name (e.g. CASE_NOTE_EMPLOYMENT_INFO).",
    )
    parser.add_argument(
        "--sandbox_dataset_prefix",
        type=str,
        default=None,
        help="Sandbox dataset prefix for reading extraction results.",
    )
    parser.add_argument(
        "--extractor_version_id",
        type=str,
        required=True,
        help="Extractor version ID to sample from.",
    )
    parser.add_argument(
        "--target_bucket",
        type=str,
        required=True,
        help="GCS bucket to export annotation tasks to.",
    )
    parser.add_argument(
        "--target_path",
        type=str,
        required=True,
        help="Path within the target bucket for exports (e.g. label_studio/accuracy/co_round1).",
    )
    parser.add_argument(
        "--extraction_job_id",
        type=str,
        default=None,
        help="Optional: filter to a specific extraction job ID.",
    )
    parser.add_argument(
        "--sample_size",
        type=int,
        default=75,
        help="Number of documents to sample (default: 75).",
    )
    parser.add_argument(
        "--non_null_percent",
        type=int,
        default=70,
        help=(
            "Minimum percentage (0-100) of exported fields that must be non-null "
            "(default: 70)."
        ),
    )
    parser.add_argument(
        "--inferred_percent",
        type=int,
        default=30,
        help=(
            "Target percentage (0-100) of non-null fields with confidence_level='inferred' "
            "(default: 30)."
        ),
    )
    parser.add_argument(
        "--required_fields",
        type=str,
        nargs="+",
        default=None,
        help=(
            "Top-level field names always included for every sampled document, regardless "
            "of the null/inferred mix (e.g. --required_fields primary_status). Array "
            "sub-fields cannot be named, since an element's values are exported together."
        ),
    )
    parser.add_argument(
        "--random_seed",
        type=int,
        default=42,
        help="Seed for the random number generator (default: 42).",
    )
    return parser.parse_args(argv[1:])


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv)

    with local_project_id_override(args.project_id):
        export_accuracy_tasks(
            project_id=args.project_id,
            extractor_config=get_first_order_llm_extractor_config(
                StateCode(args.state_code.upper()), args.collection_name.upper()
            ),
            results_narrowing=ExtractionResultsNarrowing(
                extractor_version_id=args.extractor_version_id,
                extraction_job_id=args.extraction_job_id,
            ),
            sandbox_dataset_prefix=args.sandbox_dataset_prefix,
            target_directory=GcsfsDirectoryPath.from_bucket_and_blob_name(
                bucket_name=args.target_bucket, blob_name=args.target_path
            ),
            sample_size=args.sample_size,
            non_null_percent=args.non_null_percent,
            inferred_percent=args.inferred_percent,
            required_fields=args.required_fields,
            random_seed=args.random_seed,
        )
