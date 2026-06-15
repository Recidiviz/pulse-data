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
"""Airflow tasks for the document store DAG pipeline."""

import json
import logging
from datetime import datetime, timezone

from airflow.decorators import task

from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    ENTRYPOINT_ARGUMENTS,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_store_types import (
    SingleCollectionDocumentDiscoveryResult,
)
from recidiviz.documents.store.document_upload_batching import build_document_batches
from recidiviz.documents.store.new_document_discovery import NewDocumentDiscoverer
from recidiviz.documents.store.record_document_upload_results import (
    DocumentUploadResultRecorder,
)


@task.short_circuit(ignore_downstream_trigger_rules=False)
def check_has_updates(
    collection_result: dict[str, str | int] | None,
) -> bool:
    """Skips downstream document upload/record tasks when discovery found no
    new metadata updates for the collection."""
    return collection_result is not None


@task
def run_document_discovery(
    state_code: StateCode,
    collection_name: str,
    run_id: str,
) -> dict[str, str | int] | None:
    """Runs document discovery for a single collection, writing temp BQ tables
    and returning the discovery result (or None if no new metadata rows)."""
    discoverer = NewDocumentDiscoverer(
        state_code=state_code,
        collection_name=collection_name,
        project_id=get_project_id(),
        big_query_client=BigQueryClientImpl(),
        run_id=run_id,
    )
    collection_result = discoverer.run()

    if collection_result is None:
        logging.info(
            "[%s] Collection [%s] discovery complete: no new metadata rows.",
            state_code.value,
            collection_name,
        )
        return None

    logging.info(
        "[%s] Collection [%s] discovery complete: %d new metadata rows.",
        state_code.value,
        collection_name,
        collection_result.num_document_metadata_updates_rows,
    )
    return collection_result.to_dict()


@task
def build_document_upload_pod_arguments(
    state_code: StateCode,
    collection_result: dict[str, str | int],
    upload_task_instance_count: int,
) -> list[list[str]]:
    """Builds DocumentUploadBatches for the collection, distributes them
    across |upload_task_instance_count| task instances, and returns the argv
    for each mapped DocumentUploadEntrypoint pod."""
    task_instance_batches = build_document_batches(
        collection_results=[
            SingleCollectionDocumentDiscoveryResult.from_dict(collection_result)
        ],
        num_upload_task_instances=upload_task_instance_count,
        big_query_client=BigQueryClientImpl(),
    )

    return [
        [
            *ENTRYPOINT_ARGUMENTS,
            "--entrypoint=DocumentUploadEntrypoint",
            f"--state_code={state_code.value}",
            f"--upload_batches={json.dumps([b.to_dict() for b in task_batches])}",
        ]
        for task_batches in task_instance_batches
        if task_batches
    ]


@task
def record_document_upload_results(
    collection_result: dict[str, str | int],
    state_code: StateCode,
    run_id: str,
) -> None:
    """Loads this collection's upload status CSVs into BQ, inserts metadata
    rows for successful uploads, and cleans up temp tables."""
    recorder = DocumentUploadResultRecorder(
        state_code=state_code,
        project_id=get_project_id(),
        big_query_client=BigQueryClientImpl(),
        run_id=run_id,
        metadata_row_create_datetime=datetime.now(tz=timezone.utc),
    )
    recorder.run(SingleCollectionDocumentDiscoveryResult.from_dict(collection_result))
