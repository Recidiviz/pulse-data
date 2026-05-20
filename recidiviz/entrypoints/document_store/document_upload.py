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
"""Entrypoint for document GCS upload: reads document batches assigned to this
task instance and uploads document contents to GCS."""

import argparse
import json
import logging
from datetime import datetime, timezone

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_store_types import DocumentUploadBatch
from recidiviz.documents.store.gcs_document_uploader import GcsDocumentUploader
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.utils.environment import (
    AirflowKubernetesPodEnvironment,
    in_airflow_kubernetes_pod,
)
from recidiviz.utils.metadata import project_id
from recidiviz.utils.types import assert_type


class DocumentUploadEntrypoint(EntrypointInterface):
    """Entrypoint for document GCS upload: reads document batches assigned to this
    task instance and uploads document contents to GCS."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--state_code",
            type=StateCode,
            required=True,
        )
        parser.add_argument(
            "--upload_batches",
            type=json.loads,
            required=True,
            help="JSON-serialized list of DocumentUploadBatch dicts assigned to this task instance for upload.",
        )
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        if not in_airflow_kubernetes_pod():
            raise ValueError(
                "This entrypoint should only be used from within an Airflow KubernetesPodOperator task."
            )

        upload_batches = [DocumentUploadBatch.from_dict(d) for d in args.upload_batches]

        if not upload_batches:
            raise ValueError("Expected non-empty upload_batches.")

        run_id = AirflowKubernetesPodEnvironment.get_run_id()
        task_index = int(
            assert_type(AirflowKubernetesPodEnvironment.get_map_index(), str)
        )

        logging.info(
            "Running document upload for [%s] run_id [%s] task_index [%d] "
            "with %d batches",
            args.state_code.value,
            run_id,
            task_index,
            len(upload_batches),
        )

        uploader = GcsDocumentUploader(
            state_code=args.state_code,
            project_id=project_id(),
            big_query_client=BigQueryClientImpl(),
            fs=GcsfsFactory.build(),
            run_id=run_id,
            task_index=task_index,
            upload_datetime=datetime.now(tz=timezone.utc),
        )
        uploader.run(upload_batches)
