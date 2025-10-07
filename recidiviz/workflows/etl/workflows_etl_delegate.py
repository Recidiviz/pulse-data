#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Abstract classes defining delegate interfaces for ETLing data into the
workflows' frontend database(s)."""
import abc
import logging
import os
import re
from datetime import datetime, timezone
from typing import IO, Dict, Iterator, List, Optional, Tuple

from google.api_core.exceptions import AlreadyExists
from google.cloud.firestore_admin_v1 import CreateIndexRequest, Index

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.metrics.export.export_config import WORKFLOWS_VIEWS_OUTPUT_DIRECTORY_URI
from recidiviz.utils import metadata
from recidiviz.utils.string import StrictStringFormatter

# Firestore client caps us at 500 records per batch. We've run into transaction size limits with
# this being set to 400 and 200, so it's now set to 100.
MAX_FIRESTORE_RECORDS_PER_BATCH = 100


class WorkflowsETLDelegate(abc.ABC):
    """Abstract class containing the ETL logic for transforming and exporting Workflows records."""

    def __init__(self, state_code: StateCode):
        self.state_code = state_code

    @abc.abstractmethod
    def get_supported_files(self) -> List[str]:
        """Provides list of source files supported for the given state."""

    @abc.abstractmethod
    def run_etl(self, filename: str) -> None:
        """Runs the ETL logic for the provided filename and state_code."""

    def supports_file(self, filename: str) -> bool:
        """Checks if the given filename is supported by this delegate."""
        return filename in self.get_supported_files()

    def get_filepath(self, filename: str) -> GcsfsFilePath:
        return GcsfsFilePath.from_absolute_path(
            os.path.join(
                StrictStringFormatter().format(
                    WORKFLOWS_VIEWS_OUTPUT_DIRECTORY_URI,
                    project_id=metadata.project_id(),
                ),
                self.state_code.value.upper(),
                filename,
            )
        )

    def get_file_stream(self, filename: str) -> Iterator[IO]:
        """Returns a stream of the contents of the file this delegate is watching for."""
        client = GcsfsFactory.build()

        filepath = self.get_filepath(filename)

        if not client.is_file(filepath.abs_path()):
            raise FileNotFoundError(
                f"File {filepath.abs_path()} does not exist in GCS."
            )

        with client.open(filepath) as f:
            yield f


class WorkflowsFirestoreETLDelegate(WorkflowsETLDelegate):
    """Abstract class containing the ETL logic for exporting workflows views to Firestore.

    NOTE: If you are adding an ETL delegate for a *new* collection, you need to create a composite index on the
    `stateCode` and `__loadedAt` fields. This is required to run the ETL query that deletes old documents.

    See Managing Indexes in Firestore: https://firebase.google.com/docs/firestore/query-data/indexing
    """

    @property
    @abc.abstractmethod
    def COLLECTION_BY_FILENAME(self) -> Dict[str, str]:
        """Mapping of the base name for the Firestore collection to the record type. Collection name may be
        augmented for different environment targets."""

    @abc.abstractmethod
    def transform_row(self, row: str) -> Tuple[Optional[str], Optional[dict]]:
        """Prepares a row of the exported file for Firestore ingestion.
        Returns a tuple of the document ID and the document contents."""

    @property
    def timestamp_key(self) -> str:
        """Name of the key this delegate will insert into each document to record when it was loaded."""
        return "__loadedAt"

    def filepath_url(self, filename: str) -> str:
        return f"gs://{self.get_filepath(filename).abs_path()}"

    def doc_id_for_row_id(self, row_id: str) -> str:
        doc_id = (
            f"{self.state_code.value}_{row_id}"
            # https://firebase.google.com/docs/firestore/quotas#collections_documents_and_fields
            # Document IDs cannot contain a forward slash
            .replace("/", "-")
            # Document IDs can contain spaces, but replacing them with underscores will look nicer
            # when browsing
            .replace(" ", "_")
            # Lowercase them for the same reason
            .lower()
        )
        # also replace any other special characters to look nicer when browsing
        doc_id = re.sub(r"[^a-z0-9_-]", "", doc_id)
        return doc_id

    def run_etl(self, filename: str) -> None:
        collection_name = self.COLLECTION_BY_FILENAME[filename]

        logging.info(
            'Starting export of %s to the Firestore collection "%s".',
            self.filepath_url(filename),
            collection_name,
        )
        firestore_client = FirestoreClientImpl()
        batch = firestore_client.batch()
        firestore_collection = firestore_client.get_collection(collection_name)
        num_records_to_write = 0
        total_records_written = 0

        etl_timestamp = datetime.now(timezone.utc)

        # step 1: Load new documents
        for file_stream in self.get_file_stream(filename):
            while line := file_stream.readline():
                try:
                    row_id, document_fields = self.transform_row(line)
                except Exception as e:
                    logging.error(
                        "Transform row failed on [%s] due to: %s", line, str(e)
                    )
                    continue

                if row_id is None or document_fields is None:
                    continue
                document_id = self.doc_id_for_row_id(row_id)
                new_document = {
                    **document_fields,
                    self.timestamp_key: etl_timestamp,
                }
                batch.set(firestore_collection.document(document_id), new_document)
                num_records_to_write += 1
                total_records_written += 1

                if num_records_to_write >= MAX_FIRESTORE_RECORDS_PER_BATCH:
                    batch.commit()
                    batch = firestore_client.batch()
                    logging.info(
                        '%d total records written to Firestore collection "%s".',
                        total_records_written,
                        collection_name,
                    )
                    num_records_to_write = 0

        batch.commit()
        logging.info(
            '%d total records written to Firestore collection "%s".',
            total_records_written,
            collection_name,
        )

        # step 2: Create composite indexes if they do not exist
        if not firestore_client.index_exists_for_collection(collection_name):
            try:
                create_index_request = CreateIndexRequest(
                    mapping={
                        "index": Index(
                            mapping={
                                "name": collection_name,
                                "query_scope": "COLLECTION",
                                "fields": [
                                    {"field_path": "stateCode", "order": "ASCENDING"},
                                    {"field_path": "__loadedAt", "order": "ASCENDING"},
                                ],
                            }
                        )
                    }
                )
                logging.info(
                    "Creating composite index for collection: %s", collection_name
                )
                operation = firestore_client.create_index(
                    collection_name, create_index_request
                )
                # Wait for index creation to complete
                operation.result()
            except AlreadyExists:
                # This race condition is unlikely to happen, but we want to protect from it just in case.
                logging.info(
                    "Skip create composite index for collection %s. Index already exists.",
                    collection_name,
                )

        # step 3: Delete any pre-existing documents that we didn't just overwrite
        firestore_client.delete_old_documents(
            collection_name, self.state_code.value, self.timestamp_key, etl_timestamp
        )


class WorkflowsSingleStateETLDelegate(WorkflowsFirestoreETLDelegate):
    """Abstract class containing the ETL logic for transforming and exporting Workflows records for a single state."""

    @property
    @abc.abstractmethod
    def SUPPORTED_STATE_CODE(self) -> StateCode:
        """State code of the data this delegate watches for."""

    @property
    @abc.abstractmethod
    def EXPORT_FILENAME(self) -> str:
        """Name of the file this delegate is watching for."""

    @property
    @abc.abstractmethod
    def _COLLECTION_NAME_BASE(self) -> str:
        """Name of the Firestore collection to use."""

    def get_supported_files(self) -> List[str]:
        return (
            [self.EXPORT_FILENAME]
            if self.state_code == self.SUPPORTED_STATE_CODE
            else []
        )

    @property
    def COLLECTION_BY_FILENAME(self) -> Dict[str, str]:
        return {self.EXPORT_FILENAME: self._COLLECTION_NAME_BASE}
