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
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, TextIO, Tuple

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.metrics.export.export_config import WORKFLOWS_VIEWS_OUTPUT_DIRECTORY_URI
from recidiviz.utils import metadata
from recidiviz.utils.string import StrictStringFormatter

# Firestore client caps us at 500 records per batch
MAX_FIRESTORE_RECORDS_PER_BATCH = 499


class WorkflowsETLDelegate(abc.ABC):
    """Abstract class containing the ETL logic for transforming and exporting Workflows records."""

    @abc.abstractmethod
    def get_supported_files(self, state_code: str) -> List[str]:
        """Provides list of source files supported for the given state."""

    @abc.abstractmethod
    def run_etl(self, state_code: str, filename: str) -> None:
        """Runs the ETL logic for the provided filename and state_code."""

    def supports_file(self, state_code: str, filename: str) -> bool:
        """Checks if the given filename is supported by this delegate."""
        return filename in self.get_supported_files(state_code)

    def get_filepath(self, state_code: str, filename: str) -> GcsfsFilePath:
        return GcsfsFilePath.from_absolute_path(
            os.path.join(
                StrictStringFormatter().format(
                    WORKFLOWS_VIEWS_OUTPUT_DIRECTORY_URI,
                    project_id=metadata.project_id(),
                ),
                state_code,
                filename,
            )
        )

    def get_file_stream(self, state_code: str, filename: str) -> Iterator[TextIO]:
        """Returns a stream of the contents of the file this delegate is watching for."""
        client = GcsfsFactory.build()

        filepath = self.get_filepath(state_code, filename)

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

    def filepath_url(self, state_code: str, filename: str) -> str:
        return f"gs://{self.get_filepath(state_code, filename).abs_path()}"

    def run_etl(self, state_code: str, filename: str) -> None:
        collection_name = self.COLLECTION_BY_FILENAME[filename]

        logging.info(
            'Starting export of %s to the Firestore collection "%s".',
            self.filepath_url(state_code, filename),
            collection_name,
        )
        firestore_client = FirestoreClientImpl()
        batch = firestore_client.batch()
        firestore_collection = firestore_client.get_collection(collection_name)
        num_records_to_write = 0
        total_records_written = 0

        etl_timestamp = datetime.now(timezone.utc)

        # step 1: load new documents
        for file_stream in self.get_file_stream(state_code, filename):
            while line := file_stream.readline():
                row_id, document_fields = self.transform_row(line)
                if row_id is None or document_fields is None:
                    continue
                document_id = f"{state_code.lower()}_{row_id}"
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
                    num_records_to_write = 0

        batch.commit()
        logging.info(
            '%d records written to Firestore collection "%s".',
            total_records_written,
            collection_name,
        )

        # step 2: delete any pre-existing documents that we didn't just overwrite
        firestore_client.delete_old_documents(
            collection_name, state_code, self.timestamp_key, etl_timestamp
        )


class WorkflowsSingleStateETLDelegate(WorkflowsFirestoreETLDelegate):
    """Abstract class containing the ETL logic for transforming and exporting Workflows records for a single state."""

    @property
    @abc.abstractmethod
    def STATE_CODE(self) -> str:
        """State code of the data this delegate watches for."""

    @property
    @abc.abstractmethod
    def EXPORT_FILENAME(self) -> str:
        """Name of the file this delegate is watching for."""

    @property
    @abc.abstractmethod
    def _COLLECTION_NAME_BASE(self) -> str:
        """Name of the Firestore collection to use."""

    def get_supported_files(self, state_code: str) -> List[str]:
        return [self.EXPORT_FILENAME] if state_code == self.STATE_CODE else []

    @property
    def COLLECTION_BY_FILENAME(self) -> Dict[str, str]:
        return {self.EXPORT_FILENAME: self._COLLECTION_NAME_BASE}
