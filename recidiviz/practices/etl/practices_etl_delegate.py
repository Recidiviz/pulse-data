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
practices frontend database(s)."""
import abc
import logging
import os
from datetime import datetime, timezone
from typing import Iterator, Optional, TextIO, Tuple

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.metrics.export.export_config import PRACTICES_VIEWS_OUTPUT_DIRECTORY_URI
from recidiviz.utils import metadata
from recidiviz.utils.string import StrictStringFormatter

# Firestore client caps us at 500 records per batch
MAX_FIRESTORE_RECORDS_PER_BATCH = 499


class PracticesETLDelegate(abc.ABC):
    """Abstract class containing the ETL logic for a specific exported practices view."""

    @property
    @abc.abstractmethod
    def EXPORT_FILENAME(self) -> str:
        """Name of the file this delegate is watching for."""

    @property
    @abc.abstractmethod
    def STATE_CODE(self) -> str:
        """State code of the data this delegate watches for."""

    @abc.abstractmethod
    def run_etl(self) -> None:
        """Runs the ETL logic for the view."""

    def filename_matches(self, filename: str) -> bool:
        """Checks if the given filename matches the expected filename for this delegate."""
        return filename == self.EXPORT_FILENAME

    def get_filepath(self) -> GcsfsFilePath:
        return GcsfsFilePath.from_absolute_path(
            os.path.join(
                StrictStringFormatter().format(
                    PRACTICES_VIEWS_OUTPUT_DIRECTORY_URI,
                    project_id=metadata.project_id(),
                ),
                self.STATE_CODE,
                self.EXPORT_FILENAME,
            )
        )

    def get_file_stream(self) -> Iterator[TextIO]:
        """Returns a stream of the contents of the file this delegate is watching for."""
        client = GcsfsFactory.build()

        filepath = self.get_filepath()

        if not client.is_file(filepath.abs_path()):
            raise FileNotFoundError(
                f"File {filepath.abs_path()} does not exist in GCS."
            )

        with client.open(filepath) as f:
            yield f


class PracticesFirestoreETLDelegate(PracticesETLDelegate):
    """Abstract class containing the ETL logic for exporting a specified practices view to Firestore."""

    @property
    @abc.abstractmethod
    def COLLECTION_NAME(self) -> str:
        """Name of the Firestore collection this delegate will ETL into."""

    @abc.abstractmethod
    def transform_row(self, row: str) -> Tuple[Optional[str], Optional[dict]]:
        """Prepares a row of the exported file for Firestore ingestion.
        Returns a tuple of the document ID and the document contents."""

    @property
    def timestamp_key(self) -> str:
        """Name of the key this delegate will insert into each document to record when it was loaded."""
        return "__loadedAt"

    def run_etl(self) -> None:
        logging.info(
            'Starting export of gs://%s to the Firestore collection "%s".',
            self.get_filepath().abs_path(),
            self.COLLECTION_NAME,
        )
        firestore_client = FirestoreClientImpl()
        batch = firestore_client.batch()
        firestore_collection = firestore_client.get_collection(self.COLLECTION_NAME)
        num_records_to_write = 0
        total_records_written = 0

        etl_timestamp = datetime.now(timezone.utc)

        # step 1: load new documents
        for file_stream in self.get_file_stream():
            while line := file_stream.readline():
                row_id, document_fields = self.transform_row(line)
                if row_id is None or document_fields is None:
                    continue
                new_document = {**document_fields, self.timestamp_key: etl_timestamp}
                batch.set(firestore_collection.document(row_id), new_document)
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
            self.COLLECTION_NAME,
        )

        # step 2: delete any pre-existing documents that we didn't just overwrite
        firestore_client.delete_old_documents(
            self.COLLECTION_NAME, self.timestamp_key, etl_timestamp
        )
