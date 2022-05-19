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
"""Client wrapper for interacting with Firestore."""
import abc
import logging
from datetime import datetime
from typing import Dict, Optional, Union

from google.cloud import firestore
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference
from google.cloud.firestore_v1.query import Query

from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION

FIRESTORE_STAGING_PROJECT_ID = "recidiviz-dashboard-staging"
FIRESTORE_PRODUCTION_PROJECT_ID = "recidiviz-dashboard-production"

FIRESTORE_DELETE_BATCH_SIZE = 400


class FirestoreClient(abc.ABC):
    """Interface for a wrapper around the Google Cloud Firestore API."""

    @property
    @abc.abstractmethod
    def project_id(self) -> str:
        """Returns the GCP ID for the Firestore client."""

    @abc.abstractmethod
    def get_collection(self, collection_path: str) -> CollectionReference:
        """Returns a reference to a Firestore collection."""

    @abc.abstractmethod
    def get_document(self, document_path: str) -> DocumentReference:
        """Returns a reference to a Firestore document."""

    @abc.abstractmethod
    def set_document(self, document_path: str, data: Dict) -> None:
        """Sets the data for a Firestore document."""

    @abc.abstractmethod
    def batch(self) -> firestore.WriteBatch:
        """Returns a batch for writing to Firestore."""

    @abc.abstractmethod
    def delete_collection(self, collection_path: str) -> None:
        """Deletes a collection from Firestore."""

    @abc.abstractmethod
    def delete_old_documents(
        self, collection_path: str, timestamp_field: str, cutoff: datetime
    ) -> None:
        """Deletes documents timestamped before the cutoff from a Firestore collection."""


class FirestoreClientImpl(FirestoreClient):
    """Base implementation of the FirestoreClient interface."""

    def __init__(self, project_id: Optional[str] = None):
        if not project_id:
            project_id = metadata.project_id()

            if not project_id:
                raise ValueError(
                    "Must provide a project_id if metadata.project_id() returns None"
                )

            project_id = (
                FIRESTORE_PRODUCTION_PROJECT_ID
                if project_id == GCP_PROJECT_PRODUCTION
                else FIRESTORE_STAGING_PROJECT_ID
            )

        self._project_id = project_id
        self.client = firestore.Client(project=self.project_id)

    @property
    def project_id(self) -> str:
        return self._project_id

    def get_collection(self, collection_path: str) -> CollectionReference:
        return self.client.collection(collection_path)

    def get_document(self, document_path: str) -> DocumentReference:
        return self.client.document(document_path)

    def set_document(self, document_path: str, data: Dict) -> None:
        self.client.document(document_path).set(data)

    def batch(self) -> firestore.WriteBatch:
        return self.client.batch()

    def delete_collection(self, collection_path: str) -> None:
        logging.info('Deleting collection "%s"', collection_path)
        collection = self.get_collection(collection_path)
        total_docs_deleted = self._delete_documents_batch(collection)
        logging.info(
            'Deleted %d documents from collection "%s"',
            total_docs_deleted,
            collection_path,
        )

    def delete_old_documents(
        self, collection_path: str, timestamp_field: str, cutoff: datetime
    ) -> None:
        logging.info(
            'Deleting documents older than %s from "%s"', cutoff, collection_path
        )
        collection = self.get_collection(collection_path)
        total_docs_deleted = self._delete_documents_batch(
            collection.where(timestamp_field, "<", cutoff)
        )
        logging.info(
            'Deleted %d documents from collection "%s"',
            total_docs_deleted,
            collection_path,
        )

    def _delete_documents_batch(
        self, query: Union[CollectionReference, Query], deleted_so_far: int = 0
    ) -> int:
        batch = self.batch()
        docs = query.limit(FIRESTORE_DELETE_BATCH_SIZE).stream()
        deleted_count = 0

        for doc in docs:
            batch.delete(doc.reference)
            deleted_count += 1

        batch.commit()

        if deleted_count >= FIRESTORE_DELETE_BATCH_SIZE:
            return self._delete_documents_batch(query, deleted_so_far + deleted_count)

        return deleted_so_far + deleted_count
