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
import re
from datetime import datetime
from typing import Dict, List, Optional, Union

from google.api_core.operation import Operation
from google.cloud import firestore_admin_v1, firestore_v1
from google.cloud.firestore_v1 import FieldFilter
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference
from google.cloud.firestore_v1.query import BaseQuery, CollectionGroup
from google.cloud.firestore_v1.stream_generator import StreamGenerator

from recidiviz.common.google_cloud.protobuf_builder import ProtoPlusBuilder
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.types import assert_type

FIRESTORE_STAGING_PROJECT_ID = "recidiviz-dashboard-staging"
FIRESTORE_PRODUCTION_PROJECT_ID = "recidiviz-dashboard-production"

FIRESTORE_DELETE_BATCH_SIZE = 100
TIMESTAMP_KEY = "__loadedAt"


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
    def get_collection_group(self, collection_path: str) -> CollectionGroup:
        """Returns a query object for a Firestore collection group."""

    @abc.abstractmethod
    def get_document(self, document_path: str) -> DocumentReference:
        """Returns a reference to a Firestore document."""

    @abc.abstractmethod
    def set_document(self, document_path: str, data: Dict) -> None:
        """Sets the data for a Firestore document."""

    @abc.abstractmethod
    def batch(self) -> firestore_v1.WriteBatch:
        """Returns a batch for writing to Firestore."""

    @abc.abstractmethod
    def list_collections_with_indexes(self) -> List[str]:
        """Lists all the collection names that have an index created."""

    @abc.abstractmethod
    def index_exists_for_collection(self, collection_name: str) -> bool:
        """Returns whether a collection already has an index created."""

    @abc.abstractmethod
    def create_index(
        self,
        collection_name: str,
        create_index_request: firestore_admin_v1.types.CreateIndexRequest,
    ) -> Operation:
        """Creates a composite index for the given collection_name using the create_index_request."""

    @abc.abstractmethod
    def delete_collection(self, collection_path: str) -> None:
        """Deletes a collection from Firestore."""

    @abc.abstractmethod
    def delete_old_documents(
        self,
        collection_path: str,
        state_code: str,
        timestamp_field: str,
        cutoff: datetime,
    ) -> None:
        """Deletes documents from a Firestore collection for a specific state that is timestamped before the cutoff."""


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
        # (default) is the database name automatically assigned by Firestore to the default database in a project.
        self.database_name = "(default)"
        self.client = firestore_v1.Client(project=self.project_id)
        self.admin_client = firestore_admin_v1.FirestoreAdminClient()

    @property
    def project_id(self) -> str:
        return self._project_id

    @property
    def timestamp_key(self) -> str:
        return "__loadedAt"

    def get_collection(self, collection_path: str) -> CollectionReference:
        return self.client.collection(collection_path)

    def get_collection_group(self, collection_path: str) -> CollectionGroup:
        return self.client.collection_group(collection_path)

    def get_document(self, document_path: str) -> DocumentReference:
        return self.client.document(document_path)

    def set_document(self, document_path: str, data: Dict, merge: bool = False) -> None:
        self.client.document(document_path).set(data, merge=merge)

    def add_document(self, collection_path: str, data: Dict) -> None:
        self.client.collection(collection_path).add(data)

    def update_document(self, document_path: str, data: Dict) -> None:
        self.client.document(document_path).update(data)

    def batch(self) -> firestore_v1.WriteBatch:
        return self.client.batch()

    def list_collections_with_indexes(self) -> List[str]:
        # Collection ID is set to 'all' in the request because `list_indexes` always prints all the
        # indexes in a project, regardless of the collection group specified. 'all' does not mean anything here,
        # any string provided will work, but it can not be left blank.
        project_path = f"projects/{self.project_id}/databases/{self.database_name}/collectionGroups/all"
        indexes = list(self.admin_client.list_indexes(request={"parent": project_path}))
        collections = []
        for index in indexes:
            # index.name returns a string that matches:
            # "projects/{project_id}/databases/{database_name}/collectionGroups/{collection_name}/indexes/{index_id}"
            match = re.search(r"collectionGroups/(.+)/indexes", index.name)
            if match:
                collections.append(match.group(1))
        return collections

    def index_exists_for_collection(self, collection_name: str) -> bool:
        return collection_name in self.list_collections_with_indexes()

    def create_index(
        self,
        collection_name: str,
        create_index_request: firestore_admin_v1.types.CreateIndexRequest,
    ) -> Operation:
        project_path = f"projects/{self.project_id}/databases/{self.database_name}/collectionGroups/{collection_name}"

        request = (
            ProtoPlusBuilder(firestore_admin_v1.types.CreateIndexRequest)
            .update_args(parent=project_path)
            .compose(create_index_request)
            .build()
        )

        return self.admin_client.create_index(request=request)

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
        self,
        collection_path: str,
        state_code: str,
        timestamp_field: str,
        cutoff: datetime,
    ) -> None:
        logging.info(
            '[%s] Deleting documents older than %s from "%s"',
            state_code,
            cutoff,
            collection_path,
        )
        collection = self.get_collection(collection_path)
        total_docs_deleted = self._delete_documents_batch(
            collection.where(filter=FieldFilter("stateCode", "==", state_code)).where(
                filter=FieldFilter(timestamp_field, "<", cutoff)
            )
        )
        logging.info(
            '[%s] Deleted %d documents from collection "%s"',
            state_code,
            total_docs_deleted,
            collection_path,
        )

    def delete_documents_with_state_code(
        self,
        collection_path: str,
        state_code: str,
    ) -> None:
        logging.info(
            'Deleting documents from collection "%s" with state code "%s"',
            collection_path,
            state_code,
        )
        collection = self.get_collection(collection_path)
        total_docs_deleted = self._delete_documents_batch(
            collection.where(filter=FieldFilter("stateCode", "==", state_code))
        )
        logging.info(
            '[%s] Deleted %d documents from collection "%s"',
            state_code,
            total_docs_deleted,
            collection_path,
        )

    def _delete_documents_batch(
        self, query: Union[CollectionReference, BaseQuery], deleted_so_far: int = 0
    ) -> int:
        batch = self.batch()
        # .stream()'s return value in the query interface could return either an AsyncStreamGenerator or StreamGenerator
        # however, since we do not use the async client, it will always be a StreamGenerator
        # asserting the type removes a MyPy error for a missing `__iter__` attribute in the loop below
        docs = assert_type(
            query.limit(FIRESTORE_DELETE_BATCH_SIZE).stream(),
            StreamGenerator,
        )
        deleted_count = 0

        for doc in docs:
            batch.delete(doc.reference)
            deleted_count += 1

        batch.commit()

        if deleted_count >= FIRESTORE_DELETE_BATCH_SIZE:
            return self._delete_documents_batch(query, deleted_so_far + deleted_count)

        return deleted_so_far + deleted_count
