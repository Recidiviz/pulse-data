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
import copy
import json
import logging
import os
import re
from asyncio import TaskGroup
from datetime import datetime, timezone
from typing import IO, Any, Iterator, Optional, Tuple

from faker import Faker
from google.api_core.exceptions import AlreadyExists
from google.cloud.firestore_admin_v1 import CreateIndexRequest, Index
from google.cloud.firestore_v1.async_batch import AsyncWriteBatch
from google.cloud.firestore_v1.collection import CollectionReference

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.metrics.export.export_config import WORKFLOWS_VIEWS_OUTPUT_DIRECTORY_URI
from recidiviz.utils import metadata
from recidiviz.utils.string import StrictStringFormatter

# batches can be up to 500 documents but writes are more stable with smaller batches.
# effect on speed is not that significant for async ops anyway. 20 is a default elsewhere
# in the Firestore SDK (e.g. the BulkWriter, which we are not using here because it is too
# unstable in other ways)
MAX_FIRESTORE_RECORDS_PER_BATCH = 20
# to prevent memory overruns we don't want to stream in too much input all at once
INPUT_CHUNK_SIZE = 32 * 1024 * 1024  # 32 mb
# Key identifying the state a document belongs to. Every document we write must carry it:
# the composite index below and the prune in step 3 both filter on it, so a document
# missing it would never be cleaned up and would linger in Firestore indefinitely.
STATE_CODE_KEY = "stateCode"
JSON_STRING_OPENING_CHARS = ("{", "[")


def recursive_json_loads(value: Any) -> Any:
    """Parses value recursively so all nested json strings are parsed into a dictionary."""
    if isinstance(value, str):
        # Only JSON objects and arrays are parsed, so strings like "123", "true", or
        # "null" stay strings instead of becoming ints, bools, or None.
        if not value.lstrip().startswith(JSON_STRING_OPENING_CHARS):
            return value
        try:
            parsed_value = json.loads(value)
        except json.JSONDecodeError:
            return value
        return recursive_json_loads(parsed_value)

    if isinstance(value, dict):
        return {key: recursive_json_loads(value[key]) for key in value}
    if isinstance(value, list):
        return [recursive_json_loads(nested_value) for nested_value in value]

    return value


class WorkflowsETLDelegate(abc.ABC):
    """Abstract class containing the ETL logic for transforming and exporting Workflows records."""

    def __init__(self, state_code: StateCode, filename: str):
        self.state_code = state_code
        self._faker = Faker()
        self.filename = filename

    PSEUDONYMIZED_FIELDS: dict | None = None
    """
    A map from each PII field to the Faker provider that generates its fake value (e.g.
    "first_name"). Omitted fields keep their real value even if they are nested under a
    field that has some PII (e.g. "name_suffix" stays the same). None means the delegate
    exports no pseudonymized copy.
    """

    collection_name: str = ""

    @abc.abstractmethod
    async def run_etl(self, filename: str) -> None:
        """Runs the ETL logic for the provided filename and state_code."""

    @classmethod
    @abc.abstractmethod
    def supports_file(cls, state_code: StateCode, filename: str) -> bool:
        """Returns if the class supports the given filename and state_code."""

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

        # GCSFileSystemImpl.open()'s write-mode branch catches BaseException
        # without a finally (deliberately, to always cancel the upload), which
        # makes pylint flag every caller of open() in any mode, including this
        # read-only one.
        # pylint: disable-next=contextmanager-generator-missing-cleanup
        with client.open(filepath) as f:
            yield f

    def pseudonymize_record(
        self, record: dict[str, Any], seed: str
    ) -> dict[str, Any] | None:
        if not self.PSEUDONYMIZED_FIELDS:
            return None

        self._faker.seed_instance(seed)
        result = copy.deepcopy(record)
        return self._pseudonymize_fields_on_record(result, self.PSEUDONYMIZED_FIELDS)

    def _pseudonymize_fields_on_record(
        self, record: dict, pseudonymized_fields: dict
    ) -> dict[str, Any] | None:
        """
        Updates record in place with pseudonymized values for all fields in
        `pseudonymized_fields`. Returns None if `psuedonymized_fields` structure does
        not mirror `record`.
        """
        for field, value in pseudonymized_fields.items():
            if field not in record or record[field] is None:
                continue

            nested_record = record[field]
            if isinstance(value, str):
                if isinstance(nested_record, dict):
                    logging.info(
                        "Failed to create the pseudonymized record. Expected a string, "
                        "integer, or double value for field: %s on record: %s. Did not "
                        "expect a dictionary.",
                        field,
                        record,
                    )
                    return None
                record[field] = getattr(self._faker, value)()
            elif isinstance(value, dict):
                if not isinstance(nested_record, dict):
                    logging.info(
                        "Failed to create the pseudonymized record. Expected a "
                        "dictionary for field: %s on record %s.",
                        field,
                        record,
                    )
                    return None
                self._pseudonymize_fields_on_record(
                    record=nested_record, pseudonymized_fields=value
                )
        return record


class WorkflowsFirestoreETLDelegate(WorkflowsETLDelegate):
    """Abstract class containing the ETL logic for exporting workflows views to Firestore.

    NOTE: If you are adding an ETL delegate for a *new* collection, you need to create a composite index on the
    `stateCode` and `__loadedAt` fields. This is required to run the ETL query that deletes old documents.

    See Managing Indexes in Firestore: https://firebase.google.com/docs/firestore/query-data/indexing
    """

    @abc.abstractmethod
    def transform_row(
        self, row: dict[str, Any], filename: str
    ) -> Tuple[Optional[str], Optional[dict]]:
        """Prepares a row of the exported file for Firestore ingestion.
        Returns a tuple of the document ID and the document contents.

        `filename` identifies the source file the row came from, so delegates
        that serve multiple collections can vary the transform per file."""

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

    total_records_written = 0

    async def _send_batch(self, batch: AsyncWriteBatch) -> None:
        await batch.commit()
        self.total_records_written += len(batch)

    async def run_etl(self, filename: str) -> None:
        pseudonymized_collection_name = "PSEUDONYMIZED_" + self.collection_name
        logging.info(
            'Starting export of %s to the Firestore collection "%s".',
            self.filepath_url(filename),
            self.collection_name,
        )
        firestore_client = FirestoreClientImpl()
        firestore_collection = firestore_client.get_collection(self.collection_name)
        pseudonymized_firestore_collection = firestore_client.get_collection(
            pseudonymized_collection_name
        )
        has_pseudonymized_collection = False
        num_records_to_write = 0
        self.total_records_written = 0

        etl_timestamp = datetime.now(timezone.utc)

        # step 1: Load new documents
        for file_stream in self.get_file_stream(filename):
            while rows := file_stream.readlines(INPUT_CHUNK_SIZE):
                # wait for all writes in this chunk to finish before starting the next one
                async with TaskGroup() as tg:
                    batch = firestore_client.async_batch()
                    for line in rows:
                        line_data = recursive_json_loads(line)

                        row_id = self._add_row_to_batch(
                            line_data,
                            filename,
                            etl_timestamp,
                            firestore_collection,
                            batch,
                        )

                        # If the row_id is None that means this row was not added to
                        # the firestore collection for some reason.
                        if not row_id:
                            continue

                        num_records_to_write += 1
                        pseudonymized_data = self.pseudonymize_record(
                            line_data, seed=self.doc_id_for_row_id(row_id)
                        )
                        if pseudonymized_data:
                            if self._add_row_to_batch(
                                pseudonymized_data,
                                filename,
                                etl_timestamp,
                                pseudonymized_firestore_collection,
                                batch,
                            ):
                                num_records_to_write += 1
                                has_pseudonymized_collection = True

                        if num_records_to_write >= MAX_FIRESTORE_RECORDS_PER_BATCH:
                            tg.create_task(self._send_batch(batch))
                            batch = firestore_client.async_batch()
                            num_records_to_write = 0

                    # send whatever's left over at the end of this chunk
                    tg.create_task(self._send_batch(batch))

                logging.info(
                    '%d total records written to Firestore collection "%s".',
                    self.total_records_written,
                    self.collection_name,
                )

        # step 2: Create composite indexes if they do not exist
        self._create_index_on_firestore(firestore_client, self.collection_name)
        if has_pseudonymized_collection:
            self._create_index_on_firestore(
                firestore_client, pseudonymized_collection_name
            )

        # step 3: Delete any pre-existing documents that we didn't just overwrite
        firestore_client.delete_old_documents(
            self.collection_name,
            self.state_code.value,
            self.timestamp_key,
            etl_timestamp,
        )
        if has_pseudonymized_collection:
            firestore_client.delete_old_documents(
                pseudonymized_collection_name,
                self.state_code.value,
                self.timestamp_key,
                etl_timestamp,
            )

    def _add_row_to_batch(
        self,
        row: dict[str, Any],
        filename: str,
        etl_timestamp: datetime,
        firestore_collection: CollectionReference,
        batch: AsyncWriteBatch,
    ) -> str | None:
        """Transforms and adds the row to the given `batch` to add it to the given
        `firestore_collection`."""
        try:
            row_id, document_fields = self.transform_row(row, filename)
        except Exception as e:
            logging.error(
                "Transform row failed on [%s] due to: %s",
                row,
                str(e),
            )
            return None

        if row_id is None or document_fields is None:
            return None

        if not document_fields.get(STATE_CODE_KEY):
            logging.error(
                "Skipping row [%s] in [%s]: document has no [%s]",
                row_id,
                filename,
                STATE_CODE_KEY,
            )
            return None

        document_id = self.doc_id_for_row_id(row_id)
        new_document = {
            **document_fields,
            self.timestamp_key: etl_timestamp,
        }
        batch.set(firestore_collection.document(document_id), new_document)
        return row_id

    def _create_index_on_firestore(
        self, firestore_client: FirestoreClientImpl, collection_name: str
    ) -> None:
        """Creates composite index for the given `collection_name` if the index doesn't
        already exist."""
        if not firestore_client.index_exists_for_collection(collection_name):
            try:
                create_index_request = CreateIndexRequest(
                    mapping={
                        "index": Index(
                            mapping={
                                "name": collection_name,
                                "query_scope": "COLLECTION",
                                "fields": [
                                    {
                                        "field_path": STATE_CODE_KEY,
                                        "order": "ASCENDING",
                                    },
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


class WorkflowsSingleFileETLDelegate(WorkflowsFirestoreETLDelegate):
    """Abstract class containing the logic to set the collection_name and determine if a filename is supported."""

    EXPECTED_FILENAME: str | None

    @property
    @abc.abstractmethod
    def COLLECTION_NAME(self) -> str:
        """Firestore collection the delegate loads data into."""

    @classmethod
    def supports_file(cls, state_code: StateCode, filename: str) -> bool:
        if cls.EXPECTED_FILENAME:
            return filename == cls.EXPECTED_FILENAME
        return False

    def __init__(self, state_code: StateCode, filename: str):
        super().__init__(state_code, filename)
        self.collection_name = self.COLLECTION_NAME
