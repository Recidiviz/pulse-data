# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Classes for providing access to document data from various sources."""
from contextlib import contextmanager
from typing import IO, Iterator

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_collection_config import (
    DOCUMENT_ID_COLUMN_NAME,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_store_utils import (
    DocumentId,
    gcs_path_for_document,
)
from recidiviz.utils.metadata import project_id

# Documents in the document store are stored as plain text files
DOCUMENT_MIME_TYPE = "text/plain"


@attr.define
class GCSDocument:
    """Represents a single document with its ID and access to its content."""

    document_id: DocumentId

    # File path where this document's data lives
    gcs_file_path: GcsfsFilePath

    # MIME type of the document (e.g., text/plain, application/pdf)
    mime_type: str

    @contextmanager
    def open(self) -> Iterator[IO[str]]:
        """Context manager for reading document content.

        Usage:
            with document.open() as f:
                content = f.read()
        """
        fs = GcsfsFactory.build()
        with fs.open(self.gcs_file_path, mode="r") as f:
            yield f


@attr.define
class GCSDocumentProvider:
    """Provides access to collections of documents from GCS."""

    # Query that can be run in BQ to produce a list of document_ids (query must have
    # single 'document_id' output column).
    document_id_bq_query: str

    # State code for building GCS paths
    state_code: StateCode

    # Optional sandbox bucket override
    sandbox_bucket: str | None

    def document_iterator(self, bq_client: BigQueryClient) -> Iterator[GCSDocument]:
        """Iterates over documents, yielding GCSDocument objects."""
        query_job = bq_client.run_query_async(
            query_str=self.document_id_bq_query, use_query_cache=True
        )
        for row in query_job:
            document_id = row[DOCUMENT_ID_COLUMN_NAME]
            gcs_path = gcs_path_for_document(
                project_id=project_id(),
                state_code=self.state_code,
                document_id=document_id,
                sandbox_bucket=self.sandbox_bucket,
            )
            yield GCSDocument(
                document_id=document_id,
                gcs_file_path=gcs_path,
                mime_type=DOCUMENT_MIME_TYPE,
            )
