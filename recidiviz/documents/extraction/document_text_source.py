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
"""The document-text interface the extraction request builder consumes: an id
plus a way to fetch the document's text.
"""
from typing import Protocol


class DocumentTextSource(Protocol):
    """A document the extraction request builder can assemble a request for.
    Implemented by `GCSDocumentTextSource` (the pipeline path) and by
    `GoldenEvalDocument` (the golden eval path), which already holds its text.
    """

    @property
    def document_contents_id(self) -> str:
        """Identifier of the document, carried onto the extraction request so the
        request's result can be matched back to the document.
        """

    def fetch_document_text(self) -> str:
        """Returns the document's full text.

        Raises `LLMDocumentExtractionRequestError` when the text cannot be fetched.
        """
