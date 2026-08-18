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
"""The sandbox a document-store run operates in: where each collection the run reads
or writes has its tables.
"""

import attr

from recidiviz.common import attr_validators


@attr.define(frozen=True, kw_only=True)
class DocumentCollectionSandboxLocation:
    """Where one document collection's document-store tables live for a sandbox run."""

    output_prefix: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Sandbox dataset prefix the run writes this collection's tables under. None means
    this run does not produce the collection, so it lives in the production document store."""

    diff_read_prefix: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Sandbox dataset prefix discovery diffs this collection's freshly generated
    documents against, or None to diff against the production document store."""


@attr.define(frozen=True, kw_only=True)
class DocumentStoreSandboxContext:
    """The sandbox a document-store run operates in: where each collection the run reads
    or writes has its tables, keyed separately for the document collections whose
    document store tables the run reads and writes and for the extractor collections
    whose extraction results it reads."""

    document_collection_locations: dict[
        str, DocumentCollectionSandboxLocation
    ] = attr.ib(
        validator=attr_validators.is_dict_where_each(
            key_validator=attr_validators.is_str,
            value_validator=attr.validators.instance_of(
                DocumentCollectionSandboxLocation
            ),
        )
    )
    """Read/write locations for each document collection the run touches, keyed by
    document collection name."""

    extractor_collection_read_prefixes: dict[str, str | None] = attr.ib(
        validator=attr_validators.is_dict_where_each(
            key_validator=attr_validators.is_str,
            value_validator=attr_validators.is_opt_str,
        )
    )
    """The sandbox dataset prefix scoping each extractor collection's extraction result
    tables the run reads, keyed by extractor collection name, where None means read from
    the production extraction results."""

    def output_prefix_for_writing(self, document_collection_name: str) -> str:
        """Returns the sandbox dataset prefix the run writes |document_collection_name|'s
        tables under. Raises if the collection is mapped to production, since a run never
        writes to the production document store."""
        location = self._document_collection_location(document_collection_name)
        if location.output_prefix is None:
            raise ValueError(
                f"Document collection [{document_collection_name}] is mapped to the "
                f"production document store, so a sandbox run cannot write its tables."
            )
        return location.output_prefix

    def source_read_prefix_for_document_collection(
        self, document_collection_name: str
    ) -> str | None:
        """Returns the sandbox dataset prefix a downstream reader reads
        |document_collection_name|'s written contents from, or None to read the production
        document store. This is the collection's output location, tolerating the
        production (unwritten) case."""
        return self._document_collection_location(
            document_collection_name
        ).output_prefix

    def diff_read_prefix_for_document_collection(
        self, document_collection_name: str
    ) -> str | None:
        """Returns the sandbox dataset prefix discovery diffs |document_collection_name|'s
        freshly generated documents against, or None to diff against the production
        document store."""
        return self._document_collection_location(
            document_collection_name
        ).diff_read_prefix

    def read_prefix_for_extractor_collection(
        self, extractor_collection_name: str
    ) -> str | None:
        """Returns the sandbox dataset prefix scoping the extraction result tables the run
        reads for |extractor_collection_name|, or None to read the production extraction
        results."""
        if extractor_collection_name not in self.extractor_collection_read_prefixes:
            raise ValueError(
                f"No sandbox location declared for extractor collection "
                f"[{extractor_collection_name}]. Every collection a run reads or writes "
                f"needs an entry."
            )
        return self.extractor_collection_read_prefixes[extractor_collection_name]

    def _document_collection_location(
        self, document_collection_name: str
    ) -> DocumentCollectionSandboxLocation:
        """Returns the sandbox location for |document_collection_name|, raising if the run
        declared none."""
        if document_collection_name not in self.document_collection_locations:
            raise ValueError(
                f"No sandbox location declared for document collection "
                f"[{document_collection_name}]. Every collection a run reads or writes "
                f"needs an entry."
            )
        return self.document_collection_locations[document_collection_name]
