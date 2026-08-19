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
"""Tests for DocumentStoreSandboxContext."""

import unittest

from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentCollectionSandboxLocation,
    DocumentStoreSandboxContext,
)

_SEEDED = "seeded_collection"
_PRODUCTION = "production_collection"
_EXTRACTOR = "extractor_collection"


class TestDocumentStoreSandboxContext(unittest.TestCase):
    """Tests the per-collection accessors and their production/undeclared handling."""

    def setUp(self) -> None:
        self.context = DocumentStoreSandboxContext(
            document_collection_locations={
                # Seeded from empty: written to the sandbox and diffed against it.
                _SEEDED: DocumentCollectionSandboxLocation(
                    output_prefix="pfx", diff_read_prefix="pfx"
                ),
                # Not sandboxed: not written this run, diffed against production.
                _PRODUCTION: None,
            },
            extractor_collection_read_prefixes={
                _EXTRACTOR: "pfx",
                _PRODUCTION: None,
            },
        )

    def test_output_prefix_for_writing(self) -> None:
        self.assertEqual("pfx", self.context.output_prefix_for_writing(_SEEDED))
        # An unsandboxed collection has no sandbox output to write to.
        with self.assertRaisesRegex(
            ValueError,
            r"Document collection \[production_collection\] is not sandboxed by this "
            r"run",
        ):
            self.context.output_prefix_for_writing(_PRODUCTION)

    def test_source_read_and_diff_read_prefixes(self) -> None:
        self.assertEqual(
            "pfx", self.context.source_read_prefix_for_document_collection(_SEEDED)
        )
        self.assertEqual(
            "pfx", self.context.diff_read_prefix_for_document_collection(_SEEDED)
        )
        # An unsandboxed collection reads and diffs against production (None).
        self.assertIsNone(
            self.context.source_read_prefix_for_document_collection(_PRODUCTION)
        )
        self.assertIsNone(
            self.context.diff_read_prefix_for_document_collection(_PRODUCTION)
        )

    def test_extractor_read_prefix(self) -> None:
        self.assertEqual(
            "pfx", self.context.read_prefix_for_extractor_collection(_EXTRACTOR)
        )
        self.assertIsNone(
            self.context.read_prefix_for_extractor_collection(_PRODUCTION)
        )

    def test_undeclared_collections_raise(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^No sandbox location declared for document collection \[missing\]\.",
        ):
            self.context.diff_read_prefix_for_document_collection("missing")
        with self.assertRaisesRegex(
            ValueError,
            r"^No sandbox location declared for extractor collection \[missing\]\.",
        ):
            self.context.read_prefix_for_extractor_collection("missing")
