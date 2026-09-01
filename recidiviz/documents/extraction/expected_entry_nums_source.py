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
"""The interface an extraction session uses to look up the complete entry set of
a composite document, which the entry-partition check validates an
entity-resolution result against.
"""
from typing import Protocol

import attr

from recidiviz.common import attr_validators


class ExpectedEntryNumsSource(Protocol):
    """Looks up the complete entry set of a composite document, which the
    entry-partition check validates an entity-resolution result's clustering
    against. The interface constrains only the per-document lookup: an
    implementation may hold a preloaded map, fetch lazily, or read a durable
    store.
    """

    def get_expected_entry_nums(self, *, document_contents_id: str) -> set[int]:
        """Returns the composite document's complete entry set.

        Raises when the document is unknown to the source.
        """


@attr.define(frozen=True, kw_only=True)
class InMemoryExpectedEntryNumsSource:
    """An ExpectedEntryNumsSource over a preloaded map, for callers that read
    every relevant composite document's entry set up front.
    """

    entry_nums_by_document: dict[str, set[int]] = attr.ib(
        validator=attr_validators.is_dict_of(str, set)
    )
    """The complete entry set of each composite document, keyed by
    document_contents_id."""

    def get_expected_entry_nums(self, *, document_contents_id: str) -> set[int]:
        """Returns the composite document's complete entry set.

        Raises KeyError when the document has no entry in the preloaded map.
        """
        return self.entry_nums_by_document[document_contents_id]
