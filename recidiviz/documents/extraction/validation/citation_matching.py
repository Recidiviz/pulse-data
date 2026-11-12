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
"""Locates an extracted field's cited quote within the document it was extracted
from. CitationGroundingCheck uses it to decide whether a quote appears in the
document at all, and CitationOffsetDriftAdjustment uses it to correct the offsets
the model reported for the quote.
"""
import re

import attr

from recidiviz.common import attr_validators

_WHITESPACE_RUN_PATTERN = r"\s+"


@attr.define(frozen=True, kw_only=True)
class CitationSpan:
    """Where a cited quote actually appears in the source document: the character
    offsets of the span, in the document's own coordinates.
    """

    start: int = attr.ib(validator=attr_validators.is_non_negative_int)
    """Offset of the span's first character."""

    end: int = attr.ib(validator=attr_validators.is_non_negative_int)
    """Offset one past the span's last character, so `document_text[start:end]`
    is the quoted span."""

    def __attrs_post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError(
                f"Citation span must be non-empty, but end [{self.end}] is not "
                f"after start [{self.start}]."
            )


@attr.define(frozen=True, kw_only=True)
class SourceDocumentText:
    """The text of the document an extraction's citations are matched against. Matching
    ignores case and treats any run of whitespace as equal to any other, but is
    otherwise exact.
    """

    text: str = attr.ib(validator=attr_validators.is_str)
    """The document text exactly as it was handed to the model."""

    def find_citation_span(self, *, citation_text: str) -> CitationSpan | None:
        """Returns the span of the first occurrence of |citation_text| in this
        document, or None when it appears nowhere.

        The offsets the model reports for a citation are unreliable, so they play
        no part in choosing a span. A quote that is empty or nothing but
        whitespace matches nothing.

        Whitespace at either end of the quote is ignored, and each run of
        whitespace inside it matches any run of whitespace in the document. For
        example, the quote "123 Example Dr Denver" matches the document text
        "123 Example Dr\n\nDenver", and the span covers the document text, blank
        line included.
        """
        words = citation_text.split()
        if not words:
            return None
        pattern = _WHITESPACE_RUN_PATTERN.join(re.escape(word) for word in words)
        match = re.search(pattern, self.text, flags=re.IGNORECASE)
        if match is None:
            return None
        return CitationSpan(start=match.start(), end=match.end())
