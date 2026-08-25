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
"""Tests for document_text_normalization.py."""

import unittest

from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
    ENTRY_HEADER_PREFIX,
    ENTRY_HEADER_SUFFIX,
    NULL_ENTITY_FIELD_PLACEHOLDER,
    SOURCE_DOCUMENT_HEADER_PREFIX,
    SOURCE_DOCUMENT_HEADER_SUFFIX,
    SOURCE_DOCUMENT_TEXT_LABEL,
)
from recidiviz.documents.store.document_text_normalization import (
    ALL_NORMALIZATIONS,
    COLLAPSE_WHITESPACE,
    DECODE_HTML_ENTITIES,
    STRIP_HTML_COMMENTS,
    STRIP_HTML_TAGS,
    DocumentTextNormalization,
    build_normalized_document_text_sql,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

# Raw document text -> expected normalized text. Every case in this table must also
# be idempotent: a second normalization pass leaves the expected text unchanged.
# Idempotence is a production property, not a theoretical one — the composite
# entity-resolution generation query embeds already-normalized first-order document
# text, and the generation wrapper normalizes the composite text again.
_NORMALIZATION_CASES = {
    # --- HTML tags ---
    # Allowlisted tags are removed; inline tags leave the surrounding text joined
    # in place.
    '<p>Hello <span style="font-weight:bold">world</span></p>': "Hello world",
    # Block tags become line breaks, so tag removal never joins words.
    "one<br>two": "one\ntwo",
    "<p>a</p><p>b</p>": "a\n\nb",
    # Tag matching is case-insensitive and accepts self-closing forms.
    "one<BR>two": "one\ntwo",
    "one<br/>two": "one\ntwo",
    "one<br />two": "one\ntwo",
    "one</P>two": "one\ntwo",
    # A block tag with attributes is removed like a bare one.
    '<p class="note">a</p>': "a",
    # --- Angle brackets that are not HTML ---
    # Comparison operators: the character after '<' is not a tag name.
    "score <10 but >5": "score <10 but >5",
    # A '<'...'>' span across a newline is not treated as a tag.
    "a < b\nc > d": "a < b\nc > d",
    # A '<' with no '>' anywhere in the document.
    "5 < 10": "5 < 10",
    "<3 my kids": "<3 my kids",
    # A bracketed multi-word aside: 'important' is not an allowlisted tag name.
    "<important note>": "<important note>",
    # An allowlisted tag name ('br') as the prefix of a longer word is not a tag.
    "<broken>": "<broken>",
    # A bare inline tag strips in place, so mid-word emphasis does not gain a space.
    "<b>bold</b> and mis<i>take</i>": "bold and mistake",
    # A tag name not on the allowlist survives (an unlisted real tag re-creates the
    # old citation-mismatch retries; a stripped false positive silently corrupts
    # stored text).
    "<blink>text</blink>": "<blink>text</blink>",
    # A bracketed email address.
    "<john@example.com>": "<john@example.com>",
    # --- HTML comments ---
    "before<!-- hidden -->after": "beforeafter",
    # A comment that spans lines, or wraps a tag, is removed whole.
    "a<!--\nhidden\n-->b": "ab",
    "a<!-- <br> -->b": "ab",
    # Two comments on one line are removed separately (the match is non-greedy),
    # so the text between them survives.
    "a<!-- x -->keep<!-- y -->b": "akeepb",
    # --- HTML entities ---
    "Smith &amp; Jones": "Smith & Jones",
    "a&nbsp;&nbsp;b": "a b",
    # Entity decode runs after tag-stripping, so a decoded '<' does not create a
    # strippable tag; the comparison survives re-normalization too.
    "BAC &lt; 0.08": "BAC < 0.08",
    # Quote entities decode to the bare quote the source system displays. &apos; in
    # particular exercises a substitution whose replacement is a single quote, which
    # apply_sql emits into the SQL string literal escaped.
    "she said &quot;hi&quot;": 'she said "hi"',
    "it&apos;s here": "it's here",
    # Entity matching is case-sensitive per the HTML spec, so an uppercase named
    # entity is not a defined entity and is left untouched.
    "A &AMP; B": "A &AMP; B",
    # --- Whitespace ---
    # Tabs and multi-space runs collapse to a single space; single newlines and a
    # single blank-line paragraph break are preserved.
    "Line one\t\ttext\r\n\r\n\r\nLine two": "Line one text\n\nLine two",
    # Per-line leading whitespace is stripped and the document is trimmed.
    "   Hello\n    world  ": "Hello\nworld",
    # CRLF becomes a single LF.
    "a\r\nb": "a\nb",
    # A non-break space (U+00A0) collapses like an ASCII space.
    "a\u00a0\u00a0b": "a b",
    # Already-clean text is unchanged.
    "Already clean text.": "Already clean text.",
    # --- Combined ---
    "<div>  first</div>\r\n\r\n\r\n\t\tsecond   line": "first\n\nsecond line",
    "<span>  a\t\tb </span>\r\n\r\n\r\n   c  ": "a b\n\nc",
    # A document of only tags and whitespace normalizes to the empty string; the
    # generation query's WHERE clause then drops the row (see
    # document_generation_query_builder_test.py).
    "<br> \t ": "",
}

# Raw document text -> expected normalized text for cases that are correct after one
# pass but NOT idempotent, so they are excluded from the idempotence sweep: a decoded
# entity can spell an allowlisted tag or another entity, which a second pass would
# process again. These stay rare shapes; the common entity cases above are idempotent.
_SINGLE_PASS_ONLY_CASES = {
    # A double-encoded entity decodes exactly one level, because &amp; decodes last.
    "&amp;lt;": "&lt;",
    # A decoded entity pair can spell an allowlisted tag; a second pass would strip
    # it.
    "&lt;p&gt;": "<p>",
}


class BuildNormalizedDocumentTextSqlTest(unittest.TestCase):
    """Tests the SQL composition, independent of execution."""

    def test_composes_steps_in_registry_order_innermost_first(self) -> None:
        step_one = DocumentTextNormalization(
            name="STEP_ONE",
            description="First stand-in step for ordering.",
            substitutions=[("a", "b")],
        )
        step_two = DocumentTextNormalization(
            name="STEP_TWO",
            description="Second stand-in step for ordering.",
            substitutions=[("c", "d")],
        )
        self.assertEqual(
            "REGEXP_REPLACE(REGEXP_REPLACE(col, r'a', 'b'), r'c', 'd')",
            step_two.apply_sql(step_one.apply_sql("col")),
        )

    def test_apply_sql_nests_multiple_substitutions_in_order(self) -> None:
        self.assertEqual(
            "REGEXP_REPLACE(REGEXP_REPLACE(document_text, r'<[^>]*>', ''), r'x', 'y')",
            DocumentTextNormalization(
                name="MULTI",
                description="A step with two substitutions.",
                substitutions=[(r"<[^>]*>", ""), ("x", "y")],
            ).apply_sql("document_text"),
        )

    def test_substitution_with_format_brace_rejected(self) -> None:
        for substitution in [(r"\n{2,}", r"\n\n"), (r"\n+", "{x}")]:
            with self.subTest(substitution=substitution):
                with self.assertRaisesRegex(
                    ValueError,
                    r"^Normalization \[BAD\] has a substitution containing a curly "
                    r"brace",
                ):
                    DocumentTextNormalization(
                        name="BAD",
                        description="A step with a curly brace.",
                        substitutions=[substitution],
                    )

    def test_apply_sql_escapes_single_quotes(self) -> None:
        # A single quote in either the pattern (raw literal) or the replacement
        # (non-raw literal) is emitted escaped as \' so it does not terminate the SQL
        # string literal early. Execution against the emulator is covered by the
        # &apos; case in _NORMALIZATION_CASES.
        self.assertEqual(
            r"REGEXP_REPLACE(REGEXP_REPLACE(col, r'don\'t', 'do not'), r'x', 'it\'s')",
            DocumentTextNormalization(
                name="QUOTES",
                description="A step with a single quote in a pattern and a replacement.",
                substitutions=[(r"don't", "do not"), ("x", "it's")],
            ).apply_sql("col"),
        )

    def test_registry_order(self) -> None:
        # Comments before tags (a comment can wrap a tag), tags before entities (a
        # decoded '<' must not form a tag), entities before whitespace (a decoded
        # &nbsp; must collapse).
        self.assertEqual(
            [
                STRIP_HTML_COMMENTS,
                STRIP_HTML_TAGS,
                DECODE_HTML_ENTITIES,
                COLLAPSE_WHITESPACE,
            ],
            ALL_NORMALIZATIONS,
        )


class NormalizedDocumentTextExecutionTest(BigQueryEmulatorTestCase):
    """Runs the composed normalization SQL on the emulator to pin its semantics:
    allowlisted HTML tags, comments, and entities are normalized; angle-bracket text
    that is not HTML survives; whitespace collapses while single newlines are
    preserved."""

    def _apply_sql_to_literal(self, sql_expression: str, raw: str) -> str:
        """Returns the result of evaluating |sql_expression| (which references a
        document_text column) against |raw| on the emulator."""
        escaped = raw.replace("\\", "\\\\").replace("'", "\\'")
        query = (
            f"WITH t AS (SELECT '''{escaped}''' AS document_text) "
            f"SELECT {sql_expression} AS result FROM t"
        )
        rows = list(
            self.bq_client.run_query_async(query_str=query, use_query_cache=True)
        )
        return rows[0]["result"]

    def _normalize(self, raw: str) -> str:
        return self._apply_sql_to_literal(
            build_normalized_document_text_sql("document_text"), raw
        )

    def test_every_registered_step_is_valid_bigquery_regex(self) -> None:
        # Executing each step's SQL on the emulator rejects an invalid RE2 pattern
        # with a query error, so this guards every current and future registered step
        # against the pattern-validity claim in DocumentTextNormalization.substitutions,
        # independent of whether the step has a dedicated semantics case below.
        for normalization in ALL_NORMALIZATIONS:
            with self.subTest(normalization=normalization.name):
                self._apply_sql_to_literal(
                    normalization.apply_sql("document_text"), "sample text"
                )

    def test_normalization_semantics(self) -> None:
        for raw, expected in {
            **_NORMALIZATION_CASES,
            **_SINGLE_PASS_ONLY_CASES,
        }.items():
            with self.subTest(raw=raw):
                self.assertEqual(expected, self._normalize(raw))

    def test_normalization_is_idempotent(self) -> None:
        # test_normalization_semantics pins normalize(raw) == expected, so asserting
        # normalize(expected) == expected here pins the second pass as a no-op. The
        # composite entity-resolution generation query re-normalizes already-normalized
        # first-order text, so a non-idempotent step would alter stored text there.
        for expected in _NORMALIZATION_CASES.values():
            with self.subTest(expected=expected):
                self.assertEqual(expected, self._normalize(expected))

    def test_composite_document_markers_survive_renormalization(self) -> None:
        # The composite entity-resolution generation query renders literal markers
        # (source-document and entry headers, the source-text label, the null-field
        # placeholder) around already-normalized first-order text, then the store's
        # generation wrapper normalizes the whole composite again. A rendered composite
        # document must therefore be normalization-stable: if a marker were reworded to
        # contain an allowlisted HTML tag name, an HTML entity, or a collapsing
        # whitespace run, this second pass would silently rewrite stored composite text
        # and re-hash every composite document_contents_id. Building the fixture from
        # the real marker constants makes such a change fail here.
        rendered_composite = "\n\n".join(
            [
                "\n".join(
                    [
                        f"{SOURCE_DOCUMENT_HEADER_PREFIX}2024-01-15"
                        f"{SOURCE_DOCUMENT_HEADER_SUFFIX}",
                        f"{SOURCE_DOCUMENT_TEXT_LABEL}: Client reported new job.\n",
                        f"{ENTRY_HEADER_PREFIX}1{ENTRY_HEADER_SUFFIX}",
                        "employer_name: McDonalds",
                    ]
                ),
                "\n".join(
                    [
                        f"{ENTRY_HEADER_PREFIX}2{ENTRY_HEADER_SUFFIX}",
                        f"employer_name: {NULL_ENTITY_FIELD_PLACEHOLDER}",
                    ]
                ),
            ]
        )
        self.assertEqual(rendered_composite, self._normalize(rendered_composite))
