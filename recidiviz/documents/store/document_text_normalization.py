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
"""The text-normalization steps applied to document_text at store time."""

import attr

from recidiviz.common import attr_validators, recidiviz_attr_validators


@attr.define(frozen=True, kw_only=True)
class DocumentTextNormalization:
    """A text-normalization step, expressed as an ordered list of
    BigQuery REGEXP_REPLACE substitutions applied to document_text."""

    name: str = attr.ib(
        validator=[
            attr_validators.is_non_empty_str,
            attr_validators.is_upper_snake_case,
        ]
    )
    """UPPER_SNAKE_CASE identifier for the step."""

    description: str = attr.ib(
        validator=recidiviz_attr_validators.is_meaningful_description
    )
    """What the step does and why."""

    substitutions: list[tuple[str, str]] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(tuple),
        ]
    )
    """(regex_pattern, replacement) pairs applied in order via REGEXP_REPLACE. Each
    pattern is emitted as a BigQuery raw string, so it must be a valid RE2 pattern."""

    def __attrs_post_init__(self) -> None:
        for pattern, replacement in self.substitutions:
            for value in (pattern, replacement):
                # The composed SQL is later run through a str.format-style project_id
                # interpolation, which reads `{`/`}` as a format field, so no pattern
                # or replacement may contain a curly brace (rules out `{n,}`-style
                # quantifiers).
                if "{" in value or "}" in value:
                    raise ValueError(
                        f"Normalization [{self.name}] has a substitution containing a "
                        f"curly brace, which the project_id interpolation would misread "
                        f"as a format field: [{value}]"
                    )

    def apply_sql(self, inner_sql: str) -> str:
        """Returns |inner_sql| wrapped in this step's REGEXP_REPLACE substitutions."""
        sql = inner_sql
        for pattern, replacement in self.substitutions:
            # apply_sql emits the pattern and replacement inside single-quoted SQL
            # string literals, so a single quote in either would terminate its literal
            # early. Escaping it as \' keeps the literal intact: in the non-raw
            # replacement literal \' is a bare quote, and in the raw pattern literal it
            # is a backslash-quote, which RE2 reads as a literal quote all the same.
            escaped_pattern = pattern.replace("'", r"\'")
            escaped_replacement = replacement.replace("'", r"\'")
            sql = (
                f"REGEXP_REPLACE({sql}, r'{escaped_pattern}', '{escaped_replacement}')"
            )
        return sql


def _html_tag_pattern(tag_names: list[str]) -> str:
    """Returns an RE2 pattern that matches an opening, closing, or self-closing HTML
    tag (with any attributes) whose name is in |tag_names|, case-insensitively. The
    pattern requires whitespace, `/`, or `>` directly after the tag name, so a name
    only matches at a word boundary (`<br>` matches for name `br`; `<broken>` does
    not). Only allowlisted names match at all, so angle-bracket text that is not
    HTML — a comparison like `score <10 but >5`, a bracketed aside like
    `<important note>`, a bracketed email address — survives normalization.
    """
    return rf"(?i)</?({'|'.join(tag_names)})(\s[^<>]*)?/?>"


# HTML tag names that end a line or block of text. Stripping one of these tags
# substitutes a newline, so the text on either side stays on separate lines.
_BLOCK_HTML_TAG_NAMES = [
    "blockquote",
    "br",
    "center",
    "div",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "li",
    "ol",
    "p",
    "pre",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
]

# HTML tag names that style text in place. Stripping one of these tags substitutes
# nothing, which keeps the surrounding text joined exactly as the source system
# displays it.
_INLINE_HTML_TAG_NAMES = [
    "a",
    "b",
    "big",
    "code",
    "em",
    "font",
    "i",
    "img",
    "s",
    "small",
    "span",
    "strike",
    "strong",
    "sub",
    "sup",
    "tt",
    "u",
]


STRIP_HTML_COMMENTS = DocumentTextNormalization(
    name="STRIP_HTML_COMMENTS",
    description=(
        "Removes HTML comments (<!-- ... -->), including comments that span multiple "
        "lines or that wrap HTML tags."
    ),
    substitutions=[
        # (?s) lets `.` match newlines; the non-greedy `.*?` stops each match at the
        # nearest `-->`, so two comments on one line are removed separately.
        (r"(?s)<!--.*?-->", "")
    ],
)

STRIP_HTML_TAGS = DocumentTextNormalization(
    name="STRIP_HTML_TAGS",
    description=(
        "Removes HTML tags whose name is on an allowlist of tags seen in document "
        "text. Block-level tags (e.g. <br>, <p>) become a newline; inline tags "
        "(e.g. <span>) are removed in place. Angle-bracket text that is not an "
        "allowlisted tag (comparisons like 'score <10', bracketed asides) is kept."
    ),
    substitutions=[
        (_html_tag_pattern(_BLOCK_HTML_TAG_NAMES), r"\n"),
        (_html_tag_pattern(_INLINE_HTML_TAG_NAMES), ""),
    ],
)

DECODE_HTML_ENTITIES = DocumentTextNormalization(
    name="DECODE_HTML_ENTITIES",
    description=(
        "Decodes common HTML entities to the character the source system displays, "
        "so the model's quotes of that character match the stored text. Matching is "
        "case-sensitive, per the HTML spec (&AMP; is not a defined entity)."
    ),
    substitutions=[
        ("&nbsp;", " "),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", '"'),
        ("&apos;", "'"),
        # &amp; must stay last so a double-encoded entity (e.g. &amp;lt;) decodes
        # exactly one level per pass instead of collapsing straight to '<'.
        ("&amp;", "&"),
    ],
)

COLLAPSE_WHITESPACE = DocumentTextNormalization(
    name="COLLAPSE_WHITESPACE",
    description=(
        "Normalizes whitespace while preserving line structure: converts carriage "
        "returns to newlines; collapses runs of other whitespace (tabs, vertical "
        "tabs, form feeds, spaces, and Unicode space separators such as the "
        "non-break space) to a single space; strips per-line leading and trailing "
        "whitespace; collapses runs of blank lines to a single blank line; and "
        "trims the document."
    ),
    substitutions=[
        # Carriage returns (CRLF or lone CR) to newlines, so the steps below can
        # reason about line breaks as a single \n.
        (r"\r\n?", r"\n"),
        # Strip non-newline whitespace around each newline — the per-line leading
        # and trailing whitespace. \pZ is the Unicode separator category, which
        # covers the ASCII space and non-ASCII spaces such as the non-break space
        # (U+00A0), which RE2's ASCII-only \s misses.
        (r"[\t\v\f\pZ]*\n[\t\v\f\pZ]*", r"\n"),
        # Collapse remaining interior runs of non-newline whitespace to a single
        # space.
        (r"[\t\v\f\pZ]+", " "),
        # Collapse runs of blank lines to a single blank line.
        (r"\n\n+", r"\n\n"),
        # Trim leading/trailing whitespace of the whole document.
        (r"^\s+|\s+$", ""),
    ],
)

# Order meaningful
ALL_NORMALIZATIONS: list[DocumentTextNormalization] = [
    # Remove comments before tags, so a comment that wraps a tag is removed whole.
    STRIP_HTML_COMMENTS,
    # Strip tags before entity decode, so a decoded '<' or '>' cannot form a tag
    # the next step would strip.
    STRIP_HTML_TAGS,
    # Decode entities before whitespace collapse, so a decoded &nbsp; collapses
    # with the whitespace around it.
    DECODE_HTML_ENTITIES,
    COLLAPSE_WHITESPACE,
]


def build_normalized_document_text_sql(column: str) -> str:
    """Returns the SQL expression that normalizes |column| by applying every step in
    ALL_NORMALIZATIONS, in order."""
    sql = column
    for normalization in ALL_NORMALIZATIONS:
        sql = normalization.apply_sql(sql)
    return sql
