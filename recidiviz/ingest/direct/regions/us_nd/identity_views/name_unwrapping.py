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
"""SQL fragments that unwrap US_ND raw name-part columns for identity ingest.

ND raw name columns embed alias name parts (parenthetical and quoted
nicknames, after-slash names), generational suffixes, and data-entry notes.
The fragments here strip or extract those pieces so that embedded alias name
parts and generational suffixes land in their own fields rather than staying
buried in the primary name; build_alias_list_sql assembles the extracted
parts into partial-name alias structs, one per filled field.

The values these fragments emit are US_ND-unwrapped but not yet
validator-clean: they may still carry characters the IdentityName /
IdentityAlias validators reject. Making a value conform to those validators
happens at mapping time, in
recidiviz/pipelines/ingest/identity/name_custom_parsers.py.

This module is US_ND-local. A second state needing the same unwrapping should
promote it to a shared identity home rather than copy it.
"""

from recidiviz.common.constants.name_suffixes import EMBEDDED_NAME_SUFFIX_TOKENS

# Matches a whole value that carries no name content -- no letter anywhere in
# it ("*", "123", ""). Anchored at both ends so it either consumes the entire
# value or matches nothing. The fragments below use it to null such a value,
# since several of them treat NULL as "this column holds no name".
_NO_NAME_CONTENT_PATTERN = r"^[^\pL]*$"

# The same for a suffix column, where a purely numeric value ("2") is a real
# suffix and must not null out.
_NO_SUFFIX_CONTENT_PATTERN = r"^[^\pL0-9]*$"

# Matches markers that flag a parenthetical as a data-entry note rather than a
# name: an asterisk (never valid in a name) or an instruction-like word. SEE,
# AKA, NONE, and the UNK spellings require a trailing word boundary because
# each is a prefix of a real name; a bare "SEE" would otherwise misclassify
# parenthetical nicknames like "(SEELEY)" and "(AKASHA)" as notes and drop the
# alias, and a bare "UNK" would swallow the real surnames UNKE and UNKEFER.
# DECEAS stays prefix-open so it still catches "DECEASED"; it prefixes no real
# name.
_CONTAINS_NOTE_MARKER_PATTERN = r"\*|(?i)\b((SEE|AKA|NONE|UNK|UNKN|UNKNOWN)\b|DECEAS)"

# Matches a field that is entirely a data-entry note marker ("AKA", "NONE",
# "UNKNOWN", ...) or an absent-name placeholder. The placeholders are the
# standard corrections/law-enforcement codes for a missing name part: NMN
# ("no middle name"), NMI ("no middle initial"), FNU/LNU ("first/last name
# unknown"), and N/A. Anchored to the whole (trimmed) field, so it never
# matches a real name that merely contains the letters ("NONE" matches;
# "NONEMAN" does not). The optional trailing (...) also matches
# marker-with-suffix forms like "AKA(S)" (short for "alias(es)"). The pattern
# enumerates the UNK spellings rather than using a prefix wildcard because a
# wildcard would swallow the real surnames UNKE and UNKEFER. SEE is absent for
# the same reason (it is a real surname), and an actual "see ..." instruction
# ("SEE ALIAS FILE") is more than one token, so it never matches this anchored
# single-token pattern anyway.
_WHOLE_FIELD_NOTE_OR_PLACEHOLDER_PATTERN = (
    r"(?i)^\s*(A[./]?K[./]?A\.?|DBA|FKA|NKA|ALIAS|NONE|UNK|UNKN|UNKNOWN|DECEAS\w*"
    r"|NMN|NMI|FNU|LNU|N/A)"
    r"(\s*\([^)]*\))?\s*$"
)

# Matches a leading "also known as" style marker (with or without dots or
# slashes) that some source name fields carry in front of the actual name
# ("AKA JOHNNY", "A/K/A BOBBY"). Anchored and space-terminated so it matches
# only a standalone leading marker, never the start of a real name such as
# "AKALINA".
_LEADING_NOTE_MARKER_PATTERN = r"(?i)^\s*(A[./]?K[./]?A\.?|DBA|FKA|NKA)\s+"

# Matches a trailing generational suffix (JR, SR, II, ...) glued onto the end
# of a name part ("SMITH JR" in a surname field). The suffix vocabulary comes
# from the same constant the clustering pipeline's name comparison uses, so
# the two cannot drift (see recidiviz/common/constants/name_suffixes.py).
# Sorting the tokens longest-first makes the alternation prefer "III" over
# "II". The required leading whitespace means the pattern never matches a
# field that is entirely a suffix ("JR"), matching signals.normalize_name,
# which never strips a suffix that is the whole name.
_TRAILING_SUFFIX_ALTERNATION = "|".join(
    sorted(EMBEDDED_NAME_SUFFIX_TOKENS, key=len, reverse=True)
)
_TRAILING_SUFFIX_PATTERN = rf"(?i)\s+((?:{_TRAILING_SUFFIX_ALTERNATION})\.?)$"


def build_alias_list_sql(*, field_to_expression: dict[str, str]) -> str:
    """Returns a SQL expression that JSON-encodes one partial alias per name part
    that has an embedded alias name part. |field_to_expression| maps each struct
    field name (given_name, surname, ...) to the SQL expression that fills it.
    The result is one STRUCT row per field, holding only that field's alias name
    part with the others NULL, filtered to rows where at least one field is
    non-null.
    Every row spells out every field in |field_to_expression| order because
    BigQuery requires all elements of an array literal to share one struct
    type (same field names, same order)."""
    field_names = list(field_to_expression)
    struct_rows = []
    for filled_field in field_names:
        struct_fields = [
            f"{field_to_expression[filled_field]} AS {field}"
            if field == filled_field
            else f"CAST(NULL AS STRING) AS {field}"
            for field in field_names
        ]
        struct_rows.append(f"STRUCT({', '.join(struct_fields)})")
    struct_array = ",\n      ".join(struct_rows)
    select_fields = ", ".join(field_names)
    not_null_predicate = " OR ".join(f"{field} IS NOT NULL" for field in field_names)
    return f"""TO_JSON_STRING(ARRAY(
    SELECT AS STRUCT {select_fields}
    FROM UNNEST([
      {struct_array}
    ])
    WHERE {not_null_predicate}
  ))"""


def unwrap_primary_name_sql(column: str) -> str:
    """Returns a SQL expression producing the unwrapped primary name from
    |column|: nulls a field that is entirely a note marker, strips a leading
    "also known as" marker, drops parenthetical (...), quoted "...", and
    asterisk-wrapped *...* segments, keeps only the first slash-separated
    segment, turns underscores into spaces, and normalizes whitespace. Nulls a
    result with no name content left."""
    note_field_removed = (
        f"REGEXP_REPLACE({column}, r'{_WHOLE_FIELD_NOTE_OR_PLACEHOLDER_PATTERN}', '')"
    )
    without_note_prefix = (
        f"REGEXP_REPLACE({note_field_removed}, r'{_LEADING_NOTE_MARKER_PATTERN}', '')"
    )
    without_parens = f"REGEXP_REPLACE({without_note_prefix}, r'\\s*\\([^)]*\\)', '')"
    without_quotes = f"REGEXP_REPLACE({without_parens}, r'\\s*\"[^\"]*\"', '')"
    # A paired-asterisk segment ("*DECEASED* SMITH") is a data-entry note, like
    # a note parenthetical. A single unpaired asterisk is left for the
    # mapping-time parser to strip.
    without_asterisk_notes = f"REGEXP_REPLACE({without_quotes}, r'\\s*\\*[^*]*\\*', '')"
    first_slash_segment = f"SPLIT({without_asterisk_notes}, '/')[OFFSET(0)]"
    underscores_to_spaces = f"REPLACE({first_slash_segment}, '_', ' ')"
    return _normalize_whitespace_and_null_no_content(
        underscores_to_spaces, _NO_NAME_CONTENT_PATTERN
    )


def unwrap_name_suffix_sql(column: str) -> str:
    """Returns a SQL expression producing the unwrapped name suffix from a
    dedicated raw suffix |column|: nulls a field that is entirely a note
    marker, normalizes whitespace, and nulls a result with no suffix content
    left. Digits survive, since "3RD" and "2" are real suffixes."""
    note_field_removed = (
        f"REGEXP_REPLACE({column}, r'{_WHOLE_FIELD_NOTE_OR_PLACEHOLDER_PATTERN}', '')"
    )
    return _normalize_whitespace_and_null_no_content(
        note_field_removed, _NO_SUFFIX_CONTENT_PATTERN
    )


def extract_alias_name_part_sql(column: str) -> str:
    """Returns a SQL expression producing the alias name part embedded in
    |column| (a parenthetical or quoted nickname, or the first slash-separated
    segment after the primary), or NULL when there is none, the parenthetical
    is a note rather than a name, or the result is a single character. A one-character value is noise
    ("AKA(S)" carries "S"), not a name, so the expression nulls it. Extraction
    first strips any leading "also known as" marker from the column so that
    "A/K/A BOBBY" does not fabricate an after-slash alias name part out of the
    marker's own slashes. Extraction deliberately skips the whole-field-note
    removal because a field like "AKA (JOHNNY)" nulls as a primary name while
    its parenthetical "JOHNNY" is a real alias name part that must survive."""
    without_note_prefix = (
        f"REGEXP_REPLACE({column}, r'{_LEADING_NOTE_MARKER_PATTERN}', '')"
    )
    parenthetical = f"REGEXP_EXTRACT({without_note_prefix}, r'\\(([^)]*)\\)')"
    quoted = f'REGEXP_EXTRACT({without_note_prefix}, r\'"([^"]*)"\')'
    # Capture only the first slash-separated segment, so a value with a second
    # slash drops the tail rather than handing the parser a slash to delete,
    # which would glue the segments into a name nobody has.
    after_slash = f"REGEXP_EXTRACT({without_note_prefix}, r'/([^/]*)')"
    # Drop the parenthetical when it is a note rather than a name.
    parenthetical_name = (
        f"IF(REGEXP_CONTAINS({parenthetical}, r'{_CONTAINS_NOTE_MARKER_PATTERN}'), "
        f"NULL, {parenthetical})"
    )
    alias_name_part = f"COALESCE({parenthetical_name}, {quoted}, {after_slash})"
    normalized = _normalize_whitespace_and_null_no_content(
        alias_name_part, _NO_NAME_CONTENT_PATTERN
    )
    return f"IF(CHAR_LENGTH({normalized}) < 2, NULL, {normalized})"


def strip_trailing_name_suffix_sql(name_expression: str) -> str:
    """Returns a SQL expression producing |name_expression| with a trailing
    generational suffix (JR, SR, II, ...) removed. Leaves a field that is
    entirely a suffix untouched and nulls an empty result. Expects an
    already-unwrapped name part (the output of unwrap_primary_name_sql)."""
    return (
        f"NULLIF(TRIM(REGEXP_REPLACE("
        f"{name_expression}, r'{_TRAILING_SUFFIX_PATTERN}', '')), '')"
    )


def extract_trailing_name_suffix_sql(name_expression: str) -> str:
    """Returns a SQL expression producing the trailing generational suffix (JR,
    SR, II, ...) embedded in |name_expression|, or NULL when there is none. A
    field that is entirely a suffix yields NULL. Expects an already-unwrapped
    name part (the output of unwrap_primary_name_sql)."""
    return f"REGEXP_EXTRACT({name_expression}, r'{_TRAILING_SUFFIX_PATTERN}')"


def coalesce_trailing_name_suffix_sql(*name_expressions: str) -> str:
    """Returns a SQL expression producing the first trailing generational suffix
    found across |name_expressions|, checked left to right, or NULL when none of
    them carries one."""
    extracts = ", ".join(
        extract_trailing_name_suffix_sql(expression) for expression in name_expressions
    )
    return f"COALESCE({extracts})"


def _normalize_whitespace_and_null_no_content(
    expression: str, no_content_pattern: str
) -> str:
    """Returns SQL that blanks out |expression| entirely when it matches
    |no_content_pattern| (an anchored whole-value match), collapses runs of
    whitespace to a single space, trims, and nulls out an empty result, so a
    column left holding only punctuation reads as NULL to the fragments that
    treat NULL as "no name here"."""
    return (
        f"NULLIF(TRIM(REGEXP_REPLACE("
        f"REGEXP_REPLACE({expression}, r'{no_content_pattern}', ''), "
        f"r'\\s+', ' ')), '')"
    )
