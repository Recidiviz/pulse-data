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
"""Shared builders for the result JSON the FAKE_EXTRACTOR_COLLECTION extractor
emits, conforming to the JSON Schema the extractor sends the model.

The per-field builders produce a single INFERRED field's JSON (value or null
branch) and one `assignments` array element. The result builders assemble those
into a whole document result — `build_fake_extractor_result_content` returns the
unwrapped `{is_relevant, ...}` content stored in the BQ result tables, and the
`fake_*_result_json` builders wrap that content in the top-level `{"result": ...}`
envelope the raw model response carries and the validator consumes.

Kept dependency-light (no BQ table or entity imports) so both the view builder
tests and the validator/processor tests can share it.
"""
import copy
from typing import Any, Iterator

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    ADVERSARIAL_INTERPRETATION_FIELD_NAME,
    CITATION_END_FIELD_NAME,
    CITATION_START_FIELD_NAME,
    CITATION_TEXT_FIELD_NAME,
    CITATIONS_FIELD_NAME,
    CONFIDENCE_LEVEL_FIELD_NAME,
    ENTITIES_FIELD_NAME,
    ENTITY_ID_FIELD_NAME,
    ENTRY_NUMS_FIELD_NAME,
    IS_RELEVANT_FIELD_NAME,
    NULL_REASON_FIELD_NAME,
    RESULT_KEY,
    VALUE_FIELD_NAME,
)


def build_inferred_field_result_json(
    value: Any,
    confidence_level: str = "explicit",
    adversarial_interpretation: str | None = None,
) -> dict[str, Any]:
    """Returns the JSON the extractor emits for one INFERRED field on the nonnull
    branch: its value plus the companion-metadata keys.

    |adversarial_interpretation| defaults to None, which is the only value
    consistent with the default `explicit` confidence level — validation requires
    an alternative reading to be recorded if and only if the confidence level is
    `speculative`. Pass the two together to build a speculative field.
    """
    return {
        VALUE_FIELD_NAME: value,
        CONFIDENCE_LEVEL_FIELD_NAME: confidence_level,
        ADVERSARIAL_INTERPRETATION_FIELD_NAME: adversarial_interpretation,
        CITATIONS_FIELD_NAME: [
            {
                CITATION_TEXT_FIELD_NAME: f"citation for {value}",
                CITATION_START_FIELD_NAME: 0,
                CITATION_END_FIELD_NAME: 15,
            }
        ],
    }


def build_null_inferred_field_result_json(
    null_reason: str = "no_info_found",
    confidence_level: str = "explicit",
    adversarial_interpretation: str | None = None,
    citation_text: str | None = None,
) -> dict[str, Any]:
    """Returns the JSON the extractor emits for one INFERRED field on the null
    branch: no value, just the null reason and the other companion-metadata keys.

    |adversarial_interpretation| defaults to None for the same reason it does on
    the nonnull branch.

    Citations default to empty, which is what the null branch usually carries. Pass
    |citation_text| to build a one-quote branch instead — needed when a test puts
    this wrapper where the schema allows only the value branch, whose `citations`
    requires at least one quote, so the missing `value` is the only thing flagged.
    """
    return {
        NULL_REASON_FIELD_NAME: null_reason,
        CONFIDENCE_LEVEL_FIELD_NAME: confidence_level,
        ADVERSARIAL_INTERPRETATION_FIELD_NAME: adversarial_interpretation,
        CITATIONS_FIELD_NAME: (
            []
            if citation_text is None
            else [
                {
                    CITATION_TEXT_FIELD_NAME: citation_text,
                    CITATION_START_FIELD_NAME: 0,
                    CITATION_END_FIELD_NAME: len(citation_text),
                }
            ]
        ),
    }


def build_fake_extractor_assignment_result_json(
    name: str, kind: str, rate: float, period: str
) -> dict[str, Any]:
    """Returns one element of the `assignments` ARRAY_OF_STRUCT field's JSON.

    `rate_amount` overrides the collection's minimum confidence level to
    `explicit`, so it is built at that level rather than the collection default —
    results built here stay clean through validation.
    """
    return {
        "assignment_name": build_inferred_field_result_json(name),
        "assignment_type": build_inferred_field_result_json(kind),
        "rate_amount": build_inferred_field_result_json(rate),
        "rate_period": build_inferred_field_result_json(period),
    }


def build_fake_extractor_result_content(
    *,
    primary_status: str,
    status_note: str,
    location: str | None,
    assignments: list[dict[str, Any]],
) -> dict[str, Any]:
    """Returns the full result-JSON content for one relevant extraction result.
    A None |location| takes the null branch (no value, with a null reason).
    """
    return {
        IS_RELEVANT_FIELD_NAME: True,
        "primary_status": build_inferred_field_result_json(primary_status),
        "status_note": status_note,
        "location": (
            build_inferred_field_result_json(location, "inferred")
            if location is not None
            else build_null_inferred_field_result_json()
        ),
        "assignments": assignments,
    }


def build_fake_extractor_irrelevant_result_content() -> dict[str, Any]:
    """Returns the result-JSON content stored for an irrelevant document — just
    the relevance determination, no extracted fields.
    """
    return {IS_RELEVANT_FIELD_NAME: False}


def build_fake_entity_resolution_entity_result_json(
    entity_id: int, *, entry_nums: list[int], **entity_field_values: Any
) -> dict[str, Any]:
    """Returns one element of the entity-resolution entities ARRAY_OF_STRUCT field's
    JSON: a resolved entity's sequential id, its canonical entity-field values, and the
    composite-document entry numbers it was clustered from.

    Entity fields are STRUCTURAL in the synthesized ER schema, so their values are bare
    rather than wrapped in the companion metadata a first-order INFERRED field carries.
    """
    return {
        ENTITY_ID_FIELD_NAME: entity_id,
        **entity_field_values,
        ENTRY_NUMS_FIELD_NAME: entry_nums,
    }


def build_fake_entity_resolution_result_content(
    entities: list[dict[str, Any]],
) -> dict[str, Any]:
    """Returns the full result-JSON content for one entity-resolution result, the
    resolved entities for a single composite document. The ER schema declares no
    is_relevant field, because every composite document is relevant by construction.
    """
    return {ENTITIES_FIELD_NAME: entities}


def wrap_in_result_key(result_content: dict[str, Any]) -> dict[str, Any]:
    """Wraps |result_content| in the top-level `{"result": ...}` envelope that the
    raw model response carries and the validator consumes.

    Raises if |result_content| is already wrapped, so passing a whole result JSON where
    extracted-fields content is expected fails loudly instead of producing a
    double-wrapped result that every reader would silently parse as empty.
    """
    if RESULT_KEY in result_content:
        raise ValueError(
            f"Result content already carries a [{RESULT_KEY}] key, so wrapping it "
            f"would double-wrap it. Pass the extracted-fields content, not a whole "
            f"result JSON. Found keys: {sorted(result_content)}."
        )
    return {RESULT_KEY: result_content}


def fake_minimal_relevant_result_json() -> dict[str, Any]:
    """Returns a wrapped result that conforms to FAKE_EXTRACTOR_COLLECTION's schema
    while reporting as little as it can: `primary_status` and `status_note` carry
    values, `location` takes its null branch, and `assignments` is empty.
    """
    return wrap_in_result_key(
        {
            IS_RELEVANT_FIELD_NAME: True,
            "primary_status": build_inferred_field_result_json("active"),
            "status_note": "Currently active.",
            "location": build_null_inferred_field_result_json(),
            "assignments": [],
        }
    )


def fake_all_fields_result_json() -> dict[str, Any]:
    """Returns a wrapped result that populates every field in
    FAKE_EXTRACTOR_COLLECTION's schema, including the nested `assignments` array and
    all of its sub-fields (a second element on the null branch), so the whole schema
    is exercised.
    """
    return wrap_in_result_key(
        build_fake_extractor_result_content(
            primary_status="active",
            status_note="Currently active.",
            location="Kitchen",
            assignments=[
                build_fake_extractor_assignment_result_json(
                    "Dish duty", "internal", 12.5, "hourly"
                ),
                {
                    "assignment_name": build_inferred_field_result_json("Laundry"),
                    "assignment_type": build_null_inferred_field_result_json(),
                    "rate_amount": build_null_inferred_field_result_json(),
                    "rate_period": build_null_inferred_field_result_json(
                        "not_applicable"
                    ),
                },
            ],
        )
    )


def fake_irrelevant_result_json() -> dict[str, Any]:
    """Returns a wrapped result for an irrelevant document — the irrelevant branch
    of the anyOf carries only is_relevant.
    """
    return wrap_in_result_key(build_fake_extractor_irrelevant_result_content())


FAKE_DOCUMENT_PREAMBLE = "Case note for the fake extractor collection.\n\n"
"""Lead-in text every grounded fake source document starts with, so the citations
that follow it sit at nonzero offsets and an off-by-anything offset bug can't pass
unnoticed."""

_GROUNDED_CITATION_SEPARATOR = "\n"


@attr.define(frozen=True, kw_only=True)
class GroundedFakeResult:
    """A fake result JSON paired with source document text that grounds it: every
    citation's quoted text appears in the document, at exactly the offsets that
    citation reports. Validation's citation checks pass such a pair cleanly, so
    it is the starting point for any test that needs a result that survives them.
    """

    source_document_text: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Document text containing every citation of `result_json`."""

    result_json: dict[str, Any] = attr.ib(validator=attr_validators.is_dict)
    """The result JSON, with each citation's offsets rewritten to point at its
    text within `source_document_text`."""


def ground_citations_in_fake_source_text(
    result_json: dict[str, Any]
) -> GroundedFakeResult:
    """Returns |result_json| paired with source document text that grounds it:
    the text of every citation the result carries, laid end to end after a fixed
    preamble, with each citation's reported offsets rewritten to where its text
    landed.

    Building the document from the result (rather than hand-writing a document
    and matching citations to it) keeps the pair consistent no matter which
    fields a test's result populates — a citation can never drift out of sync
    with the document that is supposed to contain it.
    """
    grounded_json = copy.deepcopy(result_json)
    document_parts = [FAKE_DOCUMENT_PREAMBLE]
    next_offset = len(FAKE_DOCUMENT_PREAMBLE)
    for citation_json in _all_citation_jsons(grounded_json):
        citation_text = citation_json[CITATION_TEXT_FIELD_NAME]
        citation_json[CITATION_START_FIELD_NAME] = next_offset
        citation_json[CITATION_END_FIELD_NAME] = next_offset + len(citation_text)
        document_parts.append(f"{citation_text}{_GROUNDED_CITATION_SEPARATOR}")
        next_offset += len(citation_text) + len(_GROUNDED_CITATION_SEPARATOR)
    return GroundedFakeResult(
        source_document_text="".join(document_parts), result_json=grounded_json
    )


def _all_citation_jsons(node: Any) -> Iterator[dict[str, Any]]:
    """Yields every citation object anywhere within |node|, in the order the JSON
    declares them.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            if key == CITATIONS_FIELD_NAME:
                yield from value
            else:
                yield from _all_citation_jsons(value)
        return
    if isinstance(node, list):
        for element in node:
            yield from _all_citation_jsons(element)
