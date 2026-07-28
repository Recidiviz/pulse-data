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
"""Validates a JSON instance against a JSON Schema, resolving `anyOf` / `oneOf`
failures down to precise leaf errors.

When an instance fails a schema union, `jsonschema` reports one opaque error at
the union's location ("... is not valid under any of the given schemas") whose
`absolute_path` points at the union as a whole, not at the offending value. The
per-branch detail is not lost, though: it hangs off the opaque error's
`context`, one sub-error per way each branch failed, each with a correct
`absolute_path` — recursively, through nested unions. Flattening therefore
needs no knowledge of the schema's structure, just a way to decide at each
union which branch the instance was *intended* to match, so that only that
branch's failures are reported.

The intended branch is picked with two generic signals, in order:

  - A branch failing on a `const` keyword is penalized: a `const` mismatch
    means the instance explicitly declared itself as a different branch of a
    discriminated union (e.g. a boolean `const` discriminator pinned to a
    different value per branch).
  - Ties break toward the branch whose declared `properties` overlap the most
    keys of the failing instance: an object carrying a `value` key intends the
    branch that declares `value`, not a sibling branch that declares
    `null_reason` instead.

Remaining ties resolve to the earliest branch. For a truly malformed instance
that matches no branch's discriminators, the choice is arbitrary but the
reported errors still carry exact paths, which beats the opaque union error.
"""
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

import jsonschema
from jsonschema.exceptions import ValidationError
from referencing import Registry, Resource

from recidiviz.utils.types import assert_type


def iter_leaf_validation_errors(
    json_dict: dict[str, Any], json_schema: dict[str, Any]
) -> Iterator[ValidationError]:
    """Yields one precise leaf error per way |json_dict| fails to conform to
    |json_schema|, or nothing when it conforms. A non-union error is itself the
    leaf; a union (`anyOf`/`oneOf`) error resolves to the leaf errors of the
    branch the instance was most plausibly intended to match (see the module
    docstring for how that branch is chosen).
    """
    # A registry only resolves external `$ref`s, which self-contained schemas do not
    # carry.
    def no_external_refs(uri: str) -> Resource:
        raise ValueError(f"Unexpected external schema reference [{uri}].")

    json_validator = jsonschema.Draft202012Validator(
        schema=json_schema, registry=Registry(retrieve=no_external_refs)
    )
    # jsonschema's type stubs omit `iter_errors` from the base Validator class,
    # though it is present at runtime.
    for error in json_validator.iter_errors(json_dict):  # type: ignore[attr-defined]
        yield from _iter_leaf_errors(error)


def _iter_leaf_errors(error: ValidationError) -> Iterator[ValidationError]:
    """Yields the precise leaf errors underneath |error|, recursing through the
    per-branch sub-errors of each union error.
    """
    if not error.context:
        yield error
        return

    errors_by_branch: dict[int, list[ValidationError]] = defaultdict(list)
    for branch_error in error.context:
        # A union sub-error's schema path starts with the index of the branch
        # it came from.
        errors_by_branch[assert_type(branch_error.schema_path[0], int)].append(
            branch_error
        )

    branch_schemas = assert_type(error.schema, dict)[error.validator]
    intended_branch = min(
        sorted(errors_by_branch),
        key=lambda branch: _branch_mismatch_score(
            branch_errors=errors_by_branch[branch],
            branch_schema=branch_schemas[branch],
            instance=error.instance,
        ),
    )
    for branch_error in errors_by_branch[intended_branch]:
        yield from _iter_leaf_errors(branch_error)


def _branch_mismatch_score(
    *,
    branch_errors: list[ValidationError],
    branch_schema: object,
    instance: object,
) -> tuple[int, int]:
    """Returns a sortable score of how implausible it is that |instance| was
    intended to match the union branch that failed with |branch_errors| — lower
    is more plausible. The first component counts `const` mismatches
    (discriminator failures); the second negates the number of instance keys the
    branch declares as properties.
    """
    const_mismatches = sum(
        1 for branch_error in branch_errors if branch_error.validator == "const"
    )
    property_overlap = 0
    if isinstance(instance, dict) and isinstance(branch_schema, dict):
        # A branch schema has no `properties` when it does not describe an
        # object with fixed keys.
        property_overlap = len(
            instance.keys() & branch_schema.get("properties", {}).keys()
        )
    return (const_mismatches, -property_overlap)
