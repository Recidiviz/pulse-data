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
"""Typed building blocks for assembling a JSON Schema document.

Each node is one schema node (an object, array, scalar, enum, or `anyOf` union);
`to_json_schema()` serializes it — and its children — to the plain nested dict
sent over the wire. Composing these typed nodes instead of nesting dict literals
keeps the structural rules (a `required` name must be a declared property, an
`anyOf` needs at least one branch) enforced in one place, and keeps callers free
of raw JSON-Schema key strings.
"""
import abc
from enum import Enum
from typing import Any, Self

import attr

from recidiviz.common import attr_validators

# A (recursively) nested JSON Schema document. The values are arbitrary JSON, so
# `Any` is unavoidable here.
JSONSchemaDict = dict[str, Any]


class JSONScalarType(Enum):
    """A JSON Schema primitive type a scalar node may take."""

    STRING = "string"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    NUMBER = "number"


@attr.define(frozen=True, kw_only=True)
class JSONSchemaNode(abc.ABC):
    """One node of a JSON Schema document."""

    @abc.abstractmethod
    def to_json_schema(self) -> JSONSchemaDict:
        """Returns this node serialized to a plain JSON Schema dict."""


@attr.define(frozen=True, kw_only=True)
class DescribedJSONSchemaNode(JSONSchemaNode):
    """A node that carries a required, human-readable description, emitted into
    its serialized output to guide the model.
    """

    description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """What this node represents, surfaced to the model in the schema."""


def _is_json_schema_node(
    _instance: object, _attribute: "attr.Attribute", value: object
) -> None:
    # `attr.validators.instance_of` can't be used for a JSONSchemaNode-typed
    # field because JSONSchemaNode is abstract (mypy `type-abstract`).
    if not isinstance(value, JSONSchemaNode):
        raise ValueError(f"Expected a JSONSchemaNode, received [{type(value)}].")


@attr.define(frozen=True, kw_only=True)
class ScalarJSONSchema(DescribedJSONSchemaNode):
    """A scalar value of a single JSON primitive type, optionally also allowing
    null.
    """

    json_type: JSONScalarType = attr.ib(validator=attr.validators.in_(JSONScalarType))
    """The primitive type of this value."""

    nullable: bool = attr.ib(default=False, validator=attr_validators.is_bool)
    """Whether null is also an allowed value, emitted as a `["<type>", "null"]`
    type union.
    """

    def to_json_schema(self) -> JSONSchemaDict:
        type_value = (
            [self.json_type.value, "null"] if self.nullable else self.json_type.value
        )
        return {"type": type_value, "description": self.description}


@attr.define(frozen=True, kw_only=True)
class ConstBooleanJSONSchema(DescribedJSONSchemaNode):
    """A boolean pinned to a single allowed value via JSON Schema `const`."""

    value: bool = attr.ib(validator=attr_validators.is_bool)
    """The exact boolean the value must equal."""

    def to_json_schema(self) -> JSONSchemaDict:
        return {
            "type": "boolean",
            "description": self.description,
            "const": self.value,
        }


@attr.define(frozen=True, kw_only=True)
class EnumJSONSchema(DescribedJSONSchemaNode):
    """A string constrained to a fixed set of allowed values."""

    values: list[str] = attr.ib(
        validator=[attr_validators.is_non_empty_list, attr_validators.is_list_of(str)]
    )
    """The allowed values."""

    def to_json_schema(self) -> JSONSchemaDict:
        return {
            "type": "string",
            "description": self.description,
            "enum": self.values,
        }

    @classmethod
    def for_enum(cls, *, enum_cls: type[Enum], description: str) -> Self:
        """Returns a node whose allowed values are the values of |enum_cls|."""
        return cls(
            description=description,
            values=[member.value for member in enum_cls],
        )


@attr.define(frozen=True, kw_only=True)
class ArrayJSONSchema(DescribedJSONSchemaNode):
    """An array whose elements all conform to a single item schema."""

    items: JSONSchemaNode = attr.ib(validator=_is_json_schema_node)
    """Schema every element of the array must conform to."""

    min_items: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_positive_int
    )
    """Minimum number of elements, or None for no minimum."""

    def to_json_schema(self) -> JSONSchemaDict:
        schema: JSONSchemaDict = {"type": "array", "description": self.description}
        if self.min_items is not None:
            schema["minItems"] = self.min_items
        schema["items"] = self.items.to_json_schema()
        return schema


@attr.define(frozen=True, kw_only=True)
class ObjectJSONSchema(DescribedJSONSchemaNode):
    """An object with a fixed, ordered set of named properties."""

    properties: dict[str, JSONSchemaNode] = attr.ib(
        validator=attr_validators.is_dict_of(str, JSONSchemaNode)
    )
    """The object's properties, keyed by name. Iteration (and therefore the
    emitted property order) follows insertion order.
    """

    required: list[str] = attr.ib(validator=attr_validators.is_list_of(str))
    """Names of the properties that must be present. Each must be a declared
    property.
    """

    def __attrs_post_init__(self) -> None:
        if unknown_required := [
            name for name in self.required if name not in self.properties
        ]:
            raise ValueError(
                f"Object schema marks {unknown_required} required, but they are "
                f"not declared properties: {sorted(self.properties)}."
            )

    def to_json_schema(self) -> JSONSchemaDict:
        return {
            "type": "object",
            "description": self.description,
            "properties": {
                name: node.to_json_schema() for name, node in self.properties.items()
            },
            "required": self.required,
        }


@attr.define(frozen=True, kw_only=True)
class AnyOfJSONSchema(JSONSchemaNode):
    """A value that must conform to at least one of several branch schemas.

    Carries no description: Vertex AI requires `anyOf` to be the only keyword on
    its schema node. Per-branch descriptions live on the branch nodes instead.
    """

    branches: list[JSONSchemaNode] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(JSONSchemaNode),
        ]
    )
    """The candidate schemas; a value is valid if it matches any one of them."""

    def to_json_schema(self) -> JSONSchemaDict:
        return {"anyOf": [branch.to_json_schema() for branch in self.branches]}
