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
"""The output schema of an LLM extractor collection, parsed from the
`output_schema` block of a collection's `collection.yaml`. This is the
structure we force the model's JSON output into: the typed fields the model
must return, which of them are required, the enum values they may take, and
the semantic-consistency constraints between them. The fields themselves are
modeled in `llm_request_output_schema_field`.
"""
import attr

from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
    LLMOutputFieldType,
    LLMRequestOutputSchemaField,
    ScalarLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.utils.yaml_dict import YAMLDict


@attr.define(frozen=True, kw_only=True)
class LLMRequestOutputSchema:
    """An extractor collection's output schema, parsed from the `output_schema`
    block of its `collection.yaml` — the structure we force the model's JSON
    output into.
    """

    full_batch_description: str = attr.ib(
        validator=recidiviz_attr_validators.is_meaningful_description
    )
    """Description of the full array of extraction results."""

    result_level_description: str = attr.ib(
        validator=recidiviz_attr_validators.is_meaningful_description
    )
    """Description of a single extraction result."""

    collection_description: str = attr.ib(
        validator=recidiviz_attr_validators.is_meaningful_description
    )
    """The owning collection's description. Held here only to compose the
    auto-generated description of the framework-injected `is_relevant` field.
    """

    user_defined_fields: list[LLMRequestOutputSchemaField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(LLMRequestOutputSchemaField),
        ]
    )
    """The top-level field definitions declared in the YAML. Does not include
    the framework-injected `is_relevant` field — use `all_fields` for the
    complete set.
    """

    fields_by_name: dict[str, LLMRequestOutputSchemaField] = attr.ib(
        init=False,
        validator=attr_validators.is_dict_of(str, LLMRequestOutputSchemaField),
    )
    """The user-defined top-level field definitions, keyed by field name."""

    @fields_by_name.default
    def _build_fields_by_name(self) -> dict[str, LLMRequestOutputSchemaField]:
        fields_by_name: dict[str, LLMRequestOutputSchemaField] = {}
        for field in self.user_defined_fields:
            if field.name in fields_by_name:
                raise ValueError(
                    f"Output schema declares multiple fields named " f"[{field.name}]."
                )
            fields_by_name[field.name] = field
        return fields_by_name

    def __attrs_post_init__(self) -> None:
        if IS_RELEVANT_FIELD_NAME in self.fields_by_name:
            raise ValueError(
                f"Output schema may not define a [{IS_RELEVANT_FIELD_NAME}] "
                f"field — it is auto-injected by the framework."
            )

    @property
    def is_relevant_field(self) -> ScalarLLMRequestOutputSchemaField:
        """Returns the framework-injected `is_relevant` field: a bare
        (STRUCTURAL) boolean, always required, whose description is composed
        from the collection description.
        """
        return ScalarLLMRequestOutputSchemaField(
            name=IS_RELEVANT_FIELD_NAME,
            description=(
                f"Whether this document contains information relevant to: "
                f"{self.collection_description}"
            ),
            required=True,
            inferred_field_config=None,
            scalar_type=LLMOutputFieldType.BOOLEAN,
        )

    @property
    def all_fields(self) -> list[LLMRequestOutputSchemaField]:
        """Returns every output field, including the framework-injected
        `is_relevant` field first, followed by the user-defined fields.
        """
        return [self.is_relevant_field, *self.user_defined_fields]

    def get_field(self, field_name: str) -> LLMRequestOutputSchemaField:
        """Returns the user-defined top-level field named |field_name|, raising
        if the schema declares no such field.
        """
        if field_name not in self.fields_by_name:
            raise ValueError(
                f"Output schema has no top-level field named [{field_name}]. "
                f"Declared fields: {sorted(self.fields_by_name)}."
            )
        return self.fields_by_name[field_name]

    @classmethod
    def from_yaml_dict(
        cls,
        *,
        yaml_dict: YAMLDict,
        collection_description: str,
        default_minimum_confidence_level: ConfidenceLevel,
    ) -> "LLMRequestOutputSchema":
        """Returns the output schema parsed from a collection's `output_schema`
        block. |collection_description| is used to compose the injected
        `is_relevant` field's description; each field's effective minimum
        confidence level resolves against |default_minimum_confidence_level|.
        """
        schema = cls(
            full_batch_description=yaml_dict.pop("full_batch_description", str),
            result_level_description=yaml_dict.pop("result_level_description", str),
            collection_description=collection_description,
            user_defined_fields=LLMRequestOutputSchemaField.build_output_schema_fields(
                field_yamls=yaml_dict.pop_dicts("inferred_fields"),
                default_minimum_confidence_level=default_minimum_confidence_level,
            ),
        )
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values in output_schema block: "
                f"{repr(yaml_dict.get())}"
            )
        return schema
