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
"""One column the parsed annotations view projects out of a Label Studio annotation
result — an answer the annotator gave, as opposed to the input they were shown.
"""
import attr
from google.cloud import bigquery

from recidiviz.common import attr_validators
from recidiviz.llm_eval.label_studio.models.label_studio_field_transform import (
    LabelStudioFieldTransform,
)
from recidiviz.utils.yaml_dict import YAMLDict

SUPPORTED_LS_TYPES = frozenset({"choices", "labels", "textarea", "rating", "number"})


@attr.define(frozen=True, kw_only=True)
class LabelStudioAnnotationField:
    """One column extracted from annotation result JSON in the parsed annotations view."""

    column_name: str = attr.ib(validator=attr_validators.is_str)
    """Output BQ column name."""

    description: str = attr.ib(validator=attr_validators.is_str)
    """Human-readable description of this annotation field."""

    ls_type: str = attr.ib(validator=attr_validators.is_str)
    """Matches the 'type' field in Label Studio result JSON."""

    bq_type: bigquery.StandardSqlTypeNames = attr.ib(
        validator=attr.validators.instance_of(bigquery.StandardSqlTypeNames)
    )
    """BigQuery column type."""

    transform: LabelStudioFieldTransform = attr.ib(
        validator=attr.validators.instance_of(LabelStudioFieldTransform)
    )
    """How to extract the value from the LS result object."""

    irr_included: bool = attr.ib(validator=attr_validators.is_bool)
    """If True, included in IRR computation. Defaults to False."""

    value_map: dict[str, str] | None = attr.ib(validator=attr_validators.is_opt(dict))
    """Optional mapping from raw LS choice strings to internal values.
    Applied after the transform. Useful when LS choice labels contain
    descriptive text (e.g. 'GOOD — Accurate, only minor errors' → 'GOOD').
    Unrecognized values pass through unchanged."""

    ordinal_values: list[str] | None = attr.ib(validator=attr_validators.is_opt_list)
    """Ordered category values (worst-to-best) for linear weighted kappa.
    Only valid on STRING fields with irr_included=True. When set, Cohen's
    kappa in the IRR view is computed as linear weighted kappa using these
    ranks — partial disagreements (e.g. GOOD vs PARTIAL) penalise less than
    full disagreements (e.g. GOOD vs BAD). Values must be the post-value_map
    normalised strings (e.g. ['BAD', 'PARTIAL', 'GOOD'])."""

    def __attrs_post_init__(self) -> None:
        if self.ls_type not in SUPPORTED_LS_TYPES:
            raise ValueError(
                f"Unsupported ls_type [{self.ls_type}] for field "
                f"[{self.column_name}]. Supported: {SUPPORTED_LS_TYPES}"
            )
        if (
            self.ordinal_values is not None
            and self.bq_type is not bigquery.StandardSqlTypeNames.STRING
        ):
            raise ValueError(
                f"ordinal_values is only valid for STRING fields, "
                f"but field [{self.column_name}] has bq_type [{self.bq_type.value}]"
            )

    @classmethod
    def from_yaml_dict(cls, d: YAMLDict) -> "LabelStudioAnnotationField":
        """Returns a LabelStudioAnnotationField parsed from a YAML dict."""
        column_name = d.pop("column_name", str)
        raw_transform = d.pop("transform", str)
        try:
            transform = LabelStudioFieldTransform(raw_transform)
        except ValueError as e:
            raise ValueError(
                f"Unknown transform for field [{column_name}]: [{raw_transform}]"
            ) from e
        value_map_raw = d.pop_optional("value_map", dict)
        value_map: dict[str, str] | None = (
            {str(k): str(v) for k, v in value_map_raw.items()}
            if value_map_raw is not None
            else None
        )
        ordinal_values_raw = d.pop_optional("ordinal_values", list)
        ordinal_values: list[str] | None = (
            [str(v) for v in ordinal_values_raw]
            if ordinal_values_raw is not None
            else None
        )
        field = cls(
            column_name=column_name,
            description=d.pop("description", str),
            ls_type=d.pop("ls_type", str),
            bq_type=bigquery.StandardSqlTypeNames(d.pop("bq_type", str)),
            transform=transform,
            irr_included=d.pop_optional("irr_included", bool) or False,
            value_map=value_map,
            ordinal_values=ordinal_values,
        )
        if d:
            raise ValueError(
                f"Unexpected keys in schema field [{column_name}]: {repr(d.get())}"
            )
        return field
