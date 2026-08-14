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
"""One column the parsed annotations view projects out of a task's task.data, which is the
input an annotator was shown, carried back through the export unchanged.
"""
import attr
from google.cloud import bigquery

from recidiviz.common import attr_validators
from recidiviz.utils.yaml_dict import YAMLDict

# A task's payload crosses to Label Studio as JSON, so a value arrives as one of JSON's
# primitives. This maps each BigQuery type the parsed annotations view casts to onto the
# JSON types that survive that cast. INT64 deliberately excludes bool even though Python's
# bool is an int subclass, and true in JSON is not an integer to BigQuery.
_JSON_TYPES_BY_BQ_TYPE: dict[bigquery.StandardSqlTypeNames, tuple[type, ...]] = {
    bigquery.StandardSqlTypeNames.STRING: (str,),
    bigquery.StandardSqlTypeNames.INT64: (int,),
    # JSON has a single number type, so a whole number is a legitimate FLOAT64.
    bigquery.StandardSqlTypeNames.FLOAT64: (int, float),
    bigquery.StandardSqlTypeNames.BOOL: (bool,),
}


@attr.define(frozen=True, kw_only=True)
class LabelStudioTaskDataField:
    """One column extracted from task.data in the parsed annotations view."""

    column_name: str = attr.ib(validator=attr_validators.is_str)
    """Output BQ column name."""

    description: str = attr.ib(validator=attr_validators.is_str)
    """Human-readable description of this field."""

    bq_type: bigquery.StandardSqlTypeNames = attr.ib(
        validator=attr.validators.instance_of(bigquery.StandardSqlTypeNames)
    )
    """BigQuery column type."""

    extract_as_json: bool = attr.ib(validator=attr_validators.is_bool)
    """If True, uses JSON_QUERY instead of JSON_VALUE to extract this column,
    preserving the raw JSON fragment as a string. Required for fields whose
    value in task.data is a JSON object or array rather than a scalar."""

    def __attrs_post_init__(self) -> None:
        if (
            self.extract_as_json
            and self.bq_type is not bigquery.StandardSqlTypeNames.STRING
        ):
            raise ValueError(
                f"Data field [{self.column_name}] has extract_as_json=True but "
                f"bq_type [{self.bq_type.value}]. JSON objects can only be stored as STRING."
            )

    @property
    def allowed_value_types(self) -> tuple[type, ...]:
        """Returns the Python types a value of this field may be, given how the parsed
        annotations view reads it back.

        A JSON-extracted field holds a JSON object or array, which the view preserves
        whole via TO_JSON_STRING; a scalar there would come back quoted, which is why a
        container is required rather than merely permitted. Otherwise the type follows
        bq_type, defaulting to str for the BigQuery types JSON has no literal for (dates,
        timestamps, numerics), which travel as strings and are cast on the way out.
        """
        if self.extract_as_json:
            return (dict, list)
        if self.bq_type in _JSON_TYPES_BY_BQ_TYPE:
            return _JSON_TYPES_BY_BQ_TYPE[self.bq_type]
        return (str,)

    def validate_value(self, value: object) -> None:
        """Raises unless the given value is one this field can hold, meaning one the parsed
        annotations view reads back as the type it declares rather than as a NULL.

        A None always passes: every task.data column the view emits is NULLABLE, so a
        field the producer had nothing for is a legitimate empty.
        """
        if value is None:
            return
        allowed_types = self.allowed_value_types
        # Python's bool is an int subclass, so an unguarded isinstance would let True
        # through as an INT64 and write true where BigQuery expects a number.
        is_disallowed_bool = isinstance(value, bool) and bool not in allowed_types
        if is_disallowed_bool or not isinstance(value, allowed_types):
            raise ValueError(
                f"Task data field [{self.column_name}] declares bq_type "
                f"[{self.bq_type.value}]"
                f"{' with extract_as_json' if self.extract_as_json else ''}, so its "
                f"value must be one of "
                f"{sorted(t.__name__ for t in allowed_types)} or None, but found "
                f"[{value}] of type [{type(value).__name__}]."
            )

    @classmethod
    def from_yaml_dict(cls, d: YAMLDict) -> "LabelStudioTaskDataField":
        """Returns a LabelStudioTaskDataField parsed from a YAML dict."""
        column_name = d.pop("column_name", str)
        field = cls(
            column_name=column_name,
            description=d.pop("description", str),
            bq_type=bigquery.StandardSqlTypeNames(d.pop("bq_type", str)),
            extract_as_json=d.pop_optional("extract_as_json", bool) or False,
        )
        if d:
            raise ValueError(
                f"Unexpected keys in task_data_field [{column_name}]: {repr(d.get())}"
            )
        return field
