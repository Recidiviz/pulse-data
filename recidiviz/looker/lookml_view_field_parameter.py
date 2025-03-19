# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Creates LookMLViewFieldParameter object and associated functions"""
import abc
import re
from enum import Enum
from typing import List, Tuple, Union

import attr

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.looker.parameterized_value import ParameterizedValue

TABLE_FIELD_PATTERN = r"\$\{TABLE\}\.([a-zA-Z0-9_]+)"
VIEW_FIELD_PATTERN = r"\$\{([a-zA-Z0-9_]+)\}[^.]"


class LookMLFieldCategory(Enum):
    DIMENSION = "dimension"
    DIMENSION_GROUP = "dimension_group"
    MEASURE = "measure"
    FILTER = "filter"
    PARAMETER = "parameter"


class LookMLFieldType(Enum):
    COUNT = "count"
    DATE = "date"
    DURATION = "duration"
    MAX = "max"
    NUMBER = "number"
    STRING = "string"
    SUM_DISTINCT = "sum_distinct"
    TIME = "time"
    UNQUOTED = "unquoted"
    YESNO = "yesno"
    AVERAGE = "average"
    SUM = "sum"
    LIST = "list"


class LookMLFieldDatatype(Enum):
    EPOCH = "epoch"
    TIMESTAMP = "timestamp"
    DATETIME = "datetime"
    DATE = "date"
    YYYYMMDD = "yyyymmdd"


class LookMLSqlReferenceType(Enum):
    TABLE_COLUMN = "table_column"
    DIMENSION = "dimension"


class LookMLTimeframesOption(Enum):
    DATE = "date"
    MONTH = "month"
    QUARTER = "quarter"
    RAW = "raw"
    TIME = "time"
    WEEK = "week"
    YEAR = "year"


@attr.define
class LookMLFieldParameter:
    """Defines a LookML field parameter, including the parameter key
    and value string and attributes specific to the subclass"""

    @property
    @abc.abstractmethod
    def key(self) -> str:
        pass

    @abc.abstractmethod
    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        pass

    @property
    @abc.abstractmethod
    def value_text(self) -> str:
        pass

    def build(self) -> str:
        return f"{self.key}: {self.value_text}"

    # TODO(#23292) Add property to return referenced fields that belong to another view

    @property
    @abc.abstractmethod
    def referenced_table_fields(self) -> set[str] | None:
        """Returns a set of BQ table fields referenced by the parameter"""

    @property
    @abc.abstractmethod
    def referenced_view_fields(self) -> set[str] | None:
        """Returns a set of view fields referenced by the parameter"""

    @classmethod
    def description(cls, description: str) -> "LookMLFieldParameter":
        return FieldParameterDescription(description)

    @classmethod
    def group_label(cls, group_label: str) -> "LookMLFieldParameter":
        return FieldParameterGroupLabel(group_label)

    @classmethod
    def view_label(cls, view_label: str) -> "LookMLFieldParameter":
        return FieldParameterViewLabel(view_label)

    @classmethod
    def label(cls, label: str) -> "LookMLFieldParameter":
        return FieldParameterLabel(label)

    @classmethod
    def allowed_value(cls, label: str, value: str) -> "LookMLFieldParameter":
        return FieldParameterAllowedValue(label, value)

    @classmethod
    def full_suggestions(cls, value: bool) -> "LookMLFieldParameter":
        return FieldParameterFullSuggestions(value)

    @classmethod
    def convert_tz(cls, value: bool) -> "LookMLFieldParameter":
        return FieldParameterConvertTz(value)

    @classmethod
    def default_value(cls, value: str) -> "LookMLFieldParameter":
        return FieldParameterDefaultValue(value)

    @classmethod
    def drill_fields(cls, fields: List[str]) -> "LookMLFieldParameter":
        return FieldParameterDrillFields(fields)

    @classmethod
    def datatype(cls, datatype: LookMLFieldDatatype) -> "LookMLFieldParameter":
        return FieldParameterDatatype(datatype)

    @classmethod
    def hidden(cls, is_hidden: bool) -> "LookMLFieldParameter":
        return FieldParameterHidden(is_hidden)

    @classmethod
    def html(cls, html_value: str) -> "LookMLFieldParameter":
        return FieldParameterHtml(html_value)

    @classmethod
    def precision(cls, precision: int) -> "LookMLFieldParameter":
        return FieldParameterPrecision(precision)

    @classmethod
    def primary_key(cls, is_primary_key: bool) -> "LookMLFieldParameter":
        return FieldParameterPrimaryKey(is_primary_key)

    @classmethod
    def suggestions(cls, suggestions: List[str]) -> "LookMLFieldParameter":
        return FieldParameterSuggestions(suggestions)

    @classmethod
    def sql(cls, sql: Union[str, ParameterizedValue]) -> "LookMLFieldParameter":
        return FieldParameterSql(sql)

    @classmethod
    def sql_distinct_key(
        cls, sql_distinct_key: Union[str, ParameterizedValue]
    ) -> "LookMLFieldParameter":
        return FieldParameterSqlDistinctKey(sql_distinct_key)

    @classmethod
    def type(cls, type_param: LookMLFieldType) -> "LookMLFieldParameter":
        return FieldParameterType(type_param)

    @classmethod
    def timeframes(
        cls, options: List[LookMLTimeframesOption]
    ) -> "LookMLFieldParameter":
        return FieldParameterTimeframes(options)

    @classmethod
    def value_format(cls, value: str) -> "LookMLFieldParameter":
        return FieldParameterValueFormat(value)

    @classmethod
    def filters(cls, filters: List[Tuple[str, str]]) -> "LookMLFieldParameter":
        return FieldParameterFilters(filters)

    @classmethod
    def list_field(cls, list_field: str) -> "LookMLFieldParameter":
        return FieldParameterListField(list_field)


# DISPLAY PARAMETERS
@attr.define
class FieldParameterDescription(LookMLFieldParameter):
    """Generates a `description` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-description).
    """

    text: str

    @property
    def key(self) -> str:
        return "description"

    @property
    def value_text(self) -> str:
        escaped_text = self.text.replace('"', '\\"')
        return f'"{escaped_text}"'

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return True

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterGroupLabel(LookMLFieldParameter):
    """Generates a `group_label` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-group-label).
    """

    text: str

    @property
    def key(self) -> str:
        return "group_label"

    @property
    def value_text(self) -> str:
        return f'"{self.text}"'

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return True

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterViewLabel(LookMLFieldParameter):
    """Generates a `view_label` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-view-label).
    """

    text: str

    @property
    def key(self) -> str:
        return "view_label"

    @property
    def value_text(self) -> str:
        return f'"{self.text}"'

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return True

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterLabel(LookMLFieldParameter):
    """Generates a `label` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-label).
    """

    text: str

    @property
    def key(self) -> str:
        return "label"

    @property
    def value_text(self) -> str:
        return f'"{self.text}"'

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return True

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


# FILTER SUGGESTION PARAMETERS
@attr.define
class FieldParameterAllowedValue(LookMLFieldParameter):
    """Generates a `allowed_value` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-parameter#specifying_allowed_values).
    """

    label_param: str
    value_param: str

    @property
    def key(self) -> str:
        return "allowed_value"

    @property
    def value_text(self) -> str:
        raise ValueError(
            "The value_text property should not be used for the allowed_value field "
            "parameter - this parameter has a custom implementation of build()."
        )

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category == LookMLFieldCategory.PARAMETER

    def build(self) -> str:
        return f"""allowed_value: {{
      label: "{self.label_param}"
      value: "{self.value_param}"
    }}"""

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterDefaultValue(LookMLFieldParameter):
    """Generates a `default_value` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-parameter#specifying_allowed_values).
    """

    text: str

    @property
    def key(self) -> str:
        return "default_value"

    @property
    def value_text(self) -> str:
        return f'"{self.text}"'

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category in (
            LookMLFieldCategory.PARAMETER,
            LookMLFieldCategory.FILTER,
        )

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterFullSuggestions(LookMLFieldParameter):
    """
    Generates a `full_suggestions` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-full-suggestions)
    """

    value: bool

    @property
    def key(self) -> str:
        return "full_suggestions"

    @property
    def value_text(self) -> str:
        return "yes" if self.value else "no"

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category in (
            LookMLFieldCategory.DIMENSION,
            LookMLFieldCategory.DIMENSION_GROUP,
            LookMLFieldCategory.FILTER,
            LookMLFieldCategory.PARAMETER,
        )

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


# QUERY PARAMETERS
@attr.define
class FieldParameterConvertTz(LookMLFieldParameter):
    """Generates a `convert_tz` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-convert-tz).
    """

    value: bool

    @property
    def key(self) -> str:
        return "convert_tz"

    @property
    def value_text(self) -> str:
        return "yes" if self.value else "no"

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return True

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterDatatype(LookMLFieldParameter):
    """Generates a `datatype` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-datatype).
    """

    field_datatype: LookMLFieldDatatype

    @property
    def key(self) -> str:
        return "datatype"

    @property
    def value_text(self) -> str:
        return self.field_datatype.value

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category != LookMLFieldCategory.PARAMETER

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterHtml(LookMLFieldParameter):
    """Generates a `html` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-html).
    """

    html_value: str

    @property
    def key(self) -> str:
        return "html"

    @property
    def value_text(self) -> str:
        return f"{self.html_value} ;;"

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category in (
            LookMLFieldCategory.DIMENSION,
            LookMLFieldCategory.DIMENSION_GROUP,
            LookMLFieldCategory.MEASURE,
        )

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterPrecision(LookMLFieldParameter):
    """Generates a `precision` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-precision).
    """

    value: int

    @property
    def key(self) -> str:
        return "precision"

    @property
    def value_text(self) -> str:
        return str(self.value)

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category == LookMLFieldCategory.MEASURE

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


# VALUE AND FORMATTING PARAMETERS
@attr.define
class FieldParameterHidden(LookMLFieldParameter):
    """Generates a `hidden` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-hidden).
    """

    is_hidden: bool

    @property
    def key(self) -> str:
        return "hidden"

    @property
    def value_text(self) -> str:
        return "yes" if self.is_hidden else "no"

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return True

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterValueFormat(LookMLFieldParameter):
    """Generates a `value_format` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-value-format).
    """

    value: str

    @property
    def key(self) -> str:
        return "value_format"

    @property
    def value_text(self) -> str:
        return f'"{self.value}"'

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category != LookMLFieldCategory.PARAMETER

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterSuggestions(LookMLFieldParameter):
    """Generates a `suggestions` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-suggestions).
    """

    suggestion_columns: List[str]

    @property
    def key(self) -> str:
        return "suggestions"

    @property
    def value_text(self) -> str:
        return f"[{list_to_query_string(self.suggestion_columns, quoted=True)}]"

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category in (
            LookMLFieldCategory.DIMENSION,
            LookMLFieldCategory.FILTER,
            LookMLFieldCategory.PARAMETER,
        )

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterSql(LookMLFieldParameter):
    """Generates a `sql` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-sql).
    """

    sql_text: Union[str, ParameterizedValue]

    @property
    def key(self) -> str:
        return "sql"

    @property
    def value_text(self) -> str:
        if isinstance(self.sql_text, ParameterizedValue):
            return f"{self.sql_text.build_liquid_template()} ;;"
        return f"{self.sql_text} ;;"

    @property
    def referenced_table_fields(self) -> set[str]:
        """Returns a set of BQ table fields of the form `${TABLE}.field_name` referenced by the parameter"""
        fields = re.findall(TABLE_FIELD_PATTERN, self.value_text)

        return set(fields)

    @property
    def referenced_view_fields(self) -> set[str]:
        """Returns a set of view fields of the form `${field_name}` referenced by the parameter"""
        fields = re.findall(VIEW_FIELD_PATTERN, self.value_text)

        return set(fields)

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category != LookMLFieldCategory.PARAMETER


@attr.define
class FieldParameterSqlDistinctKey(FieldParameterSql):
    """Generates a `sql_distinct_key` field parameter
    (see https://cloud.google.com/looker/docs/reference/param-field-sql-distinct-key)"""

    @property
    def key(self) -> str:
        return "sql_distinct_key"

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category == LookMLFieldCategory.MEASURE


@attr.define
class FieldParameterType(LookMLFieldParameter):
    """Generates a `type` field parameter. The allowed types varies based on the field
    category (see https://cloud.google.com/looker/docs/reference/param-dimension-filter-parameter-types,
    https://cloud.google.com/looker/docs/reference/param-field-dimension-group#types,
    and https://cloud.google.com/looker/docs/reference/param-measure-types).
    """

    field_type: LookMLFieldType

    @property
    def key(self) -> str:
        return "type"

    @property
    def value_text(self) -> str:
        return self.field_type.value

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        """Determines types compatible with category based on
        https://cloud.google.com/looker/docs/reference/param-dimension-filter-parameter-types#type_definitions
        """
        if field_category == LookMLFieldCategory.DIMENSION_GROUP:
            return self.field_type in (LookMLFieldType.TIME, LookMLFieldType.DURATION)
        if field_category in (
            LookMLFieldCategory.DIMENSION,
            LookMLFieldCategory.FILTER,
        ):
            return self.field_type in (
                LookMLFieldType.DATE,
                LookMLFieldType.NUMBER,
                LookMLFieldType.STRING,
                LookMLFieldType.YESNO,
            )
        if field_category == LookMLFieldCategory.MEASURE:
            return self.field_type in (
                LookMLFieldType.AVERAGE,
                LookMLFieldType.COUNT,
                LookMLFieldType.DATE,
                LookMLFieldType.LIST,
                LookMLFieldType.MAX,
                LookMLFieldType.NUMBER,
                LookMLFieldType.STRING,
                LookMLFieldType.SUM,
                LookMLFieldType.SUM_DISTINCT,
            )
        if field_category == LookMLFieldCategory.PARAMETER:
            return self.field_type in (
                LookMLFieldType.DATE,
                LookMLFieldType.NUMBER,
                LookMLFieldType.STRING,
                LookMLFieldType.UNQUOTED,
                LookMLFieldType.YESNO,
            )
        return False

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


# OTHER PARAMETERS
@attr.define
class FieldParameterTimeframes(LookMLFieldParameter):
    """Generates a `timeframes` field parameter,
    to be used with a `dimension_group` field of type `time`
    https://cloud.google.com/looker/docs/reference/param-field-dimension-group#timeframes
    """

    timeframe_options: List[LookMLTimeframesOption] = attr.field(
        validator=attr.validators.min_len(1)
    )

    @property
    def key(self) -> str:
        return "timeframes"

    @property
    def value_text(self) -> str:
        str_options = [t.value for t in self.timeframe_options]
        multiline_options_str = ",\n      ".join(str_options)
        return f"[\n      {multiline_options_str}\n    ]"

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category == LookMLFieldCategory.DIMENSION_GROUP

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterPrimaryKey(LookMLFieldParameter):
    """Generates a `primary_key` field parameter,
    https://cloud.google.com/looker/docs/reference/param-field-primary-key
    """

    is_primary_key: bool

    @property
    def key(self) -> str:
        return "primary_key"

    @property
    def value_text(self) -> str:
        return "yes" if self.is_primary_key else "no"

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category == LookMLFieldCategory.DIMENSION

    @property
    def referenced_view_fields(self) -> None:
        return None

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterDrillFields(LookMLFieldParameter):
    """Generates a `drill_fields` field parameter,
    https://cloud.google.com/looker/docs/reference/param-view-drill-fields
    """

    fields: List[str]

    @property
    def key(self) -> str:
        return "drill_fields"

    @property
    def value_text(self) -> str:
        return "[" + ", ".join(self.fields) + "]"

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category in (
            LookMLFieldCategory.DIMENSION,
            LookMLFieldCategory.MEASURE,
        )

    @property
    def referenced_view_fields(self) -> set[str]:
        return set(self.fields)

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterFilters(LookMLFieldParameter):
    """Generates a `filters` field parameter,
    https://cloud.google.com/looker/docs/reference/param-field-filters
    """

    dimension_and_filter_tuples: List[Tuple[str, str]]

    @property
    def key(self) -> str:
        return "filters"

    @property
    def value_text(self) -> str:
        return (
            "["
            + ", ".join(f'{f[0]}: "{f[1]}"' for f in self.dimension_and_filter_tuples)
            + "]"
        )

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category == LookMLFieldCategory.MEASURE

    @property
    def referenced_view_fields(self) -> set[str]:
        return {f[0] for f in self.dimension_and_filter_tuples}

    @property
    def referenced_table_fields(self) -> None:
        return None


@attr.define
class FieldParameterListField(LookMLFieldParameter):
    """Generates a `list` field parameter,
    https://cloud.google.com/looker/docs/reference/param-measure-types#list
    """

    list_field_name: str

    @property
    def key(self) -> str:
        return "list_field"

    @property
    def value_text(self) -> str:
        return self.list_field_name

    def allowed_for_category(self, field_category: LookMLFieldCategory) -> bool:
        return field_category == LookMLFieldCategory.MEASURE

    @property
    def referenced_view_fields(self) -> set[str]:
        return {self.list_field_name}

    @property
    def referenced_table_fields(self) -> None:
        return None
