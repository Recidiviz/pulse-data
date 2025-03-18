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
"""Creates LookMLViewField object and associated functions"""
import abc
from typing import List, Optional, TypeVar

import attr
from more_itertools import one

from recidiviz.looker.lookml_view_field_parameter import (
    FieldParameterAllowedValue,
    FieldParameterDatatype,
    FieldParameterTimeframes,
    FieldParameterType,
    FieldParameterViewLabel,
    LookMLFieldCategory,
    LookMLFieldDatatype,
    LookMLFieldParameter,
    LookMLFieldType,
    LookMLTimeframesOption,
)
from recidiviz.utils.string import StrictStringFormatter

FIELD_TEMPLATE = """
  {field_type}: {field_name} {{
    {parameter_declarations}
  }}"""

LookMLViewFieldT = TypeVar("LookMLViewFieldT", bound="LookMLViewField")

PRIMARY_KEY_COLUMN_NAME = "primary_key"


def enforce_has_datatype_if_date_field(
    field_parameters: List[LookMLFieldParameter],
) -> None:
    """Enforce that datatype exists when type is date"""
    if any(
        isinstance(param, FieldParameterType)
        and param.field_type is LookMLFieldType.DATE
        for param in field_parameters
    ) and not any(
        isinstance(param, FieldParameterDatatype) for param in field_parameters
    ):
        raise ValueError(
            "Datatype parameter must be supplied when type parameter is `date`."
        )


@attr.define
class LookMLViewField:
    """Produces a LookML view field clause that satisfies the syntax described in
    https://cloud.google.com/looker/docs/reference/param-field. Not all field syntax
    is supported.
    Although not required by Looker, consider providing a `description` and `group_label` parameter where possible.
    """

    field_name: str
    parameters: List[LookMLFieldParameter]

    @property
    @abc.abstractmethod
    def field_category(self) -> LookMLFieldCategory:
        pass

    def __attrs_post_init__(self) -> None:
        disallowed_parameters = sorted(
            {
                p.key
                for p in self.parameters
                if not p.allowed_for_category(self.field_category)
            }
        )

        if disallowed_parameters:
            raise ValueError(
                f"The following parameter types are not allowed for "
                f"[{self.field_category.value}] fields: {disallowed_parameters}"
            )

        # Enforce that there aren't multiple parameters with the same key (except allowed_values)
        single_value_keys = [
            param.key
            for param in self.parameters
            if not isinstance(param, FieldParameterAllowedValue)
        ]

        if len(single_value_keys) != len(set(single_value_keys)):
            raise ValueError(
                f"Defined field parameters contain repeated key: {single_value_keys}"
            )

    def build(self) -> str:
        parameter_declarations = "\n    ".join(
            [param.build() for param in self.parameters]
        )
        return StrictStringFormatter().format(
            FIELD_TEMPLATE,
            field_type=self.field_category.value,
            field_name=self.field_name,
            parameter_declarations=parameter_declarations,
        )

    def extend(
        self: LookMLViewFieldT, additional_parameters: List[LookMLFieldParameter]
    ) -> LookMLViewFieldT:
        """Returns a new view field of the same type with additional parameters added"""
        return self.__class__(self.field_name, self.parameters + additional_parameters)

    def view_label(self) -> FieldParameterViewLabel:
        return one(p for p in self.parameters if isinstance(p, FieldParameterViewLabel))

    @property
    def referenced_table_fields(self) -> set[str]:
        return {
            field
            for p in self.parameters
            if p.referenced_table_fields is not None
            for field in p.referenced_table_fields
        }

    @property
    def referenced_view_fields(self) -> set[str]:
        return {
            field
            for p in self.parameters
            if p.referenced_view_fields is not None
            for field in p.referenced_view_fields
        }


@attr.define
class DimensionLookMLViewField(LookMLViewField):
    """Defines a LookML dimension field object."""

    field_category = LookMLFieldCategory.DIMENSION

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        # Enforce that datatype exists when type is date
        enforce_has_datatype_if_date_field(self.parameters)

    @classmethod
    def for_column(
        cls,
        column_name: str,
        field_type: LookMLFieldType = LookMLFieldType.STRING,
        custom_params: Optional[List[LookMLFieldParameter]] = None,
    ) -> "DimensionLookMLViewField":
        """
        Generates simple dimension referencing a column name present in the view source table.
        """
        additional_params: List[LookMLFieldParameter] = custom_params or []
        if field_type is LookMLFieldType.DATE:
            additional_params.append(
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE)
            )
        if field_type is LookMLFieldType.NUMBER:
            additional_params.append(LookMLFieldParameter.value_format("0"))

        return DimensionLookMLViewField(
            field_name=column_name,
            parameters=[
                LookMLFieldParameter.type(field_type),
                *additional_params,
                LookMLFieldParameter.sql(f"${{TABLE}}.{column_name}"),
            ],
        )

    @classmethod
    def for_days_in_period(
        cls,
        start_date_column_name: str,
        end_date_column_name: str,
        view_label: Optional[str] = None,
    ) -> "DimensionLookMLViewField":
        """Generates a dimension calculating the difference in days between two date columns"""
        additional_params: List[LookMLFieldParameter] = []
        if view_label is not None:
            additional_params.append(LookMLFieldParameter.view_label(view_label))

        return DimensionLookMLViewField(
            field_name="days_in_period",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                *additional_params,
                LookMLFieldParameter.sql(
                    f"DATE_DIFF(${{TABLE}}.{end_date_column_name}, ${{TABLE}}.{start_date_column_name}, DAY)"
                ),
            ],
        )

    @classmethod
    def for_compound_primary_key(
        cls, field_1: str, field_2: str
    ) -> "DimensionLookMLViewField":
        """Generates a dimension that concatenates two fields to create a compound primary key"""
        return cls(
            field_name=PRIMARY_KEY_COLUMN_NAME,
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.primary_key(True),
                LookMLFieldParameter.sql(
                    f'CONCAT(${{TABLE}}.{field_1}, "_", ${{TABLE}}.{field_2})'
                ),
            ],
        )


@attr.define
class FilterLookMLViewField(LookMLViewField):
    field_category = LookMLFieldCategory.FILTER

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        # Enforce that datatype exists when type is date
        enforce_has_datatype_if_date_field(self.parameters)


@attr.define
class MeasureLookMLViewField(LookMLViewField):
    field_category = LookMLFieldCategory.MEASURE

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        # Enforce that datatype exists when type is date
        enforce_has_datatype_if_date_field(self.parameters)


@attr.define
class ParameterLookMLViewField(LookMLViewField):
    field_category = LookMLFieldCategory.PARAMETER

    def allowed_values(self) -> List[FieldParameterAllowedValue]:
        return [p for p in self.parameters if isinstance(p, FieldParameterAllowedValue)]


@attr.define
class DimensionGroupLookMLViewField(LookMLViewField):
    """Defines a LookML dimension group field object.

    A dimension group is used to create a set of individual dimensions for different intervals or timeframes.
    https://cloud.google.com/looker/docs/reference/param-field-dimension-group
    """

    field_category = LookMLFieldCategory.DIMENSION_GROUP

    @property
    @abc.abstractmethod
    def dimension_names(self) -> List[str]:
        """Returns a list of dimension names created by the dimension group"""


@attr.define
class TimeDimensionGroupLookMLViewField(DimensionGroupLookMLViewField):
    """Defines a LookML dimension group used to create a set of time-based dimensions."""

    field_category = LookMLFieldCategory.DIMENSION_GROUP

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        type_param = one(
            param for param in self.parameters if isinstance(param, FieldParameterType)
        )
        if type_param.field_type != LookMLFieldType.TIME:
            raise ValueError(
                f"Type parameter must be `time`, got [{type_param.field_type}]."
            )
        # Enforce that datatype exists when type is date
        enforce_has_datatype_if_date_field(self.parameters)

    @classmethod
    def for_column(
        cls,
        column_name: str,
        datatype: LookMLFieldDatatype,
        timeframe_options: List[LookMLTimeframesOption],
        custom_params: List[LookMLFieldParameter] | None = None,
    ) -> "TimeDimensionGroupLookMLViewField":
        """
        Generates a dimension group for a column.
        Looker automatically appends each timeframe to the end of the field name,
        so we need to strip out any suffix that matches one of the timeframe options
        to get the base field name. This isn't strictly necessary but is best practice.
        """

        def _extract_base_field_name(column_name: str) -> str:
            for option in timeframe_options:
                suffix = f"_{option.value.lower()}"
                if column_name.endswith(suffix):
                    return column_name[: -len(suffix)]
            return column_name

        return cls(
            field_name=_extract_base_field_name(column_name),
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.TIME),
                LookMLFieldParameter.timeframes(timeframe_options),
                LookMLFieldParameter.convert_tz(False),
                LookMLFieldParameter.datatype(datatype),
                LookMLFieldParameter.sql(f"${{TABLE}}.{column_name}"),
            ]
            + (custom_params or []),
        )

    @classmethod
    def for_datetime_column(
        cls,
        column_name: str,
        custom_params: List[LookMLFieldParameter] | None = None,
    ) -> "TimeDimensionGroupLookMLViewField":
        """
        Generates a dimension group for a datetime column.
        """
        return cls.for_column(
            column_name=column_name,
            datatype=LookMLFieldDatatype.DATETIME,
            timeframe_options=[
                LookMLTimeframesOption.RAW,
                LookMLTimeframesOption.TIME,
                LookMLTimeframesOption.DATE,
                LookMLTimeframesOption.WEEK,
                LookMLTimeframesOption.MONTH,
                LookMLTimeframesOption.QUARTER,
                LookMLTimeframesOption.YEAR,
            ],
            custom_params=custom_params,
        )

    @classmethod
    def for_date_column(
        cls,
        column_name: str,
        custom_params: List[LookMLFieldParameter] | None = None,
    ) -> "TimeDimensionGroupLookMLViewField":
        """
        Generates a dimension group for a date column.
        """
        return cls.for_column(
            column_name=column_name,
            datatype=LookMLFieldDatatype.DATE,
            timeframe_options=[
                LookMLTimeframesOption.RAW,
                LookMLTimeframesOption.DATE,
                LookMLTimeframesOption.WEEK,
                LookMLTimeframesOption.MONTH,
                LookMLTimeframesOption.QUARTER,
                LookMLTimeframesOption.YEAR,
            ],
            custom_params=custom_params,
        )

    @property
    def timeframe_options(self) -> list[LookMLTimeframesOption]:
        return one(
            param
            for param in self.parameters
            if isinstance(param, FieldParameterTimeframes)
        ).timeframe_options

    def _dimension_name_for_timeframe(self, timeframe: LookMLTimeframesOption) -> str:
        """Returns the field name for the given timeframe"""
        if timeframe not in self.timeframe_options:
            raise ValueError(
                f"Timeframe {timeframe.value} not found in {self.timeframe_options}"
            )

        return f"{self.field_name}_{timeframe.value.lower()}"

    @property
    def dimension_names(self) -> list[str]:
        """Returns a list of dimension names created by the dimension group"""
        return [
            self._dimension_name_for_timeframe(timeframe)
            for timeframe in self.timeframe_options
        ]

    @property
    def date_dimension_name(self) -> str:
        """Returns the dimension name for the date timeframe"""
        return self._dimension_name_for_timeframe(LookMLTimeframesOption.DATE)


@attr.define
class DurationDimensionGroupLookMLViewField(DimensionGroupLookMLViewField):
    """Defines a LookML dimension group used to create a set of interval-based dimensions."""

    field_category = LookMLFieldCategory.DIMENSION_GROUP

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        type_param = one(
            param for param in self.parameters if isinstance(param, FieldParameterType)
        )
        if type_param.field_type != LookMLFieldType.DURATION:
            raise ValueError(
                f"Type parameter must be `duration`, got [{type_param.field_type}]."
            )
        # Enforce that datatype exists when type is date
        enforce_has_datatype_if_date_field(self.parameters)

    @property
    def dimension_names(self) -> list[str]:
        raise NotImplementedError(
            "DurationDimensionGroupLookMLViewField.dimension_names not implemented"
        )
