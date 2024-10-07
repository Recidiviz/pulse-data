# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Utils for writing validations of Dataflow metrics."""
from enum import Enum
from typing import List, Set, Type

from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    generate_metric_view_names,
)
from recidiviz.common.attr_mixins import (
    attr_field_enum_cls_for_field_name,
    attribute_field_type_reference_for_class,
)
from recidiviz.pipelines.dataflow_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.utils.string import StrictStringFormatter

SELECT_FROM_METRICS_TEMPLATE = (
    "(SELECT state_code, state_code AS region_code, {columns} FROM "
    "`{{project_id}}.{{materialized_metrics_dataset}}.most_recent_{metric_view_name}_materialized` "
    "{invalid_rows_filter_clause})"
)

SELECT_ROWS_FROM_METRIC_ALL_INVALID_FIELD_TEMPLATE = """
SELECT
    state_code, 
    state_code AS region_code, 
    '{field}' AS field_name,
    '{metric_view_name}' AS metric,
    COUNTIF({field} {invalid_clause}) AS invalid_count,
    COUNT(*) AS row_count FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_{metric_view_name}_materialized`
    GROUP BY 1, 2, 3
    HAVING row_count = invalid_count
"""


def _metric_views_for_metric_classes(
    metric_classes: List[Type[RecidivizMetric]],
) -> List[str]:
    """Returns all metric view names that are associated with the given
    |metric_classes|.
    """
    return [
        metric_view_name
        for metric in metric_classes
        for metric_view_name in generate_metric_view_names(
            DATAFLOW_METRICS_TO_TABLES[metric]
        )
    ]


def _metrics_with_enum_field_of_type(
    enum_field_name: str, enum_field_type: Type[Enum]
) -> List[Type[RecidivizMetric]]:
    """Returns all metric classes that contain the given |enum_field_name| storing the
    given |enum_field_type|."""
    return [
        metric
        for metric in DATAFLOW_METRICS_TO_TABLES
        if (enum_cls := attr_field_enum_cls_for_field_name(metric, enum_field_name))
        and enum_cls == enum_field_type
    ]


def _metrics_with_field(field_name: str) -> List[Type[RecidivizMetric]]:
    """Returns all metric classes that contain the given |field_name|."""
    return [
        metric
        for metric in DATAFLOW_METRICS_TO_TABLES
        if field_name in attribute_field_type_reference_for_class(metric).fields
    ]


def all_enum_fields_across_all_metrics_with_field_value(enum_value: str) -> Set[str]:
    """Returns all fields across all metric classes that are enums that could be a
    certain value.
    """
    result = set()
    for metric in DATAFLOW_METRICS_TO_TABLES:
        metric_class_ref = attribute_field_type_reference_for_class(metric)
        for field_name in metric_class_ref.fields:
            cached_attribute_info = metric_class_ref.get_field_info(field_name)
            if (
                enum_cls := cached_attribute_info.enum_cls
            ) and enum_value in enum_cls.__members__:
                result.add(field_name)
    return result


def _validate_metric_has_all_fields(
    metric: Type[RecidivizMetric], fields_to_validate: List[str]
) -> None:
    """Asserts that the given |metric| class contains all of the fields in |fields_to_validate|.
    Raises an error if the metric does not contain all of the fields."""
    class_reference = attribute_field_type_reference_for_class(metric)
    for field in fields_to_validate:
        if field not in class_reference.fields:
            raise ValueError(
                f"The {metric.__name__} does not contain metric field: " f"{field}."
            )


def _validation_query_for_metrics(
    metric_classes: List[Type[RecidivizMetric]],
    additional_columns_to_select: List[str],
    invalid_rows_filter_clause: str,
    validation_description: str,
) -> str:
    """Builds a validation query for all metric views associated with the given
    |metric_classes|."""
    if not invalid_rows_filter_clause.startswith("WHERE"):
        raise ValueError(
            "Invalid filter clause. Must start with 'WHERE'. "
            f"Found: {invalid_rows_filter_clause}."
        )

    queries_for_metric_views = [
        StrictStringFormatter().format(
            SELECT_FROM_METRICS_TEMPLATE,
            columns=", ".join(additional_columns_to_select),
            metric_view_name=metric_view_name,
            invalid_rows_filter_clause=invalid_rows_filter_clause,
        )
        for metric_view_name in _metric_views_for_metric_classes(
            metric_classes=metric_classes
        )
    ]

    return f"/*{validation_description}*/\n" + "\n UNION ALL \n".join(
        queries_for_metric_views
    )


def validation_query_for_metric_views_with_all_invalid_fields(
    field_name: str, invalid_clause: str, validation_description: str
) -> str:
    """Builds a validation query for all Dataflow metrics that contain a field with the
    given |field_name| that returns rows for any metric where the field satisfies the
    |invalid_clause| in ALL rows (e.g. all values are NULL)."""
    metrics_with_field = _metrics_with_field(field_name)

    if not metrics_with_field:
        raise ValueError(f"No metric classes with the field {field_name}.")

    if not invalid_clause == "IS NULL" and not invalid_clause.startswith("= "):
        raise ValueError(
            "Invalid clause for the field value. Must be 'IS NULL' or start with '= '. "
            f"Found: {invalid_clause}"
        )

    queries_for_metric_views = [
        StrictStringFormatter().format(
            SELECT_ROWS_FROM_METRIC_ALL_INVALID_FIELD_TEMPLATE,
            field=field_name,
            metric_view_name=metric_view_name,
            invalid_clause=invalid_clause,
        )
        for metric_view_name in _metric_views_for_metric_classes(metrics_with_field)
    ]

    return f"/*{validation_description}*/\n" + "\n UNION ALL \n".join(
        queries_for_metric_views
    )


def validation_query_for_metric_views_with_invalid_enums(
    enum_field_name: str,
    enum_field_type: Type[Enum],
    additional_columns_to_select: List[str],
    invalid_rows_filter_clause: str,
    validation_description: str,
) -> str:
    """Returns a validation query that finds all of the metric views that contain the
    given |enum_field_name| storing the given |enum_field_type|, where there are
    invalid rows.
    """
    metrics_with_enum_field = _metrics_with_enum_field_of_type(
        enum_field_name=enum_field_name, enum_field_type=enum_field_type
    )

    if not metrics_with_enum_field:
        raise ValueError(
            f"No metric classes with the field {enum_field_name} that "
            f"store the enum {enum_field_type.__name__}."
        )

    for metric in metrics_with_enum_field:
        _validate_metric_has_all_fields(metric, additional_columns_to_select)

    return _validation_query_for_metrics(
        metric_classes=metrics_with_enum_field,
        additional_columns_to_select=additional_columns_to_select,
        invalid_rows_filter_clause=invalid_rows_filter_clause,
        validation_description=validation_description,
    )


def validation_query_for_metric(
    metric: Type[RecidivizMetric],
    invalid_rows_filter_clause: str,
    additional_columns_to_select: List[str],
    validation_description: str,
) -> str:
    """Returns a validation query that finds all of the metric views corresponding to
    a given metric class, and identifies any invalid rows according to the
    |invalid_rows_filter_clause|.
    """
    _validate_metric_has_all_fields(metric, additional_columns_to_select)

    return _validation_query_for_metrics(
        metric_classes=[metric],
        additional_columns_to_select=additional_columns_to_select,
        invalid_rows_filter_clause=invalid_rows_filter_clause,
        validation_description=validation_description,
    )
