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
from typing import List, Type

import attr

from recidiviz.calculator.dataflow_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    generate_metric_view_names,
)
from recidiviz.common.attr_utils import get_enum_cls
from recidiviz.common.constants.entity_enum import EntityEnum
from recidiviz.utils.string import StrictStringFormatter

SELECT_FROM_METRICS_TEMPLATE = (
    "(SELECT state_code AS region_code, {columns} FROM "
    "`{{project_id}}.{{materialized_metrics_dataset}}.most_recent_{metric_view_name}_materialized` "
    "{invalid_rows_filter_clause})"
)


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
    enum_field_name: str, enum_field_type: Type[EntityEnum]
) -> List[Type[RecidivizMetric]]:
    """Returns all metric classes that contain the given |enum_field_name|
    storing the given |enum_field_type|.
    """
    return [
        metric
        for metric in DATAFLOW_METRICS_TO_TABLES
        if (field := attr.fields_dict(metric).get(enum_field_name)) is not None
        and get_enum_cls(field) == enum_field_type
    ]


def _validate_metric_has_all_fields(
    metric: Type[RecidivizMetric], fields_to_validate: List[str]
) -> None:
    """Asserts that the given |metric| class contains all of the fields in
    |fields_to_validate|.

    Raises an error if the metric does not contain all of the fields.
    """
    for field in fields_to_validate:
        if (attr.fields_dict(metric).get(field)) is None:
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


def validation_query_for_metric_views_with_invalid_enums(
    enum_field_name: str,
    enum_field_type: Type[EntityEnum],
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
