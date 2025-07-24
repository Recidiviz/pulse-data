# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for the various calculation pipelines."""
import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Type, TypeVar

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.common.date import (
    current_date_us_eastern,
    first_day_of_month,
    last_day_of_month,
    split_range_by_birthdate,
    year_and_month_for_today_us_eastern,
)
from recidiviz.persistence.entity.state.normalized_entities import NormalizedStatePerson
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.pipelines.utils.identifier_models import Event, IdentifierResult, Span

RecidivizMetricT = TypeVar("RecidivizMetricT", bound=RecidivizMetric)


def person_characteristics(
    person: NormalizedStatePerson, person_age: Optional[int]
) -> Dict[str, Any]:
    """Adds the person's demographic characteristics to the given |characteristics|
    dictionary. Adds the person's person_id and, if applicable,
    a person_external_id and potentially a secondary person_external_id.
    """
    characteristics: Dict[str, Any] = {}

    if person_age is not None:
        characteristics["age"] = person_age
    if person.gender is not None:
        characteristics["gender"] = person.gender

    characteristics["person_id"] = person.person_id

    return characteristics


def age_at_date(
    person: NormalizedStatePerson, check_date: datetime.date
) -> Optional[int]:
    """Calculates the age of the NormalizedStatePerson at the given date.

    Args:
        person: the NormalizedStatePerson
        check_date: the date to check

    Returns:
        The age of the NormalizedStatePerson at the given date. None if no birthdate is
         known.
    """
    birthdate = person.birthdate
    return (
        None
        if birthdate is None
        else check_date.year
        - birthdate.year
        - ((check_date.month, check_date.day) < (birthdate.month, birthdate.day))
    )


def include_in_output(
    year: int,
    month: int,
    calculation_month_upper_bound: datetime.date,
    calculation_month_lower_bound: Optional[datetime.date],
) -> bool:
    """Determines whether the event with the given year and month should be included in
    the metric output. If the calculation_month_lower_bound is None, then includes
    the event if it occurred in or before the month of the
    calculation_month_upper_bound. If the calculation_month_lower_bound is set, then
    includes the event if it happens in a month between the
    calculation_month_lower_bound and the calculation_month_upper_bound (inclusive). The
    calculation_month_upper_bound is always the last day of a month, and, if set, the
    calculation_month_lower_bound is always the first day of a month."""
    if not calculation_month_lower_bound:
        return year < calculation_month_upper_bound.year or (
            year == calculation_month_upper_bound.year
            and month <= calculation_month_upper_bound.month
        )

    return (
        calculation_month_lower_bound
        <= datetime.date(year, month, 1)
        <= calculation_month_upper_bound
    )


def get_calculation_month_upper_bound_date() -> datetime.date:
    """Returns returns the last day of the current month. String must be in the format
    YYYY-MM."""
    year, month = year_and_month_for_today_us_eastern()
    return last_day_of_month(datetime.date(year, month, 1))


def get_calculation_month_lower_bound_date(
    calculation_month_upper_bound: datetime.date, calculation_month_count: int
) -> Optional[datetime.date]:
    """Returns the date at the beginning of the first month that should be included in the monthly calculations."""

    first_of_last_month = first_day_of_month(calculation_month_upper_bound)

    calculation_month_lower_bound = (
        first_of_last_month - relativedelta(months=calculation_month_count - 1)
        if calculation_month_count != -1
        else None
    )

    return calculation_month_lower_bound


def safe_list_index(list_of_values: List[Any], value: Any, default: int) -> int:
    """Returns the index of the |value| in the |list_of_values|, if the |value| exists in the list. If the |value| is
    not present in the |list_of_values|, returns the provided |default| value."""
    try:
        return list_of_values.index(value)
    except ValueError:
        return default


def produce_standard_event_metrics(
    person: NormalizedStatePerson,
    identifier_results: Sequence[Event],
    calculation_month_count: int,
    event_to_metric_classes: Mapping[
        Type[Event],
        Sequence[Type[RecidivizMetricT]],
    ],
    pipeline_job_id: str,
    additional_attributes: Optional[Dict[str, Any]] = None,
) -> List[RecidivizMetricT]:
    """Produces metrics for pipelines with a standard mapping of event to metric
    type."""
    metrics: List[RecidivizMetricT] = []

    calculation_month_upper_bound = get_calculation_month_upper_bound_date()

    calculation_month_lower_bound = get_calculation_month_lower_bound_date(
        calculation_month_upper_bound, calculation_month_count
    )

    for event in identifier_results:
        event_date = event.event_date
        event_year = event.event_date.year
        event_month = event.event_date.month

        if not include_in_output(
            event_year,
            event_month,
            calculation_month_upper_bound,
            calculation_month_lower_bound,
        ):
            continue

        metric_classes = event_to_metric_classes[type(event)]
        if not metric_classes:
            raise ValueError(
                f"No included metric classes for event of type {type(event)}"
            )

        for metric_class in metric_classes:
            metric = build_metric(
                result=event,
                metric_class=metric_class,
                person=person,
                person_age=age_at_date(person, event_date),
                pipeline_job_id=pipeline_job_id,
                additional_attributes={
                    "year": event_date.year,
                    "month": event_date.month,
                    **(additional_attributes or {}),
                },
            )

            metrics.append(metric)

    return metrics


def produce_standard_span_metrics(
    person: NormalizedStatePerson,
    identifier_results: Sequence[Span],
    event_to_metric_classes: Mapping[
        Type[Span],
        Sequence[Type[RecidivizMetricT]],
    ],
    pipeline_job_id: str,
    additional_attributes: Optional[Dict[str, Any]] = None,
) -> List[RecidivizMetricT]:
    """Produces metrics for pipelines with a standard mapping of span to metric
    type. This first splits the span if a person has a birthdate by that date so that
    we can produce age-based spans within a larger span."""
    metrics: List[RecidivizMetricT] = []

    for span in identifier_results:
        original_date_range = (
            span.start_date_inclusive,
            span.end_date_exclusive,
        )
        new_ranges = (
            split_range_by_birthdate(original_date_range, person.birthdate)
            if person.birthdate is not None
            else [original_date_range]
        )
        for start_date, end_date in new_ranges:
            new_span = attr.evolve(
                span,
                start_date_inclusive=start_date,
                end_date_exclusive=end_date,
            )
            age = age_at_date(person, new_span.start_date_inclusive)

            metric_classes = event_to_metric_classes[type(span)]
            if not metric_classes:
                raise ValueError(
                    f"No included metric classes for span of type {type(span)}"
                )
            for metric_class in metric_classes:
                metric = build_metric(
                    result=new_span,
                    metric_class=metric_class,
                    person=person,
                    person_age=age,
                    pipeline_job_id=pipeline_job_id,
                    additional_attributes=additional_attributes,
                )

                metrics.append(metric)

    return metrics


def build_metric(
    result: IdentifierResult,
    metric_class: Type[RecidivizMetricT],
    person: NormalizedStatePerson,
    person_age: Optional[int],
    pipeline_job_id: str,
    additional_attributes: Optional[Dict[str, Any]] = None,
) -> RecidivizMetricT:
    """Builds a RecidivizMetric of the defined metric_class using the provided
    information.
    """
    metric_attributes = attr.fields_dict(metric_class).keys()  # type: ignore[arg-type]

    person_attributes = person_characteristics(person, person_age)

    metric_cls_builder = metric_class.builder()

    # Set pipeline attributes
    setattr(metric_cls_builder, "job_id", pipeline_job_id)
    setattr(metric_cls_builder, "created_on", current_date_us_eastern())

    # Add all demographic and person-level dimensions
    for attribute, value in person_attributes.items():
        setattr(metric_cls_builder, attribute, value)

    # Add attributes from the event that are relevant to the metric_class
    for metric_attribute in metric_attributes:
        if hasattr(result, metric_attribute):
            attribute_value = getattr(result, metric_attribute)
            setattr(metric_cls_builder, metric_attribute, attribute_value)

    # Add any additional attributes not on the event
    if additional_attributes:
        for attribute, value in additional_attributes.items():
            if attribute in metric_attributes:
                setattr(metric_cls_builder, attribute, value)

    return metric_cls_builder.build()
