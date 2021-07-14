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
from typing import Any, Dict, List, Optional, Type

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
)
from recidiviz.calculator.pipeline.utils.event_utils import IdentifierEvent
from recidiviz.calculator.pipeline.utils.metric_utils import (
    RecidivizMetric,
    RecidivizMetricTypeT,
)
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.common.constants.state.external_id_types import (
    US_ID_DOC,
    US_MO_DOC,
    US_ND_ELITE,
    US_ND_SID,
    US_PA_CONTROL,
    US_PA_PBPP,
)
from recidiviz.common.date import (
    first_day_of_month,
    last_day_of_month,
    year_and_month_for_today,
)
from recidiviz.persistence.entity.state.entities import StatePerson

PRIMARY_PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE = {
    "incarceration": {
        "US_ID": US_ID_DOC,
        "US_MO": US_MO_DOC,
        "US_ND": US_ND_ELITE,
        "US_PA": US_PA_CONTROL,
    },
    "recidivism": {"US_ND": US_ND_ELITE},
    "supervision": {
        "US_ID": US_ID_DOC,
        "US_MO": US_MO_DOC,
        "US_ND": US_ND_SID,
        "US_PA": US_PA_PBPP,
    },
}

# For certain metrics we may record multiple kinds of external IDs for a person
SECONDARY_PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE = {
    "incarceration": {
        "US_ID": US_ID_DOC,
        "US_MO": US_MO_DOC,
        "US_ND": US_ND_SID,
        "US_PA": US_PA_PBPP,
    }
}


def person_characteristics(
    person: StatePerson,
    event_date: datetime.date,
    person_metadata: PersonMetadata,
    pipeline: str,
) -> Dict[str, Any]:
    """Adds the person's demographic characteristics to the given |characteristics|
    dictionary. For the 'age_bucket' field, calculates the person's age on the
    |event_date|. Adds the person's person_id and, if applicable,
    a person_external_id and potentially a secondary person_external_id.
    """
    characteristics: Dict[str, Any] = {}

    event_age = age_at_date(person, event_date)
    event_age_bucket = age_bucket(event_age)
    if event_age_bucket is not None:
        characteristics["age_bucket"] = event_age_bucket
    if person.gender is not None:
        characteristics["gender"] = person.gender
    if person_metadata and person_metadata.prioritized_race_or_ethnicity:
        characteristics[
            "prioritized_race_or_ethnicity"
        ] = person_metadata.prioritized_race_or_ethnicity

    characteristics["person_id"] = person.person_id

    primary_person_external_id = person_external_id_to_include(
        pipeline, person.state_code, person, secondary=False
    )

    if primary_person_external_id is not None:
        characteristics["person_external_id"] = primary_person_external_id

    secondary_person_external_id = person_external_id_to_include(
        pipeline, person.state_code, person, secondary=True
    )

    if secondary_person_external_id is not None:
        characteristics["secondary_person_external_id"] = secondary_person_external_id

    return characteristics


def age_at_date(person: StatePerson, check_date: datetime.date) -> Optional[int]:
    """Calculates the age of the StatePerson at the given date.

    Args:
        person: the StatePerson
        check_date: the date to check

    Returns:
        The age of the StatePerson at the given date. None if no birthdate is
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


def age_bucket(age: Optional[int]) -> Optional[str]:
    """Calculates the age bucket that applies to measurement.

    Age buckets for measurement: <25, 25-29, 30-34, 35-39, 40<

    Args:
        age: the person's age

    Returns:
        A string representation of the age bucket for the person. None if the
            age is not known.
    """
    if age is None:
        return None
    if age < 25:
        return "<25"
    if age <= 29:
        return "25-29"
    if age <= 34:
        return "30-34"
    if age <= 39:
        return "35-39"
    return "40<"


def person_external_id_to_include(
    pipeline: str, state_code: str, person: StatePerson, secondary: bool = False
) -> Optional[str]:
    """Finds an external_id on the person that should be included in calculations for
    person-level metrics in the given pipeline.

    If |secondary| is True, finds an external_id of a type that is listed as a
    "secondary" type to be included for metrics of the given pipeline. Else, finds an
    external_id of the "primary" type.
    """
    external_ids = person.external_ids

    if not external_ids:
        return None

    id_types_to_include_for_pipeline = (
        SECONDARY_PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE.get(pipeline)
        if secondary
        else PRIMARY_PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE.get(pipeline)
    )

    if (
        not id_types_to_include_for_pipeline
        or state_code not in id_types_to_include_for_pipeline
    ):
        return None

    id_type_to_include = id_types_to_include_for_pipeline.get(state_code)

    if not id_type_to_include:
        return None

    external_ids_with_type = []
    for external_id in external_ids:
        if external_id.state_code != state_code:
            raise ValueError(
                f"Found unexpected state code [{external_id.state_code}] on external_id [{external_id.external_id}]. "
                f"Expected state code: [{state_code}]."
            )

        if external_id.id_type == id_type_to_include:
            external_ids_with_type.append(external_id.external_id)

    if not external_ids_with_type:
        return None

    return sorted(external_ids_with_type)[0]


def include_in_output(
    year: int,
    month: int,
    calculation_month_upper_bound: datetime.date,
    calculation_month_lower_bound: Optional[datetime.date],
) -> bool:
    """Determines whether the event with the given year and month should be included in the metric output.
    If the calculation_month_lower_bound is None, then includes the bucket if it occurred in or before the month of the
    calculation_month_upper_bound. If the calculation_month_lower_bound is set, then includes the event if it happens
    in a month between the calculation_month_lower_bound and the calculation_month_upper_bound (inclusive). The
    calculation_month_upper_bound is always the last day of a month, and, if set, the calculation_month_lower_bound is
    always the first day of a month."""
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


def get_calculation_month_upper_bound_date(
    calculation_end_month: Optional[str],
) -> datetime.date:
    """Returns the date at the end of the month represented in the calculation_end_month string. String must
    be in the format YYYY-MM. If calculation_end_month is unset, returns the last day of the current month."""
    if not calculation_end_month:
        year, month = year_and_month_for_today()
        return last_day_of_month(datetime.date(year, month, 1))

    try:
        end_month_date = datetime.datetime.strptime(
            calculation_end_month, "%Y-%m"
        ).date()
    except ValueError as e:
        raise ValueError(
            f"Invalid value for calculation_end_month: {calculation_end_month}"
        ) from e

    return last_day_of_month(end_month_date)


def get_calculation_month_lower_bound_date(
    calculation_month_upper_bound: datetime.date, calculation_month_count: int
) -> Optional[datetime.date]:
    """Returns the date at the beginning of the first month that should be included in the monthly calculations."""

    first_of_last_month = first_day_of_month(calculation_month_upper_bound)

    calculation_month_lower_bound = (
        (first_of_last_month - relativedelta(months=(calculation_month_count - 1)))
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


def produce_standard_metrics(
    pipeline: str,
    person: StatePerson,
    identifier_events: List[IdentifierEvent],
    metric_inclusions: Dict[RecidivizMetricTypeT, bool],
    calculation_end_month: Optional[str],
    calculation_month_count: int,
    person_metadata: PersonMetadata,
    event_to_metric_classes: Dict[
        Type[IdentifierEvent],
        List[Type[RecidivizMetric[RecidivizMetricTypeT]]],
    ],
    pipeline_job_id: str,
    additional_attributes: Optional[Dict[str, Any]] = None,
) -> List[RecidivizMetric]:
    """Produces metrics for pipelines with a standard mapping of event to metric
    type."""
    metrics: List[RecidivizMetric] = []

    calculation_month_upper_bound = get_calculation_month_upper_bound_date(
        calculation_end_month
    )

    calculation_month_lower_bound = get_calculation_month_lower_bound_date(
        calculation_month_upper_bound, calculation_month_count
    )

    for event in identifier_events:
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

        for metric_class in metric_classes:
            metric_type = metric_type_for_metric_class(metric_class)

            if metric_inclusions.get(metric_type):
                metric = build_metric(
                    pipeline=pipeline,
                    event=event,
                    metric_class=metric_class,
                    person=person,
                    event_date=event_date,
                    person_metadata=person_metadata,
                    pipeline_job_id=pipeline_job_id,
                    additional_attributes=additional_attributes,
                )

                metrics.append(metric)

    return metrics


def build_metric(
    pipeline: str,
    event: IdentifierEvent,
    metric_class: Type[RecidivizMetric],
    person: StatePerson,
    event_date: datetime.date,
    person_metadata: PersonMetadata,
    pipeline_job_id: str,
    additional_attributes: Optional[Dict[str, Any]] = None,
) -> RecidivizMetric:
    """Builds a RecidivizMetric of the defined metric_class using the provided
    information.
    """
    metric_attributes = attr.fields_dict(metric_class).keys()

    person_attributes = person_characteristics(
        person,
        event_date,
        person_metadata,
        pipeline,
    )

    metric_cls_builder = metric_class.builder()

    # Set pipeline attributes
    setattr(metric_cls_builder, "job_id", pipeline_job_id)
    setattr(metric_cls_builder, "created_on", datetime.date.today())

    # Set date attributes if applicable
    if "year" in metric_attributes:
        setattr(metric_cls_builder, "year", event_date.year)
    if "month" in metric_attributes:
        setattr(metric_cls_builder, "month", event_date.month)

    # Add all demographic and person-level dimensions
    for attribute, value in person_attributes.items():
        setattr(metric_cls_builder, attribute, value)

    # Add attributes from the event that are relevant to the metric_class
    for metric_attribute in metric_attributes:
        if hasattr(event, metric_attribute):
            attribute_value = getattr(event, metric_attribute)
            setattr(metric_cls_builder, metric_attribute, attribute_value)

    # Add any additional attributes not on the event
    if additional_attributes:
        for attribute, value in additional_attributes.items():
            if attribute in metric_attributes:
                setattr(metric_cls_builder, attribute, value)

    return metric_cls_builder.build()


def metric_type_for_metric_class(
    metric_class: Type[RecidivizMetric[RecidivizMetricTypeT]],
) -> RecidivizMetricTypeT:
    """Returns the RecidivizMetricType corresponding to the given RecidivizMetric class."""
    metric_table = DATAFLOW_METRICS_TO_TABLES[metric_class]
    metric_type = DATAFLOW_TABLES_TO_METRIC_TYPES[metric_table]
    if not isinstance(metric_type, metric_class.metric_type_cls):
        raise ValueError(
            f"Found incorrect metric type [{metric_type}], expected value of type "
            f"[{metric_class.metric_type_cls}]"
        )
    return metric_type
