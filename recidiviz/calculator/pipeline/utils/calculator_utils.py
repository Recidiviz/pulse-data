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
from datetime import date
from typing import Optional, List, Any, Dict, Type, Union

import dateutil
import attr
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.incarceration.incarceration_event import IncarcerationEvent
from recidiviz.calculator.pipeline.program.program_event import ProgramEvent
from recidiviz.calculator.pipeline.recidivism.release_event import ReleaseEvent
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import SupervisionTimeBucket
from recidiviz.calculator.pipeline.utils.execution_utils import year_and_month_for_today
from recidiviz.calculator.pipeline.utils.metric_utils import MetricMethodologyType, RecidivizMetric, \
    PersonLevelMetric
from recidiviz.common.constants.state.external_id_types import US_ID_DOC, US_MO_DOC, US_PA_CONTROL, US_PA_PBPP
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseDecision
from recidiviz.persistence.entity.state.entities import StatePerson

# Relevant metric period month lengths for dashboard person-based calculations
METRIC_PERIOD_MONTHS = [36, 12, 6, 3]

PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE = {
    'incarceration': {
        'US_ID': US_ID_DOC,
        'US_MO': US_MO_DOC,
        'US_PA': US_PA_CONTROL,
    },
    'supervision': {
        'US_ID': US_ID_DOC,
        'US_MO': US_MO_DOC,
        'US_PA': US_PA_PBPP,
    },
}

DECISION_SEVERITY_ORDER = [
        StateSupervisionViolationResponseDecision.REVOCATION,
        StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
        StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON,
        StateSupervisionViolationResponseDecision.WARRANT_ISSUED,
        StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
        StateSupervisionViolationResponseDecision.EXTENSION,
        StateSupervisionViolationResponseDecision.SPECIALIZED_COURT,
        StateSupervisionViolationResponseDecision.SUSPENSION,
        StateSupervisionViolationResponseDecision.SERVICE_TERMINATION,
        StateSupervisionViolationResponseDecision.TREATMENT_IN_FIELD,
        StateSupervisionViolationResponseDecision.COMMUNITY_SERVICE,
        StateSupervisionViolationResponseDecision.DELAYED_ACTION,
        StateSupervisionViolationResponseDecision.OTHER,
        StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN,
        StateSupervisionViolationResponseDecision.WARNING,
        StateSupervisionViolationResponseDecision.CONTINUANCE,
    ]


def person_characteristics(person: StatePerson,
                           event_date: date,
                           pipeline: str) -> Dict[str, Any]:
    """Adds the person's demographic characteristics to the given |characteristics| dictionary. For the 'age_bucket'
    field, calculates the person's age on the |event_date|. Adds the person's person_id and, if applicable, a
    person_external_id.
    """
    characteristics: Dict[str, Any] = {}

    event_age = age_at_date(person, event_date)
    event_age_bucket = age_bucket(event_age)
    if event_age_bucket is not None:
        characteristics['age_bucket'] = event_age_bucket
    if person.gender is not None:
        characteristics['gender'] = person.gender
    if person.races:
        races = [race_object.race for race_object in person.races if race_object.race is not None]
        if races:
            characteristics['race'] = races
    if person.ethnicities:
        ethnicities = [ethnicity_object.ethnicity for ethnicity_object in person.ethnicities
                       if ethnicity_object.ethnicity is not None]
        if ethnicities:
            characteristics['ethnicity'] = ethnicities

    characteristics['person_id'] = person.person_id

    person_external_id = person_external_id_to_include(pipeline, person.state_code, person)

    if person_external_id is not None:
        characteristics['person_external_id'] = person_external_id

    return characteristics


def age_at_date(person: StatePerson, check_date: date) -> Optional[int]:
    """Calculates the age of the StatePerson at the given date.

    Args:
        person: the StatePerson
        check_date: the date to check

    Returns:
        The age of the StatePerson at the given date. None if no birthdate is
         known.
    """
    birthdate = person.birthdate
    return None if birthdate is None else \
        check_date.year - birthdate.year - \
        ((check_date.month, check_date.day) < (birthdate.month, birthdate.day))


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
        return '<25'
    if age <= 29:
        return '25-29'
    if age <= 34:
        return '30-34'
    if age <= 39:
        return '35-39'
    return '40<'


def augment_combination(characteristic_combo: Dict[str, Any],
                        parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Returns a copy of the combo with the additional parameters added.

    Creates a shallow copy of the given characteristic combination and sets the
    given attributes on the copy. This avoids updating the
    existing characteristic combo.

    Args:
        characteristic_combo: the combination to copy and augment
        parameters: dictionary of additional attributes to add to the combo

    Returns:
        The augmented characteristic combination, ready for tracking.
    """
    augmented_combo = characteristic_combo.copy()

    for key, value in parameters.items():
        augmented_combo[key] = value

    return augmented_combo


def first_day_of_month(any_date: datetime.date):
    """Returns the date corresponding to the first day of the month for the given date."""
    year = any_date.year
    month = any_date.month

    return date(year, month, 1)


def last_day_of_month(any_date: datetime.date):
    """Returns the date corresponding to the last day of the month for the given date."""
    first_of_next_month = first_day_of_next_month(any_date)
    return first_of_next_month - datetime.timedelta(days=1)


def first_day_of_next_month(any_date: datetime.date) -> datetime.date:
    """Returns the date corresponding to the first day of the next month for the given date."""
    next_month_date = any_date.replace(day=28) + datetime.timedelta(days=4)
    return next_month_date.replace(day=1)


def identify_most_severe_response_decision(
        decisions:
        List[StateSupervisionViolationResponseDecision]) -> \
        Optional[StateSupervisionViolationResponseDecision]:
    """Identifies the most severe decision on the responses according
    to the static decision type ranking."""
    return next((decision for decision in DECISION_SEVERITY_ORDER
                 if decision in decisions), None)


def relevant_metric_periods(event_date: date,
                            end_year: int, end_month: int) -> List[int]:
    """Given the year and month when this metric period ends, returns the
    relevant metric period months lengths for the given event_date.

    For example, if the end_year is 2009 and the end_month is 10, then we are
    looking for events that occurred since the start of the following months:
        - 10-2009 (metric_period = 1)
        - 08-2009 (metric_period = 3)
        - 05-2009 (metric_period = 6)
        - 11-2008 (metric_period = 12)
        - 11-2006 (metric_period = 36)


    If the event happened in 11-2008, then this function will return:
    [12, 36], because the event occurred within the 12-month metric period and
    the 36-month metric period of the given month.
    """
    start_of_month = date(end_year, end_month, 1)
    end_of_month = last_day_of_month(start_of_month)

    relevant_periods = []

    for metric_period in METRIC_PERIOD_MONTHS:
        start_of_bucket_boundary_month = \
            start_of_month - \
            dateutil.relativedelta.relativedelta(months=metric_period)

        boundary_date = last_day_of_month(start_of_bucket_boundary_month)

        if boundary_date < event_date <= end_of_month:
            relevant_periods.append(metric_period)
        else:
            break

    return relevant_periods


def augmented_combo_for_calculations(combo: Dict[str, Any],
                                     state_code: str,
                                     year: int,
                                     month: Optional[int],
                                     methodology: MetricMethodologyType,
                                     metric_period_months: Optional[int]) -> Dict[str, Any]:
    """Augments the given combo dictionary with the given parameters of the calculation.

    Args:
        combo: the base combo to be augmented with methodology and period
        state_code: the state code of the metric combo
        year: the year this metric describes
        month: the month this metric describes
        methodology: the MetricMethodologyType to add to each combo
        metric_period_months: the metric_period_months value to add to each
            combo

    Returns: Returns a dictionary that has been augmented with necessary parameters.
    """
    parameters: Dict[str, Any] = {'state_code': state_code,
                                  'methodology': methodology,
                                  'year': year}

    if month:
        parameters['month'] = month

    if metric_period_months is not None:
        parameters['metric_period_months'] = metric_period_months

    return augment_combination(combo, parameters)


def person_external_id_to_include(pipeline: str,
                                  state_code: str,
                                  person: StatePerson) -> Optional[str]:
    """Finds an external_id on the person that should be included in calculations for person-level metrics in the
    given pipeline."""
    external_ids = person.external_ids

    if not external_ids:
        return None

    id_types_to_include_for_pipeline = PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE.get(pipeline)

    if not id_types_to_include_for_pipeline or state_code not in id_types_to_include_for_pipeline:
        return None

    id_type_to_include = id_types_to_include_for_pipeline.get(state_code)

    if not id_type_to_include:
        return None

    external_ids_with_type = []
    for external_id in external_ids:
        if external_id.state_code != state_code:
            raise ValueError(
                f'Found unexpected state code [{external_id.state_code}] on external_id [{external_id.external_id}]. '
                f'Expected state code: [{state_code}].')

        if external_id.id_type == id_type_to_include:
            external_ids_with_type.append(external_id.external_id)

    if not external_ids_with_type:
        return None

    return sorted(external_ids_with_type)[0]


def include_in_historical_metrics(year: int,
                                  month: int,
                                  calculation_month_upper_bound: date,
                                  calculation_month_lower_bound: Optional[date]) -> bool:
    """Determines whether the event with the given year and month should be included in the historical metric output.
    If the calculation_month_lower_bound is None, then includes the bucket if it occurred in or before the month of the
    calculation_month_upper_bound. If the calculation_month_lower_bound is set, then includes the event if it happens
    in a month between the calculation_month_lower_bound and the calculation_month_upper_bound (inclusive). The
    calculation_month_upper_bound is always the last day of a month, and, if set, the calculation_month_lower_bound is
    always the first day of a month."""
    if not calculation_month_lower_bound:
        return (year < calculation_month_upper_bound.year
                or (year == calculation_month_upper_bound.year and month <= calculation_month_upper_bound.month))

    return calculation_month_lower_bound <= date(year, month, 1) <= calculation_month_upper_bound


def get_calculation_month_upper_bound_date(calculation_end_month: Optional[str]) -> date:
    """Returns the date at the end of the month represented in the calculation_end_month string. String must
    be in the format YYYY-MM. If calculation_end_month is unset, returns the last day of the current month. """
    if not calculation_end_month:
        year, month = year_and_month_for_today()
        return last_day_of_month(date(year, month, 1))

    try:
        end_month_date = datetime.datetime.strptime(calculation_end_month, '%Y-%m').date()
    except ValueError:
        raise ValueError(f"Invalid value for calculation_end_month: {calculation_end_month}")

    return last_day_of_month(end_month_date)


def get_calculation_month_lower_bound_date(calculation_month_upper_bound: date, calculation_month_count: int) -> \
        Optional[date]:
    """Returns the date at the beginning of the first month that should be included in the monthly calculations."""

    first_of_last_month = first_day_of_month(calculation_month_upper_bound)

    calculation_month_lower_bound = (first_of_last_month - relativedelta(months=(calculation_month_count - 1))) \
        if calculation_month_count != -1 else None

    return calculation_month_lower_bound


def characteristics_dict_builder(
        pipeline: str,
        event: Union[IncarcerationEvent, ProgramEvent, ReleaseEvent, SupervisionTimeBucket],
        metric_class: Type[RecidivizMetric],
        person: StatePerson, event_date: date, include_person_attributes: bool) -> Dict[str, Any]:
    """Builds a dictionary from the provided event and person that will eventually populate the values on the given
    metric_class. Only adds attributes to the dictionary that are relevant to the metric_class.

    Args:
        - state_code: The state_code corresponding to the event
        - pipeline: The name of the pipeline this dictionary is being populated for
        - event: The event that was a product of the pipeline's identifier step
        - metric_class: The type of RecidivizMetric that this event will contribute to
        - person: The StatePerson related to this event

    """
    characteristics: Dict[str, Any] = {}
    metric_attributes = attr.fields_dict(metric_class).keys()

    if include_person_attributes:
        person_attributes = person_characteristics(person, event_date, pipeline)

        # Add relevant demographic and person-level dimensions
        for attribute, value in person_attributes.items():
            if attribute in metric_attributes:
                characteristics[attribute] = value

    fields_not_in_events = list(attr.fields_dict(RecidivizMetric).keys())
    fields_not_in_events.extend(attr.fields_dict(PersonLevelMetric).keys())
    fields_not_in_events.extend([
        # These are determined by the period of time the metric describes
        'year',
        'month',
        'follow_up_period',
        'metric_period_months',

        # This is set by the contents of the `violation_type_frequency_counter` on
        # RevocationReturnSupervisionTimeBuckets
        'violation_count_type',

        # TODO(#3873): Remove deprecated aggregate fields from metrics
        # These are deprecated aggregated values from before when all metric outputs were aggregates. These are
        # currently being set in the `Produce...Metrics` step of each pipeline.
        'count',
        'total_releases',
        'recidivated_releases',
        'returns',
        'recidivism_rate',
        'successful_completion_count',
        'projected_completion_count',
        'average_days_served',
    ])

    # Add attributes from the event that are relevant to the metric_class
    for metric_attribute in metric_attributes:
        if hasattr(event, metric_attribute) and metric_attribute not in fields_not_in_events:
            attribute_value = getattr(event, metric_attribute)
            if attribute_value is not None:
                characteristics[metric_attribute] = attribute_value
        elif metric_attribute not in fields_not_in_events:
            raise ValueError(
                f'Did not find expected field [{metric_attribute}] in {event.__class__}. Metric class: {metric_class}')

    return characteristics


def safe_list_index(list_of_values: List[Any], value: Any, default: int) -> int:
    """Returns the index of the |value| in the |list_of_values|, if the |value| exists in the list. If the |value| is
    not present in the |list_of_values|, returns the provided |default| value."""
    try:
        return list_of_values.index(value)
    except ValueError:
        return default
