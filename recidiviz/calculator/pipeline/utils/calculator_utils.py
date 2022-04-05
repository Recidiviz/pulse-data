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
from typing import Optional, List, Any, Dict, Tuple

import dateutil
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.state_utils.us_mo import us_mo_violation_utils
from recidiviz.calculator.pipeline.utils.execution_utils import year_and_month_for_today
from recidiviz.calculator.pipeline.utils.metric_utils import MetricMethodologyType
from recidiviz.common.constants.state.external_id_types import US_ID_DOC, US_MO_DOC, US_PA_CONTROL, US_PA_PBPP
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseDecision
from recidiviz.persistence.entity.state.entities import StatePerson, StateSupervisionViolation

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

VIOLATION_TYPE_SEVERITY_ORDER = [
    StateSupervisionViolationType.FELONY,
    StateSupervisionViolationType.MISDEMEANOR,
    StateSupervisionViolationType.ABSCONDED,
    StateSupervisionViolationType.MUNICIPAL,
    StateSupervisionViolationType.ESCAPED,
    StateSupervisionViolationType.TECHNICAL
]


def add_demographic_characteristics(characteristics: Dict[str, Any],
                                    person: StatePerson,
                                    event_date: date) -> Dict[str, Any]:
    """Adds the person's demographic characteristics to the given |characteristics| dictionary. For the 'age_bucket'
    field, calculates the person's age on the |event_date|.
    """
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


def identify_most_severe_violation_type_and_subtype(violations: List[StateSupervisionViolation]) \
        -> Tuple[Optional[StateSupervisionViolationType], Optional[str]]:
    """Identifies the most severe violation type on the provided |violations|, and, if relevant, the subtype of that
    most severe violation type. Returns both as a tuple.
    """
    most_severe_violation_type = _identify_most_severe_violation_type(violations)
    identified_subtype = identify_violation_subtype(most_severe_violation_type, violations)
    # TODO(2853): Figure out how to model unset variables generally in the calculation pipeline.
    violation_subtype = identified_subtype
    return most_severe_violation_type, violation_subtype


def _identify_most_severe_violation_type(
        violations: List[StateSupervisionViolation]) -> Optional[StateSupervisionViolationType]:
    """Identifies the most severe violation type on the violation according
    to the static violation type ranking."""
    if not violations:
        return None

    state_code = violations[0].state_code

    if state_code.upper() == 'US_MO':
        return us_mo_violation_utils.us_mo_identify_most_severe_violation_type(
            violations, VIOLATION_TYPE_SEVERITY_ORDER)

    violation_types = []
    for violation in violations:
        for violation_type_entry in violation.supervision_violation_types:
            violation_types.append(violation_type_entry.violation_type)
    return next((violation_type for violation_type in VIOLATION_TYPE_SEVERITY_ORDER
                 if violation_type in violation_types), None)


def identify_violation_subtype(violation_type: Optional[StateSupervisionViolationType],
                               violations: List[StateSupervisionViolation]) \
        -> Optional[str]:
    """Looks through all provided |violations| of type |violation_type| and returns a string subtype, if necessary."""
    if not violation_type or not violations:
        return None

    state_code = violations[0].state_code
    if state_code.upper() == 'US_MO':
        return us_mo_violation_utils.us_mo_identify_violation_subtype(violation_type, violations)

    return None


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


def characteristics_with_person_id_fields(
        characteristics: Dict[str, Any], state_code: str, person: StatePerson, pipeline: str) -> Dict[str, Any]:
    """Returns an updated characteristics dictionary with the person's person_id and, if applicable, a
    person_external_id."""
    updated_characteristics = characteristics.copy()

    # person_id and person_external_id is added to a characteristics combination dictionary that has all fields set. We
    # only want person-level output that has all possible fields set.
    person_id = person.person_id

    updated_characteristics['person_id'] = person_id

    person_external_id = person_external_id_to_include(pipeline, state_code, person)

    if person_external_id is not None:
        updated_characteristics['person_external_id'] = person_external_id

    return updated_characteristics
