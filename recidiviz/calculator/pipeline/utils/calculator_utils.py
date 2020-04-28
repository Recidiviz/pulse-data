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
import json
from datetime import date
from itertools import combinations
from typing import Optional, List, Any, Dict, Tuple

import dateutil
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils import us_mo_utils
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType, json_serializable_metric_key
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseDecision
from recidiviz.persistence.entity.state.entities import StatePerson, StateSupervisionViolation

# Relevant metric period month lengths for dashboard person-based calculations
METRIC_PERIOD_MONTHS = [36, 12, 6, 3]

PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE = {
    'incarceration': {
        'US_MO': ['US_MO_DOC']
    },
    'supervision': {
        'US_MO': ['US_MO_DOC']
    },
}

DECISION_SEVERITY_ORDER = [
        StateSupervisionViolationResponseDecision.REVOCATION,
        StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
        StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON,
        StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        StateSupervisionViolationResponseDecision.EXTENSION,
        StateSupervisionViolationResponseDecision.SPECIALIZED_COURT,
        StateSupervisionViolationResponseDecision.SUSPENSION,
        StateSupervisionViolationResponseDecision.SERVICE_TERMINATION,
        StateSupervisionViolationResponseDecision.DELAYED_ACTION,
        StateSupervisionViolationResponseDecision.CONTINUANCE
    ]

VIOLATION_TYPE_SEVERITY_ORDER = [
    StateSupervisionViolationType.FELONY,
    StateSupervisionViolationType.MISDEMEANOR,
    StateSupervisionViolationType.ABSCONDED,
    StateSupervisionViolationType.MUNICIPAL,
    StateSupervisionViolationType.ESCAPED,
    StateSupervisionViolationType.TECHNICAL
]


def for_characteristics_races_ethnicities(characteristics: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Produces the list of all combinations of the given metric characteristics, given the fact that there can be
    multiple races and ethnicities present.

    For example, this function call:
        for_characteristics_races_ethnicities(characteristics: {
            'gender': Gender.FEMALE, 'age': '<25', 'race': [Race.BLACK, Race.WHITE],
            'ethnicity': [Ethnicity.HISPANIC, Ethnicity.NOT_HISPANIC]
            }
        )

    First computes all combinations for the given base characteristics, excluding the race and ethnicity values. Then,
    for each race present, adds a copy of each combination augmented with the race. For each ethnicity present, adds a
    copy of each combination augmented with the ethnicity. Finally, for every combination of race and ethnicity, adds a
    copy of each combination augmented with both the race and the ethnicity.
    """
    base_characteristics = characteristics.copy()

    # Remove and store the race and ethnicity lists
    races = base_characteristics.pop('race', [])
    ethnicities = base_characteristics.pop('ethnicity', [])

    # Initial combinations
    combos: List[Dict[str, Any]] = for_characteristics(base_characteristics)

    # Race additions
    race_combos: List[Dict[Any, Any]] = []
    for race in races:
        for combo in combos:
            augmented_combo = combo.copy()
            augmented_combo['race'] = race
            race_combos.append(augmented_combo)

    # Ethnicity additions
    ethnicity_combos: List[Dict[Any, Any]] = []
    for ethnicity in ethnicities:
        for combo in combos:
            augmented_combo = combo.copy()
            augmented_combo['ethnicity'] = ethnicity
            ethnicity_combos.append(augmented_combo)

    # Multi-race and ethnicity additions
    race_ethnicity_combos: List[Dict[Any, Any]] = []
    for race in races:
        for ethnicity in ethnicities:
            for combo in combos:
                augmented_combo = combo.copy()
                augmented_combo['race'] = race
                augmented_combo['ethnicity'] = ethnicity
                race_ethnicity_combos.append(augmented_combo)

    combos = combos + race_combos + ethnicity_combos + race_ethnicity_combos

    return combos


def for_characteristics(characteristics) -> List[Dict[str, Any]]:
    """Produces the list of all combinations of the given metric
     characteristics.

    Example:
        for_characteristics(
        {'race': Race.BLACK, 'gender': Gender.FEMALE, 'age': '<25'}) =
            [{},
            {'age': '<25'}, {'race': Race.BLACK}, {'gender': Gender.FEMALE},
            {'age': '<25', 'race': Race.BLACK}, {'age': '<25',
                'gender': Gender.FEMALE},
            {'race': Race.BLACK, 'gender': Gender.FEMALE},
            {'age': '<25', 'race': Race.BLACK, 'gender': Gender.FEMALE}]


    Args:
        characteristics: a dictionary of metric characteristics to derive
            combinations from

    Returns:
        A list of dictionaries containing all unique combinations of
        characteristics.
    """
    combos: List[Dict[Any, Any]] = [{}]
    for i in range(len(characteristics)):
        i_combinations = map(dict,
                             combinations(characteristics.items(), i + 1))
        for combo in i_combinations:
            combos.append(combo)
    return combos


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
        characteristics['race'] = [race_object.race for race_object in person.races]
    if person.ethnicities:
        characteristics['ethnicity'] = [ethnicity_object.ethnicity for ethnicity_object in person.ethnicities]

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


def convert_event_based_to_person_based_metrics(
        metrics: List[Tuple[Dict[str, Any], Any]]) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """
    Takes in a set of event-based metrics and converts them to be person-based
    by removing any duplicate metric dictionaries attributed to this person.

    By eliminating duplicate instances of metric keys, this person will only
    contribute a +1 to a metric once per metric for all person-based counts.
    """

    person_based_metrics_set = set()

    for metric, value in metrics:
        metric['methodology'] = MetricMethodologyType.PERSON
        # Converting the metric key to a JSON string so it is hashable
        serializable_dict = json_serializable_metric_key(metric)
        json_key = json.dumps(serializable_dict, sort_keys=True)
        # Add the metric to the set
        person_based_metrics_set.add((json_key, value))

    person_based_metrics: List[Tuple[Dict[str, Any], Any]] = []

    for json_metric, value in person_based_metrics_set:
        # Convert JSON string to dictionary
        dict_metric_key = json.loads(json_metric)

        person_based_metrics.append((dict_metric_key, value))

    return person_based_metrics


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
    violation_subtype = identified_subtype if identified_subtype else 'UNSET'
    return most_severe_violation_type, violation_subtype


def _identify_most_severe_violation_type(
        violations: List[StateSupervisionViolation]) -> Optional[StateSupervisionViolationType]:
    """Identifies the most severe violation type on the violation according
    to the static violation type ranking."""
    if not violations:
        return None

    state_code = violations[0].state_code

    if state_code.upper() == 'US_MO':
        return us_mo_utils.us_mo_identify_most_severe_violation_type(violations, VIOLATION_TYPE_SEVERITY_ORDER)

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
        return us_mo_utils.us_mo_identify_violation_subtype(violation_type, violations)

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


def augmented_combo_list(combo: Dict[str, Any],
                         state_code: str,
                         year: int,
                         month: Optional[int],
                         methodology: MetricMethodologyType,
                         metric_period_months: Optional[int]) -> \
        List[Dict[str, Any]]:
    """Returns a list of combo dictionaries that have been augmented with
    necessary parameters.

    Args:
        combo: the base combo to be augmented with methodology and period
        state_code: the state code of the metric combo
        year: the year this metric describes
        month: the month this metric describes
        methodology: the MetricMethodologyType to add to each combo
        metric_period_months: the metric_period_months value to add to each
            combo

    Returns: a list of combos augmented with various parameters
    """

    combos = []
    parameters: Dict[str, Any] = {'state_code': state_code,
                                  'methodology': methodology,
                                  'year': year
                                  }

    if month:
        parameters['month'] = month

    if metric_period_months:
        parameters['metric_period_months'] = metric_period_months

    base_combo = augment_combination(combo, parameters)
    combos.append(base_combo)

    return combos


def person_external_id_to_include(pipeline: str, person: StatePerson) -> Optional[str]:
    """Finds an external_id on the person that should be included in calculations for person-level metrics in the
    given pipeline."""
    external_ids = person.external_ids

    values_for_pipeline = PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE.get(pipeline)

    if values_for_pipeline:
        for external_id in external_ids:
            if external_id.state_code in values_for_pipeline.keys():
                id_types_to_include = values_for_pipeline.get(external_id.state_code)

                if id_types_to_include and external_id.id_type in id_types_to_include:
                    return external_id.external_id

    return None


def include_in_monthly_metrics(year: int, month: int, calculation_month_lower_bound: Optional[date]) -> bool:
    """Determines whether the event with the given year and month should be included in the monthly metric output. If
    the calculation_month_lower_bound is None, then includes the bucket by default. If the calculation_month_lower_bound
    is set, then includes the event if it happens in a month on or after the calculation_month_lower_bound. The
    calculation_month_lower_bound is always the first day of a month."""
    if not calculation_month_lower_bound:
        return True

    return year >= calculation_month_lower_bound.year and (date(year, month, 1) >= calculation_month_lower_bound)


def get_calculation_month_lower_bound_date(calculation_month_upper_bound: date, calculation_month_limit: int) -> \
        Optional[date]:
    """Returns the date at the beginning of the first month that should be included in the monthly calculations."""

    first_of_last_month = first_day_of_month(calculation_month_upper_bound)

    calculation_month_lower_bound = (first_of_last_month - relativedelta(months=(calculation_month_limit - 1))) \
        if calculation_month_limit != -1 else None

    return calculation_month_lower_bound


def characteristics_with_person_id_fields(characteristics: Dict[str, Any], person: StatePerson, pipeline: str) -> \
        Dict[str, Any]:
    """Returns an updated characteristics dictionary with the person's person_id and, if applicable, a
    person_external_id."""
    updated_characteristics = characteristics.copy()

    # person_id and person_external_id is added to a characteristics combination dictionary that has all fields set. We
    # only want person-level output that has all possible fields set.
    person_id = person.person_id

    updated_characteristics['person_id'] = person_id

    person_external_id = person_external_id_to_include(pipeline, person)

    if person_external_id is not None:
        updated_characteristics['person_external_id'] = person_external_id

    return updated_characteristics
