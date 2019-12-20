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
"""Calculates program metrics from program events.

This contains the core logic for calculating program metrics on a
person-by-person basis. It transforms ProgramEvents into program
metrics, key-value pairs where the key represents all of the dimensions
represented in the data point, and the value represents an indicator of whether
the person should contribute to that metric.
"""
import logging
from copy import deepcopy
from datetime import date
from typing import List, Dict, Tuple, Any, Optional

from recidiviz.calculator.pipeline.program.metrics import ProgramMetricType
from recidiviz.calculator.pipeline.program.program_event import ProgramEvent, \
    ProgramReferralEvent
from recidiviz.calculator.pipeline.utils.calculator_utils import age_at_date, \
    age_bucket, for_characteristics_races_ethnicities, for_characteristics, \
    convert_event_based_to_person_based_metrics
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.persistence.entity.state.entities import StatePerson


def map_program_combinations(person: StatePerson,
                             program_events:
                             List[ProgramEvent],
                             inclusions: Dict[str, bool]) \
        -> List[Tuple[Dict[str, Any], Any]]:
    """Transforms ProgramEvents and a StatePerson into metric
    combinations.

    Takes in a StatePerson and all of her ProgramEvents and returns an
    array of "program combinations". These are key-value pairs where the key
    represents a specific metric and the value represents whether or not
    the person should be counted as a positive instance of that metric.

    This translates a particular interaction with a program into many different
    program metrics. Each metric represents one of many possible
    combinations of characteristics being tracked for that event. For example,
    if a White male is referred to a program, there is a metric that corresponds
    to White people, one to males, one to White males, one to all people, and
    more depending on other dimensions in the data.

    Args:
        person: the StatePerson
        program_events: A list of ProgramEvents for the given
            StatePerson.
        inclusions: A dictionary containing the following keys that correspond
            to characteristic dimensions:
                - age_bucket
                - ethnicity
                - gender
                - race
            Where the values are boolean flags indicating whether to include
            the dimension in the calculations.
    Returns:
        A list of key-value tuples representing specific metric combinations and
        the value corresponding to that metric.
    """

    metrics: List[Tuple[Dict[str, Any], Any]] = []

    event_based_metrics: List[Tuple[Dict[str, Any], Any]] = []

    for program_event in program_events:
        if isinstance(program_event, ProgramReferralEvent):
            characteristic_combos = characteristic_combinations(
                person, program_event, inclusions)

            program_referral_metrics_event_based = map_metric_combinations(
                characteristic_combos, program_event,
                ProgramMetricType.REFERRAL
            )

            event_based_metrics.extend(program_referral_metrics_event_based)

    metrics.extend(event_based_metrics)

    # Convert the event-based population metrics to person-based
    person_based_metrics = convert_event_based_to_person_based_metrics(
        deepcopy(event_based_metrics)
    )

    metrics.extend(person_based_metrics)

    return metrics


def characteristic_combinations(person: StatePerson,
                                program_event: ProgramEvent,
                                inclusions: Dict[str, bool]) -> \
        List[Dict[str, Any]]:
    """Calculates all program metric combinations.

    Returns the list of all combinations of the metric characteristics, of all
    sizes, given the StatePerson and ProgramEvent. That is, this
    returns a list of dictionaries where each dictionary is a combination of 0
    to n unique elements of characteristics, where n is the number of keys in
    the given inclusions dictionary that are set to True + the dimensions for
    the given type of event.

    For each event, we need to calculate metrics across combinations of:
    MetricMethodologyType (Event-based, Person-based);
    Demographics (age, race, ethnicity, gender);

    Methodology is not included in the output here. It is added into augmented
    versions of these combinations later.

    Args:
        person: the StatePerson we are picking characteristics from
        program_event: the ProgramEvent we are picking
            characteristics from
        inclusions: A dictionary containing the following keys that correspond
            to characteristic dimensions:
                - age_bucket
                - ethnicity
                - gender
                - race
            Where the values are boolean flags indicating whether to include
            the dimension in the calculations.

    Returns:
        A list of dictionaries containing all unique combinations of
        characteristics.
    """

    characteristics: Dict[str, Any] = {}

    if isinstance(program_event,
                  ProgramReferralEvent):
        if program_event.supervision_type:
            characteristics['supervision_type'] = \
                program_event.supervision_type
        if program_event.assessment_score and program_event.assessment_type:
            characteristics['assessment_score_bucket'] = \
                assessment_score_bucket(program_event.assessment_score,
                                        program_event.assessment_type)
            characteristics['assessment_type'] = \
                program_event.assessment_type
        if program_event.supervising_officer_external_id:
            characteristics['supervising_officer_external_id'] = \
                program_event.supervising_officer_external_id
        if program_event.supervising_district_external_id:
            characteristics['supervising_district_external_id'] = \
                program_event.supervising_district_external_id

    if program_event.program_id:
        characteristics['program_id'] = \
            program_event.program_id

    if inclusions.get('age_bucket'):
        year = program_event.year
        month = program_event.month

        if month is None:
            month = 1

        start_of_bucket = date(year, month, 1)
        entry_age = age_at_date(person, start_of_bucket)
        entry_age_bucket = age_bucket(entry_age)
        if entry_age_bucket is not None:
            characteristics['age_bucket'] = entry_age_bucket
    if inclusions.get('gender'):
        if person.gender is not None:
            characteristics['gender'] = person.gender
    if person.races or person.ethnicities:
        if inclusions.get('race'):
            races = person.races
        else:
            races = []

        if inclusions.get('ethnicity'):
            ethnicities = person.ethnicities
        else:
            ethnicities = []

        return for_characteristics_races_ethnicities(
            races, ethnicities, characteristics)

    return for_characteristics(characteristics)


def map_metric_combinations(
        characteristic_combos: List[Dict[str, Any]],
        program_event: ProgramEvent,
        metric_type: ProgramMetricType) -> \
        List[Tuple[Dict[str, Any], Any]]:
    """Maps the given program event and characteristic combinations to a variety
    of metrics that track program interactions.

    All values will be 1 for these count metrics, because the presence
    of a ProgramEvent for a given event implies that the person interacted
    with the program in the way being described.

    Args:
        characteristic_combos: A list of dictionaries containing all unique
            combinations of characteristics.
        program_event: The program event from which the combination was derived.
        metric_type: The metric type to set on each combination

    Returns:
        A list of key-value tuples representing specific metric combinations and
        the metric value corresponding to that metric.
    """
    metrics = []

    year = program_event.year
    month = program_event.month
    state_code = program_event.state_code

    for combo in characteristic_combos:
        combo['metric_type'] = metric_type.value
        combo['state_code'] = state_code
        combo['year'] = year
        combo['month'] = month
        combo['methodology'] = MetricMethodologyType.EVENT

        metrics.append((combo, 1))

    return metrics


def assessment_score_bucket(assessment_score: int,
                            assessment_type: StateAssessmentType) -> \
        Optional[str]:
    """Calculates the assessment score bucket that applies to measurement.

    Args:
        assessment_score: the person's assessment score
        assessment_type: the type of assessment

    NOTE: Only LSIR buckets are currently supported
    TODO(2742): Add calculation support for all supported StateAssessmentTypes

    Returns:
        A string representation of the assessment score for the person.
        None if the assessment type is not supported.
    """
    if assessment_type == StateAssessmentType.LSIR:
        if assessment_score < 24:
            return '0-23'
        if assessment_score <= 29:
            return '24-29'
        if assessment_score <= 38:
            return '30-38'
        return '39+'

    logging.warning("Assessment type %s is unsupported.", assessment_type)

    return None
