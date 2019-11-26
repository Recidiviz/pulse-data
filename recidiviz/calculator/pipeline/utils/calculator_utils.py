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
from datetime import date
from itertools import combinations
from typing import Optional, List, Any, Dict

from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonRace, StatePersonEthnicity


def for_characteristics_races_ethnicities(
        races: List[StatePersonRace], ethnicities: List[StatePersonEthnicity],
        characteristics: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Produces the list of all combinations of the given metric
    characteristics, given the fact that there can be multiple races and
    ethnicities present.

    For example, this function call:
        for_characteristics_races_ethnicities(races: [Race.BLACK, Race.WHITE],
        ethnicities: [Ethnicity.HISPANIC, Ethnicity.NOT_HISPANIC],
        characteristics: {'gender': Gender.FEMALE, 'age': '<25'})

    First computes all combinations for the given characteristics. Then, for
    each race present, adds a copy of each combination augmented with the race.
    For each ethnicity present, adds a copy of each combination augmented with
    the ethnicity. Finally, for every combination of race and ethnicity, adds a
    copy of each combination augmented with both the race and the ethnicity.
    """
    # Initial combinations
    combos: List[Dict[str, Any]] = for_characteristics(characteristics)

    # Race additions
    race_combos: List[Dict[Any, Any]] = []
    for race_object in races:
        for combo in combos:
            augmented_combo = combo.copy()
            augmented_combo['race'] = race_object.race
            race_combos.append(augmented_combo)

    # Ethnicity additions
    ethnicity_combos: List[Dict[Any, Any]] = []
    for ethnicity_object in ethnicities:
        for combo in combos:
            augmented_combo = combo.copy()
            augmented_combo['ethnicity'] = ethnicity_object.ethnicity
            ethnicity_combos.append(augmented_combo)

    # Multi-race and ethnicity additions
    race_ethnicity_combos: List[Dict[Any, Any]] = []
    for race_object in races:
        for ethnicity_object in ethnicities:
            for combo in combos:
                augmented_combo = combo.copy()
                augmented_combo['race'] = race_object.race
                augmented_combo['ethnicity'] = ethnicity_object.ethnicity
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
