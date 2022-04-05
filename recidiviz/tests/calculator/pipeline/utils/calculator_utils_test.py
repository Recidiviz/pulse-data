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
"""Tests for calculator_utils.py."""
import unittest
from datetime import date
from datetime import datetime

from recidiviz.calculator.pipeline.utils import calculator_utils
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response import \
    StateSupervisionViolationResponseDecision
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonRace, Race, StatePersonEthnicity, Ethnicity, StatePersonExternalId, StateSupervisionViolationTypeEntry


def test_for_characteristics():
    characteristics = {'race': 'black', 'gender': 'female', 'age': '<25'}
    combinations = calculator_utils.for_characteristics(characteristics)

    assert combinations == [{},
                            {'race': 'black'},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'race': 'black', 'gender': 'female'},
                            {'age': '<25', 'race': 'black'},
                            {'age': '<25', 'gender': 'female'},
                            {'age': '<25', 'race': 'black', 'gender': 'female'}]


def test_for_characteristics_one_characteristic():
    characteristics = {'gender': 'male'}
    combinations = calculator_utils.for_characteristics(characteristics)

    assert combinations == [{}, {'gender': 'male'}]


def test_for_characteristics_races_ethnicities_one_race():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    characteristics = {'gender': 'female', 'age': '<25'}

    combinations = calculator_utils.for_characteristics_races_ethnicities(
        [race_white], [], characteristics)

    assert combinations == [{},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'gender': 'female', 'age': '<25'},
                            {'race': Race.WHITE},
                            {'race': Race.WHITE, 'gender': 'female'},
                            {'age': '<25', 'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE,
                             'gender': 'female'}]


def test_for_characteristics_races_ethnicities_two_races():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    race_black = StatePersonRace.new_with_defaults(state_code='MT',
                                                   race=Race.BLACK)

    characteristics = {'gender': 'female', 'age': '<25'}

    combinations = calculator_utils.for_characteristics_races_ethnicities(
        [race_white, race_black], [], characteristics)

    assert combinations == [{},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'gender': 'female', 'age': '<25'},
                            {'race': Race.WHITE},
                            {'race': Race.WHITE, 'gender': 'female'},
                            {'age': '<25', 'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE,
                             'gender': 'female'},
                            {'race': Race.BLACK},
                            {'race': Race.BLACK, 'gender': 'female'},
                            {'age': '<25', 'race': Race.BLACK},
                            {'age': '<25', 'race': Race.BLACK,
                             'gender': 'female'}]


def test_for_characteristics_races_ethnicities_one_ethnicity():
    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    characteristics = {'gender': 'female', 'age': '<25'}

    combinations = calculator_utils.for_characteristics_races_ethnicities(
        [], [ethnicity_hispanic], characteristics)

    assert combinations == [{},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'gender': 'female', 'age': '<25'},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'}]


def test_for_characteristics_races_ethnicities_multiple_ethnicities():
    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    ethnicity_not_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='FL',
        ethnicity=Ethnicity.NOT_HISPANIC)

    characteristics = {'gender': 'female', 'age': '<25'}

    combinations = calculator_utils.for_characteristics_races_ethnicities(
        [], [ethnicity_hispanic, ethnicity_not_hispanic], characteristics)

    assert combinations == [{},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'gender': 'female', 'age': '<25'},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'ethnicity': Ethnicity.NOT_HISPANIC,
                             'gender': 'female'},
                            {'age': '<25', 'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.NOT_HISPANIC,
                             'gender': 'female'}
                            ]


def test_for_characteristics_races_ethnicities_one_race_one_ethnicity():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    characteristics = {'gender': 'female', 'age': '<25'}

    combinations = calculator_utils.for_characteristics_races_ethnicities(
        [race_white], [ethnicity_hispanic], characteristics)

    assert combinations == [{},
                            {'gender': 'female'},
                            {'age': '<25'},
                            {'gender': 'female', 'age': '<25'},
                            {'race': Race.WHITE},
                            {'race': Race.WHITE, 'gender': 'female'},
                            {'age': '<25', 'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE,
                             'gender': 'female'},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC,
                             'gender': 'female'},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC,
                             'age': '<25',
                             'gender': 'female'},
                            ]


def test_for_characteristics_races_ethnicities_multiple_races_one_ethnicity():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    race_black = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.BLACK)

    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    characteristics = {'age': '<25'}

    combinations = calculator_utils.for_characteristics_races_ethnicities(
        [race_white, race_black], [ethnicity_hispanic], characteristics)

    assert combinations == [{},
                            {'age': '<25'},
                            {'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE},
                            {'race': Race.BLACK},
                            {'age': '<25', 'race': Race.BLACK},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.BLACK,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'race': Race.BLACK,
                             'ethnicity': Ethnicity.HISPANIC}
                            ]


def test_for_characteristics_races_ethnicities_one_race_multiple_ethnicities():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    ethnicity_not_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.NOT_HISPANIC)

    characteristics = {'age': '<25'}

    combinations = calculator_utils.for_characteristics_races_ethnicities(
        [race_white], [ethnicity_hispanic, ethnicity_not_hispanic],
        characteristics)

    assert combinations == [{},
                            {'age': '<25'},
                            {'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.NOT_HISPANIC}
                            ]


def test_for_characteristics_races_ethnicities_no_races_or_ethnicities():
    characteristics = {'age': '<25'}

    combinations = calculator_utils.for_characteristics_races_ethnicities(
        [], [], characteristics)

    assert combinations == [{}, {'age': '<25'}]


def test_for_characteristics_races_ethnicities_multiple_races_and_ethnicities():
    race_white = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.WHITE)

    race_black = StatePersonRace.new_with_defaults(state_code='CA',
                                                   race=Race.BLACK)

    ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.HISPANIC)

    ethnicity_not_hispanic = StatePersonEthnicity.new_with_defaults(
        state_code='CA',
        ethnicity=Ethnicity.NOT_HISPANIC)

    characteristics = {'age': '<25'}

    combinations = calculator_utils.for_characteristics_races_ethnicities(
        [race_white, race_black], [ethnicity_hispanic, ethnicity_not_hispanic],
        characteristics)

    assert combinations == [{},
                            {'age': '<25'},
                            {'race': Race.WHITE},
                            {'age': '<25', 'race': Race.WHITE},
                            {'race': Race.BLACK},
                            {'age': '<25', 'race': Race.BLACK},
                            {'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.HISPANIC},
                            {'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.WHITE,
                             'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'race': Race.WHITE,
                             'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'race': Race.BLACK,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'age': '<25', 'race': Race.BLACK,
                             'ethnicity': Ethnicity.HISPANIC},
                            {'race': Race.BLACK,
                             'ethnicity': Ethnicity.NOT_HISPANIC},
                            {'age': '<25', 'race': Race.BLACK,
                             'ethnicity': Ethnicity.NOT_HISPANIC}
                            ]


def test_age_at_date_earlier_month():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 4, 15)
    person = StatePerson.new_with_defaults(birthdate=birthdate)

    assert calculator_utils.age_at_date(person, check_date) == 24


def test_age_at_date_same_month_earlier_date():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 6, 16)
    person = StatePerson.new_with_defaults(birthdate=birthdate)

    assert calculator_utils.age_at_date(person, check_date) == 24


def test_age_at_date_same_month_same_date():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 6, 17)
    person = StatePerson.new_with_defaults(birthdate=birthdate)

    assert calculator_utils.age_at_date(person, check_date) == 25


def test_age_at_date_same_month_later_date():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 6, 18)
    person = StatePerson.new_with_defaults(birthdate=birthdate)

    assert calculator_utils.age_at_date(person, check_date) == 25


def test_age_at_date_later_month():
    birthdate = date(1989, 6, 17)
    check_date = date(2014, 7, 11)
    person = StatePerson.new_with_defaults(birthdate=birthdate)

    assert calculator_utils.age_at_date(person, check_date) == 25


def test_age_at_date_birthdate_unknown():
    assert calculator_utils.age_at_date(
        StatePerson.new_with_defaults(), datetime.today()) is None


def test_age_bucket():
    assert calculator_utils.age_bucket(24) == '<25'
    assert calculator_utils.age_bucket(27) == '25-29'
    assert calculator_utils.age_bucket(30) == '30-34'
    assert calculator_utils.age_bucket(39) == '35-39'
    assert calculator_utils.age_bucket(40) == '40<'


def test_augment_combination():
    combo = {'age': '<25', 'race': 'black', 'gender': 'female'}

    parameters = {'A': 'a', 'B': 9}

    augmented = calculator_utils.augment_combination(combo, parameters)

    assert augmented == {'age': '<25',
                         'A': 'a', 'B': 9,
                         'race': 'black',
                         'gender': 'female'}
    assert augmented != combo


class TestRelevantMetricPeriods(unittest.TestCase):
    """Tests the relevant_metric_periods function."""

    def test_relevant_metric_periods_all_periods(self):
        """Tests the relevant_metric_periods function when all metric periods
        are relevant."""
        event_date = date(2020, 1, 3)
        end_year = 2020
        end_month = 1

        relevant_periods = calculator_utils.relevant_metric_periods(
            event_date, end_year, end_month)

        expected_periods = [36, 12, 6, 3]

        self.assertEqual(expected_periods, relevant_periods)

    def test_relevant_metric_periods_all_after_3(self):
        """Tests the relevant_metric_periods function when all metric periods
        are relevant except the 3 month period."""
        event_date = date(2020, 1, 3)

        end_year = 2020
        end_month = 6

        relevant_periods = calculator_utils.relevant_metric_periods(
            event_date, end_year, end_month)

        expected_periods = [36, 12, 6]

        self.assertEqual(expected_periods, relevant_periods)

    def test_relevant_metric_periods_all_after_6(self):
        """Tests the relevant_metric_periods function when all metric periods
        are relevant except the 1, 3, and 6 month periods."""
        event_date = date(2007, 2, 3)

        end_year = 2008
        end_month = 1

        relevant_periods = calculator_utils.relevant_metric_periods(
            event_date, end_year, end_month)

        expected_periods = [36, 12]

        self.assertEqual(expected_periods, relevant_periods)

    def test_relevant_metric_periods_only_36(self):
        """Tests the relevant_metric_periods function when only the 36 month
        period is relevant."""
        event_date = date(2006, 3, 3)

        end_year = 2008
        end_month = 1

        relevant_periods = calculator_utils.relevant_metric_periods(
            event_date, end_year, end_month)

        expected_periods = [36]

        self.assertEqual(expected_periods, relevant_periods)

    def test_relevant_metric_periods_none_relevant(self):
        """Tests the relevant_metric_periods function when no metric periods
        are relevant."""
        event_date = date(2001, 2, 23)

        end_year = 2008
        end_month = 1

        relevant_periods = calculator_utils.relevant_metric_periods(
            event_date, end_year, end_month)

        expected_periods = []

        self.assertEqual(expected_periods, relevant_periods)

    def test_relevant_metric_periods_end_of_month(self):
        """Tests the relevant_metric_periods function when the event is on
        the last day of the month of the end of the metric period."""
        event_date = date(2008, 1, 31)

        end_year = 2008
        end_month = 1

        relevant_periods = calculator_utils.relevant_metric_periods(
            event_date, end_year, end_month)

        expected_periods = [36, 12, 6, 3]

        self.assertEqual(expected_periods, relevant_periods)

    def test_relevant_metric_periods_first_of_month(self):
        """Tests the relevant_metric_periods function when the event is on
        the first day of the month of the 36 month period start."""
        event_date = date(2005, 2, 1)

        end_year = 2008
        end_month = 1

        relevant_periods = calculator_utils.relevant_metric_periods(
            event_date, end_year, end_month)

        expected_periods = [36]

        self.assertEqual(expected_periods, relevant_periods)


class TestPersonExternalIdToInclude(unittest.TestCase):
    """Tests the person_external_id_to_include function."""
    def test_person_external_id_to_include(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id='SID1341',
            id_type='US_MO_DOC',
            state_code='US_MO'
        )

        person.external_ids = [person_external_id]

        external_id = calculator_utils.person_external_id_to_include(person)

        self.assertEqual(external_id, person_external_id.external_id)

    def test_person_external_id_to_include_no_results(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id='SID10928',
            id_type='US_ND_SID',
            state_code='US_ND'
        )

        person.external_ids = [person_external_id]

        external_id = calculator_utils.person_external_id_to_include(person)

        self.assertIsNone(external_id)

    def test_person_external_id_to_include_multiple(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        person_external_id_exclude = StatePersonExternalId.new_with_defaults(
            external_id='SID10928',
            id_type='US_ND_SID',
            state_code='US_ND'
        )

        person_external_id_include = StatePersonExternalId.new_with_defaults(
            external_id='SID1341',
            id_type='US_MO_DOC',
            state_code='US_MO'
        )

        person.external_ids = [person_external_id_exclude, person_external_id_include]

        external_id = calculator_utils.person_external_id_to_include(person)

        self.assertEqual(external_id, person_external_id_include.external_id)


class TestIdentifyMostSevereResponseDecision(unittest.TestCase):
    def test_identify_most_severe_response_decision(self):
        decisions = [StateSupervisionViolationResponseDecision.CONTINUANCE,
                     StateSupervisionViolationResponseDecision.REVOCATION]

        most_severe_decision = calculator_utils.identify_most_severe_response_decision(decisions)

        self.assertEqual(most_severe_decision, StateSupervisionViolationResponseDecision.REVOCATION)

    def test_identify_most_severe_response_decision_test_all_types(self):
        for decision in StateSupervisionViolationResponseDecision:
            decisions = [decision]

            most_severe_decision = calculator_utils.identify_most_severe_response_decision(decisions)

            self.assertEqual(most_severe_decision, decision)


class TestIdentifyMostSevereViolationType(unittest.TestCase):
    def test_identify_most_severe_violation_type(self):
        violation_type_entries = [
            StateSupervisionViolationTypeEntry.new_with_defaults(
                violation_type=StateSupervisionViolationType.TECHNICAL
            ),
            StateSupervisionViolationTypeEntry.new_with_defaults(
                violation_type=StateSupervisionViolationType.FELONY
            )
        ]

        most_severe_violation_type = calculator_utils.identify_most_severe_violation_type(violation_type_entries)

        self.assertEqual(most_severe_violation_type, StateSupervisionViolationType.FELONY)

    def test_identify_most_severe_violation_type_test_all_types(self):
        for violation_type in StateSupervisionViolationType:
            violation_type_entries = [
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=violation_type)
            ]

            most_severe_violation_type = calculator_utils.identify_most_severe_violation_type(violation_type_entries)

            self.assertEqual(most_severe_violation_type, violation_type)
