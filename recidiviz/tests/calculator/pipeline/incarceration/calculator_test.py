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
"""Tests for incarceration/calculator.py."""
# pylint: disable=unused-import,wrong-import-order
import unittest
from datetime import date
from typing import List, Dict, Set, Tuple

from freezegun import freeze_time

from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationEvent, IncarcerationAdmissionEvent,\
    IncarcerationReleaseEvent, IncarcerationStayEvent
from recidiviz.calculator.pipeline.incarceration import calculator
from recidiviz.calculator.pipeline.incarceration.metrics import IncarcerationMetricType
from recidiviz.calculator.pipeline.utils import calculator_utils
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason, StateSpecializedPurposeForIncarceration, \
    StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonRace, StatePersonEthnicity
from recidiviz.tests.calculator.calculator_test_utils import \
    demographic_metric_combos_count_for_person, combo_has_enum_value_for_key

ALL_INCLUSIONS_DICT = {
    'age_bucket': True,
    'gender': True,
    'race': True,
    'ethnicity': True,
}

CALCULATION_METHODOLOGIES = len(MetricMethodologyType)
_COUNTY_OF_RESIDENCE = 'county'
_STATUTE = 'XXXX'
_NCIC_CODE = '1234'


class TestMapIncarcerationCombinations(unittest.TestCase):
    """Tests the map_incarceration_combinations function."""

    # Freezing time to before the events so none of them fall into the
    # relevant metric periods
    @freeze_time('1900-01-01')
    def test_map_incarceration_combinations(self):
        person = StatePerson.new_with_defaults(person_id=12345, birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(state_code='CA', ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text='NEW_ADMISSION',
            supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PROBATION,
            event_date=date(2000, 3, 12),
            facility='SAN QUENTIN',
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [incarceration_event]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2000

    @freeze_time('1900-01-01')
    def test_map_incarceration_combinations_all_types(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationStayEvent(
                admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                admission_reason_raw_text='NEW_ADMISSION',
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PAROLE,
                state_code='CA',
                event_date=date(2000, 3, 31),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code=_NCIC_CODE,
                most_serious_offense_statute=_STATUTE,
            ),
            IncarcerationAdmissionEvent(
                state_code='CA',
                event_date=date(2000, 3, 12),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                supervision_type_at_admission=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason_raw_text='PAROLE_REVOCATION',
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            IncarcerationReleaseEvent(
                state_code='CA',
                event_date=date(2003, 4, 12),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

    @freeze_time('1900-01-01')
    def test_map_incarceration_combinations_two_admissions_same_month(self):
        person = StatePerson.new_with_defaults(person_id=12345, birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(state_code='CA', ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationAdmissionEvent(
                state_code='CA',
                event_date=date(2000, 3, 12),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.NEW_ADMISSION
            ),
            IncarcerationAdmissionEvent(
                state_code='CA',
                event_date=date(2000, 3, 17),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.NEW_ADMISSION
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

    @freeze_time('1900-01-01')
    def test_map_incarceration_combinations_two_releases_same_month(self):
        person = StatePerson.new_with_defaults(person_id=12345, birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(state_code='CA', ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationReleaseEvent(
                state_code='CA',
                event_date=date(2010, 3, 12),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationReleaseEvent(
                state_code='CA',
                event_date=date(2010, 3, 24),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

    @freeze_time('1900-01-01')
    def test_map_incarceration_combinations_two_stays_same_month(self):
        person = StatePerson.new_with_defaults(person_id=12345, birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(state_code='CA', ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 3, 31),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code=_NCIC_CODE,
                most_serious_offense_statute=_STATUTE,
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                admission_reason_raw_text='PAROLE_REVOCATION',
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 3, 31),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code=_NCIC_CODE,
                most_serious_offense_statute=_STATUTE,
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                admission_reason_raw_text='PAROLE_REVOCATION',
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

    @freeze_time('1900-01-01')
    def test_map_incarceration_combinations_two_stays_same_month_facility(self):
        person = StatePerson.new_with_defaults(person_id=12345, birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 3, 31),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code=_NCIC_CODE,
                most_serious_offense_statute=_STATUTE,
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                admission_reason_raw_text='PAROLE_REVOCATION',
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 3, 31),
                facility='FACILITY 18',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                admission_reason_raw_text='PAROLE_REVOCATION',

            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

    @freeze_time('1900-01-01')
    def test_map_incarceration_combinations_multiple_stays(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 3, 31),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 4, 30),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 5, 31),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

    @freeze_time('1900-01-01')
    def test_map_incarceration_combinations_multiple_overlapping_stays(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationStayEvent(
                state_code='US_ND',
                event_date=date(2019, 11, 30),
                facility='JRCC',
                county_of_residence=_COUNTY_OF_RESIDENCE
            ),
            IncarcerationStayEvent(
                state_code='US_ND',
                event_date=date(2019, 11, 30),
                facility='JRCC',
                county_of_residence=_COUNTY_OF_RESIDENCE
            ),
            IncarcerationStayEvent(
                state_code='US_ND',
                event_date=date(2019, 11, 30),
                facility='JRCC',
                county_of_residence=_COUNTY_OF_RESIDENCE
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

    @freeze_time('2000-3-20')
    def test_map_incarceration_combinations_admission_relevant_periods(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            event_date=date(2000, 3, 12),
            facility='SAN QUENTIN',
            county_of_residence=_COUNTY_OF_RESIDENCE,
            admission_reason=AdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
        )

        incarceration_events = [incarceration_event]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT,
            len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)
        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2000
            if combo.get('person_id') is not None:
                assert combo.get('admission_date') is not None

    @freeze_time('2010-10-20')
    def test_map_incarceration_combinations_release_relevant_periods(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationReleaseEvent(
            state_code='CA',
            event_date=date(2010, 10, 2),
            facility='SAN QUENTIN',
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [incarceration_event]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT,
            0, len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2010

    @freeze_time('2010-10-20')
    def test_map_incarceration_combinations_relevant_periods(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationAdmissionEvent(
                state_code='CA',
                event_date=date(2010, 10, 2),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationReleaseEvent(
                state_code='CA',
                event_date=date(2010, 10, 19),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT,
            len(calculator_utils.METRIC_PERIOD_MONTHS),
            len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2010

    @freeze_time('2010-10-20')
    def test_map_incarceration_combinations_relevant_periods_duplicates(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationAdmissionEvent(
                state_code='CA',
                event_date=date(2010, 10, 2),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationAdmissionEvent(
                state_code='CA',
                event_date=date(2010, 10, 19),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT,
            len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2010

    @freeze_time('2010-10-20')
    def test_map_incarceration_combinations_relevant_periods_revocations(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationAdmissionEvent(
                state_code='CA',
                event_date=date(2010, 10, 2),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.PAROLE_REVOCATION
            ),
            IncarcerationAdmissionEvent(
                state_code='CA',
                event_date=date(2010, 10, 19),
                facility='SAN QUENTIN',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.NEW_ADMISSION
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT,
            len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2010
        for combo, _ in incarceration_combinations:
            if combo.get('admission_reason')\
                    and combo_has_enum_value_for_key(
                            combo, 'metric_type', MetricMethodologyType.PERSON):
                # Ensure that all person-based metrics have the parole
                # revocation admission reason on them
                assert combo.get('admission_reason') == \
                       AdmissionReason.PAROLE_REVOCATION

    @freeze_time('2000-03-30')
    def test_map_incarceration_combinations_calculation_month_limit(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            event_date=date(2000, 3, 12),
            facility='SAN QUENTIN',
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [incarceration_event]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT,
            num_relevant_periods_admissions=len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)
        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2000

    @freeze_time('2000-03-30')
    def test_map_incarceration_combinations_calculation_month_limit_exclude(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            event_date=date(1990, 3, 12),
            facility='SAN QUENTIN',
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [incarceration_event]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=1
        )

        self.assertEqual(0, len(incarceration_combinations))

    @freeze_time('2000-03-30')
    def test_map_incarceration_combinations_calculation_month_limit_include_one(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_event_include = IncarcerationAdmissionEvent(
            state_code='CA',
            event_date=date(2000, 3, 12),
            facility='SAN QUENTIN',
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_event_exclude = IncarcerationAdmissionEvent(
            state_code='CA',
            event_date=date(1994, 3, 12),
            facility='SAN QUENTIN',
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [incarceration_event_include, incarceration_event_exclude]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=36
        )

        expected_combinations_count = expected_metric_combos_count(
            person, [incarceration_event_include], ALL_INCLUSIONS_DICT,
            num_relevant_periods_admissions=len(calculator_utils.METRIC_PERIOD_MONTHS))

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)
        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2000

    @freeze_time('2010-12-31')
    def test_map_incarceration_combinations_calculation_month_limit_include_monthly(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            event_date=date(2007, 12, 12),
            facility='SAN QUENTIN',
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [incarceration_event]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=37
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT,)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)
        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2007

    @freeze_time('1900-01-01')
    def test_map_incarceration_combinations_includes_statute_output(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 3, 31),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code=_NCIC_CODE,
                most_serious_offense_statute=_STATUTE
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 4, 30),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code=_NCIC_CODE,
                most_serious_offense_statute=_STATUTE
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 5, 31),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code=_NCIC_CODE,
                most_serious_offense_statute=_STATUTE
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            inclusions=ALL_INCLUSIONS_DICT,
            calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, incarceration_events, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)
        assert all(combo.get('most_serious_offense_statute') is not None
                   for combo, value in incarceration_combinations if combo.get('person_id') is not None)


class TestCharacteristicCombinations(unittest.TestCase):
    """Tests the characteristic_combinations function."""

    def test_characteristic_combinations(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            event_date=date(2018, 9, 13),
            facility='SAN QUENTIN',
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        combinations = calculator.characteristic_combinations(
            person, incarceration_event, ALL_INCLUSIONS_DICT, IncarcerationMetricType.ADMISSION)

        # 64 combinations of demographics + 1 person-level metric
        assert len(combinations) == 65

    def test_characteristic_combinations_no_facility(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            event_date=date(2018, 9, 13),
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        combinations = calculator.characteristic_combinations(
            person, incarceration_event, ALL_INCLUSIONS_DICT, IncarcerationMetricType.POPULATION)

        # 32 combinations of demographics + 1 person-level metric
        assert len(combinations) == 33

    def test_characteristic_combinations_no_county(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='CA',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='CA',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationAdmissionEvent(
            state_code='CA',
            event_date=date(2018, 9, 13),
            facility='facility',
        )

        combinations = calculator.characteristic_combinations(
            person, incarceration_event, ALL_INCLUSIONS_DICT, IncarcerationMetricType.RELEASE)

        # 32 combinations of demographics + 1 person-level metric
        assert len(combinations) == 33


def demographic_metric_combos_count_for_person_incarceration(
        person: StatePerson,
        inclusions: Dict[str, bool]) -> int:
    """Returns the number of possible demographic metric combinations for a
    given person, given the metric inclusions list."""

    total_metric_combos = demographic_metric_combos_count_for_person(
        person, inclusions
    )

    return total_metric_combos


def expected_metric_combos_count(
        person: StatePerson,
        incarceration_events: List[IncarcerationEvent],
        inclusions: Dict[str, bool],
        num_relevant_periods_admissions: int = 0,
        num_relevant_periods_releases: int = 0,
        with_methodologies: bool = True) -> int:
    """Calculates the expected number of characteristic combinations given the person, the incarceration events, and
    the dimensions that should be included in the explosion of feature combinations."""

    demographic_metric_combos = demographic_metric_combos_count_for_person_incarceration(
        person, inclusions)

    # Some test cases above use a different call that doesn't take methodology into account as a dimension
    methodology_multiplier = 1
    if with_methodologies:
        methodology_multiplier *= CALCULATION_METHODOLOGIES

    stay_events = [event for event in incarceration_events if isinstance(event, IncarcerationStayEvent)]

    num_stay_events = len(stay_events)
    num_duplicated_stay_months = 0
    stay_months: Set[Tuple[int, int]] = set()

    for stay_event in stay_events:
        if (stay_event.event_date.year, stay_event.event_date.month) in stay_months:
            num_duplicated_stay_months += 1

        stay_months.add((stay_event.event_date.year, stay_event.event_date.month))

    stay_dimension_multiplier = 1
    if stay_events:
        first_stay_event = stay_events[0]

        stay_dimensions = [
            'facility',
            'county_of_residence',
            'admission_reason',
            'supervision_type_at_admission',
        ]

        for dimension in stay_dimensions:
            if getattr(first_stay_event, dimension) is not None:
                stay_dimension_multiplier *= 2

    admission_events = [event for event in incarceration_events if isinstance(event, IncarcerationAdmissionEvent)]

    num_admission_events = len(admission_events)
    num_duplicated_admission_months = 0
    admission_months: Set[Tuple[int, int]] = set()

    for admission_event in admission_events:
        if (admission_event.event_date.year, admission_event.event_date.month) in admission_months:
            num_duplicated_admission_months += 1

        admission_months.add((admission_event.event_date.year, admission_event.event_date.month))

    admission_dimension_multiplier = 1
    if admission_events:
        first_admission_event = admission_events[0]

        admission_dimensions = [
            'facility',
            'county_of_residence',
            'admission_reason',
            'specialized_purpose_for_incarceration',
            'supervision_type_at_admission',
        ]

        for dimension in admission_dimensions:
            if getattr(first_admission_event, dimension) is not None:
                admission_dimension_multiplier *= 2

    release_events = [event for event in incarceration_events if isinstance(event, IncarcerationReleaseEvent)]

    num_release_events = len(release_events)
    num_duplicated_release_months = 0
    release_months: Set[Tuple[int, int]] = set()

    for release_event in release_events:
        if (release_event.event_date.year, release_event.event_date.month) in release_months:
            num_duplicated_release_months += 1

        release_months.add((release_event.event_date.year, release_event.event_date.month))

    release_dimension_multiplier = 1
    if release_events:
        first_release_event = release_events[0]

        release_dimensions = [
            'facility',
            'county_of_residence',
            'release_reason'
        ]

        for dimension in release_dimensions:
            if getattr(first_release_event, dimension) is not None:
                release_dimension_multiplier *= 2

    admission_combos = (demographic_metric_combos * methodology_multiplier *
                        num_admission_events * admission_dimension_multiplier)
    stay_combos = (demographic_metric_combos * methodology_multiplier *
                   num_stay_events * stay_dimension_multiplier)
    release_combos = (demographic_metric_combos * methodology_multiplier *
                      num_release_events * release_dimension_multiplier)

    if num_relevant_periods_admissions > 0:
        admission_combos += (demographic_metric_combos *
                             (len(admission_events) - num_duplicated_admission_months) *
                             num_relevant_periods_admissions * admission_dimension_multiplier)

    if num_relevant_periods_releases > 0:
        release_combos += (demographic_metric_combos *
                           (len(release_events) - num_duplicated_release_months) *
                           num_relevant_periods_releases * release_dimension_multiplier)

    duplicated_admission_combos = int(demographic_metric_combos * num_duplicated_admission_months
                                      * admission_dimension_multiplier)

    duplicated_release_combos = int(demographic_metric_combos * num_duplicated_release_months *
                                    release_dimension_multiplier)

    duplicated_stay_combos = int(demographic_metric_combos * num_duplicated_stay_months *
                                 stay_dimension_multiplier)

    admission_combos -= duplicated_admission_combos
    stay_combos -= duplicated_stay_combos
    release_combos -= duplicated_release_combos

    admission_combos += (num_admission_events +
                         (num_admission_events -
                          num_duplicated_admission_months)*(num_relevant_periods_admissions + 1))

    stay_combos += (num_stay_events + (num_stay_events - num_duplicated_stay_months))

    release_combos += (num_release_events +
                       (num_release_events - num_duplicated_release_months)*(num_relevant_periods_releases + 1))

    return admission_combos + stay_combos + release_combos
