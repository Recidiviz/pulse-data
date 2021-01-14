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
from collections import defaultdict
from datetime import date
from typing import List, Dict

from freezegun import freeze_time

from recidiviz.calculator.pipeline.incarceration.calculator import EVENT_TO_METRIC_TYPES
from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationEvent, IncarcerationAdmissionEvent,\
    IncarcerationReleaseEvent, IncarcerationStayEvent
from recidiviz.calculator.pipeline.incarceration import calculator
from recidiviz.calculator.pipeline.incarceration.metrics import IncarcerationMetricType, IncarcerationAdmissionMetric, \
    IncarcerationPopulationMetric, IncarcerationReleaseMetric
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason, StateSpecializedPurposeForIncarceration, \
    StateIncarcerationPeriodAdmissionReason, StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonRace, StatePersonEthnicity

ALL_METRICS_INCLUSIONS_DICT = {
    IncarcerationMetricType.INCARCERATION_ADMISSION: True,
    IncarcerationMetricType.INCARCERATION_POPULATION: True,
    IncarcerationMetricType.INCARCERATION_RELEASE: True
}

CALCULATION_METHODOLOGIES = len(MetricMethodologyType)
_COUNTY_OF_RESIDENCE = 'county'
_STATUTE = 'XXXX'
_NCIC_CODE = '1234'

_DEFAULT_PERSON_METADATA = PersonMetadata(prioritized_race_or_ethnicity='BLACK')


class TestMapIncarcerationCombinations(unittest.TestCase):
    """Tests the map_incarceration_combinations function."""

    def test_map_incarceration_combinations(self):
        person = StatePerson.new_with_defaults(
            state_code='CA', person_id=12345, birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

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
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2000-03',
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2000

    def test_map_incarceration_combinations_all_types(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345,
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
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
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
                supervision_type_at_release=StateSupervisionPeriodSupervisionType.PAROLE
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

    def test_map_incarceration_combinations_two_admissions_same_month(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345, birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

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
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

    def test_map_incarceration_combinations_two_releases_same_month(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345, birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

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
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

    @freeze_time('2020-01-01')
    def test_map_incarceration_combinations_two_stays_same_month(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345, birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

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
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
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
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)


    def test_map_incarceration_combinations_two_stays_same_month_facility(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345, birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

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
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 3, 31),
                facility='FACILITY 18',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                admission_reason_raw_text='PAROLE_REVOCATION',
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)

    def test_map_incarceration_combinations_multiple_stays(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345,
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
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 4, 30),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 5, 31),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

    def test_map_incarceration_combinations_multiple_stays_one_month(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345,
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
                event_date=date(2010, 3, 13),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 3, 14),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 3, 15),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

    def test_map_incarceration_combinations_multiple_overlapping_stays(self):
        person = StatePerson.new_with_defaults(state_code='US_ND', person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationStayEvent(
                state_code='US_ND',
                event_date=date(2019, 11, 30),
                facility='JRCC',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            IncarcerationStayEvent(
                state_code='US_ND',
                event_date=date(2019, 11, 30),
                facility='JRCC',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            IncarcerationStayEvent(
                state_code='US_ND',
                event_date=date(2019, 11, 30),
                facility='JRCC',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)

    @freeze_time('2000-03-30')
    def test_map_incarceration_combinations_calculation_month_count(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345,
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
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2000-03',
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(
            incarceration_events)

        self.assertEqual(expected_combinations_count, len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)
        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2000

    @freeze_time('2000-03-30')
    def test_map_incarceration_combinations_calculation_month_count_exclude(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345,
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
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )


        self.assertEqual(0, len(incarceration_combinations))

    @freeze_time('2000-03-30')
    def test_map_incarceration_combinations_calculation_month_count_include_one(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345,
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
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=36,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count([incarceration_event_include])

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)
        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2000

    @freeze_time('2010-12-31')
    def test_map_incarceration_combinations_calculation_month_count_include_monthly(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345,
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
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=37,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value
                   in incarceration_combinations)
        for combo, _ in incarceration_combinations:
            assert combo.get('year') == 2007

    def test_map_incarceration_combinations_includes_statute_output(self):
        person = StatePerson.new_with_defaults(state_code='CA', person_id=12345,
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
                most_serious_offense_statute=_STATUTE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 4, 30),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code=_NCIC_CODE,
                most_serious_offense_statute=_STATUTE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ),
            IncarcerationStayEvent(
                state_code='CA',
                event_date=date(2010, 5, 31),
                facility='FACILITY 33',
                county_of_residence=_COUNTY_OF_RESIDENCE,
                most_serious_offense_ncic_code=_NCIC_CODE,
                most_serious_offense_statute=_STATUTE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            )
        ]

        incarceration_combinations = calculator.map_incarceration_combinations(
            person=person,
            incarceration_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA
        )

        expected_combinations_count = expected_metric_combos_count(incarceration_events)

        self.assertEqual(expected_combinations_count,
                         len(incarceration_combinations))
        assert all(value == 1 for _combination, value in incarceration_combinations)
        assert all(combo.get('most_serious_offense_statute') is not None
                   for combo, value in incarceration_combinations if combo.get('person_id') is not None)


def expected_metric_combos_count(
        incarceration_events: List[IncarcerationEvent]) -> int:
    """Calculates the expected number of characteristic combinations given the incarceration events."""
    output_count_by_metric_type: Dict[IncarcerationMetricType, int] = defaultdict(int)

    for event_type, metric_type in EVENT_TO_METRIC_TYPES.items():
        output_count_by_metric_type[metric_type] += len([
                event for event in incarceration_events
                if isinstance(event, event_type)
            ])

    return sum(value for value in output_count_by_metric_type.values())
