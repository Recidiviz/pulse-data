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
"""Tests for supervision/calculator.py."""
# pylint: disable=unused-import,wrong-import-order
import unittest
from datetime import date
from typing import Dict, List, Tuple, Set

from freezegun import freeze_time

from recidiviz.calculator.pipeline.supervision import calculator
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetricType
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    NonRevocationReturnSupervisionTimeBucket, SupervisionTimeBucket, \
    RevocationReturnSupervisionTimeBucket,\
    ProjectedSupervisionCompletionBucket, SupervisionTerminationBucket
from recidiviz.calculator.pipeline.utils import calculator_utils
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodTerminationReason
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType as ViolationType, \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType as RevocationType, \
    StateSupervisionViolationResponseDecision
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonRace, StatePersonEthnicity, StatePersonExternalId
from recidiviz.tests.calculator.calculator_test_utils import \
    demographic_metric_combos_count_for_person, combo_has_enum_value_for_key

ALL_INCLUSIONS_DICT = {
        'age_bucket': True,
        'gender': True,
        'race': True,
        'ethnicity': True,
        SupervisionMetricType.ASSESSMENT_CHANGE.value: True,
        SupervisionMetricType.SUCCESS.value: True,
        SupervisionMetricType.REVOCATION.value: True,
        SupervisionMetricType.REVOCATION_ANALYSIS.value: True,
        SupervisionMetricType.POPULATION.value: True,
    }

CALCULATION_METHODOLOGIES = len(MetricMethodologyType)


# TODO(2732): Implement more full test coverage of the officer, district,
#  the supervision success functionality
class TestMapSupervisionCombinations(unittest.TestCase):
    """Tests the map_supervision_combinations function."""

    # Freezing time to before the events so none of them fall into the
    # relevant metric periods
    @freeze_time('1900-01-01')
    def test_map_supervision_combinations(self):
        """Tests the map_supervision_combinations function."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=None,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_assessment(self):
        """Tests the map_supervision_combinations function when there is
        assessment data present."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=3,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=None,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_exclude_assessment(self):
        """Tests the map_supervision_combinations function when there is
        assessment data present, but it should not be included for this state and pipeline type."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=3,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=None,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        # Remove the dimensional explosions for assessment_type and assessment_score_bucket
        expected_combinations_count = expected_combinations_count / 4

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_supervising_officer_district(self):
        """Tests the map_supervision_combinations function when there is
        supervising officer and district data present."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=3,
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.VERY_HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                supervising_officer_external_id='143',
                supervising_district_external_id='DISTRICT X'),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=None,
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.VERY_HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                supervising_officer_external_id='143',
                supervising_district_external_id='DISTRICT X')
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_revocation_combinations(self):
        """Tests the map_supervision_combinations function for a
        revocation month."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id='SID1341',
            id_type='US_MO_DOC',
            state_code='US_MO'
        )

        person.external_ids = [person_external_id]

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=3,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=12,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                revocation_type=RevocationType.SHOCK_INCARCERATION,
                source_violation_type=ViolationType.FELONY,
                most_severe_violation_type=ViolationType.FELONY,
                most_severe_violation_type_subtype='SUBTYPE',
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=10)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_revocation_analysis=True)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))

        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_supervision_success(self):
        """Tests the map_supervision_combinations function when there is a
        ProjectedSupervisionCompletionBucket."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=True,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=None,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_supervision_unsuccessful(self):
        """Tests the map_supervision_combinations function when there is a
        ProjectedSupervisionCompletionBucket and the supervision is not
        successfully completed."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=False,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=None,
                supervision_type=StateSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations if not
                   combo_has_enum_value_for_key(_combination, 'metric_type',
                                                SupervisionMetricType.SUCCESS))
        assert all(value == 0 for _combination, value
                   in supervision_combinations if
                   combo_has_enum_value_for_key(_combination, 'metric_type',
                                                SupervisionMetricType.SUCCESS))

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_supervision_mixed_success(self):
        """Tests the map_supervision_combinations function when there is a
        ProjectedSupervisionCompletionBucket and the supervision is not
        successfully completed."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PAROLE,
                successful_completion=False,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PROBATION,
                successful_completion=True,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=None,
                supervision_type=StateSupervisionType.PAROLE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            duplicated_months_different_supervision_types=True
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations if not
                   combo_has_enum_value_for_key(
                       _combination, 'metric_type',
                       SupervisionMetricType.SUCCESS) or
                   combo_has_enum_value_for_key(
                       _combination, 'supervision_type',
                       StateSupervisionType.PROBATION))
        assert all(value == 0 for _combination, value
                   in supervision_combinations if
                   combo_has_enum_value_for_key(
                       _combination, 'metric_type',
                       SupervisionMetricType.SUCCESS) and not
                   combo_has_enum_value_for_key(
                       _combination, 'supervision_type',
                       StateSupervisionType.PROBATION) and not
                   _combination.get('supervision_type') is None
                   )

    @freeze_time('2020-02-01')
    def test_map_supervision_combinations_supervision_with_district_officer(self):
        """Tests the map_supervision_combinations function when there is a mix of missing & non-null district/officer
        data for one person over many ProjectedSupervisionCompletionBuckets."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1993, 4, 2),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2017, month=6, supervision_type=StateSupervisionType.PROBATION,
                successful_completion=False,
            ),
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2020, month=1, supervision_type=StateSupervisionType.PROBATION,
                successful_completion=False,
                supervising_officer_external_id='officer45', supervising_district_external_id='district5'
            ),
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2018, month=12, supervision_type=StateSupervisionType.PROBATION,
                successful_completion=False
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
        )

        tested_metric_period_months = set()

        for combo, value in supervision_combinations:
            metric_period_months = combo.get('metric_period_months')

            # Track the metric period months where the officer & district data is set for the most recent 2020 metrics
            if ((combo.get('supervising_district_external_id') is not None)
                    and (combo.get('supervising_officer_external_id') is not None)):
                tested_metric_period_months.add(metric_period_months)

            self.assertEqual(0, value,
                             f'Expected supervision success value 0'
                             f' but got {value} for metric period months {metric_period_months}')

        # Validate the most recent 2020 metrics have officer & district for all metric period months
        self.assertEqual(len(tested_metric_period_months), len([1, 3, 6, 12, 36]))

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_revocation_and_not(self):
        """Tests the map_supervision_combinations function."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id='SID1341',
            id_type='US_MO_DOC',
            state_code='US_MO'
        )

        person.external_ids = [person_external_id]

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=4,
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=13,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                supervising_officer_external_id='OFFICER',
                supervising_district_external_id='DISTRICT',
                revocation_type=RevocationType.SHOCK_INCARCERATION,
                source_violation_type=ViolationType.FELONY,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=19
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=2,
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=13,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                supervising_officer_external_id='OFFICER',
                supervising_district_external_id='DISTRICT'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=13,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                supervising_officer_external_id='OFFICER',
                supervising_district_external_id='DISTRICT'
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_revocation_analysis=True)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))

        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_multiple_months(self):
        """Tests the map_supervision_combinations function where the person
        was on supervision for multiple months."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 4, StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 5, StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 6, StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, None, StateSupervisionType.PAROLE),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_overlapping_months(self):
        """Tests the map_supervision_combinations function where the person
        was serving multiple supervision sentences simultaneously in a given
        month."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 3, StateSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 3, StateSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, None, StateSupervisionType.PROBATION)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_overlapping_months_types(self):
        """Tests the map_supervision_combinations function where the person
        was serving multiple supervision sentences simultaneously in a given
        month, but the supervisions are of different types."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 3, StateSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, None, StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, None, StateSupervisionType.PROBATION)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            duplicated_months_different_supervision_types=True
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionType.PAROLE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            include_revocation_analysis=True
        )

        # Hack to get the right number. Subtracting the person-based metrics for the revocation analysis for
        # the month of the revocation and all relevant periods because there is no applicable external id.
        expected_combinations_count -= (len(calculator_utils.METRIC_PERIOD_MONTHS) + 1)

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_one_non_relevant_period(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        relevant_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2010, 1, StateSupervisionType.PAROLE)
        not_relevant_bucket = RevocationReturnSupervisionTimeBucket(
            'US_MO', 1980, 1, StateSupervisionType.PAROLE)

        supervision_time_buckets = [relevant_bucket, not_relevant_bucket]


        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )
        # Get the expected count for the relevant bucket without revocation analysis metrics
        expected_combinations_count = expected_metric_combos_count(
            person, [relevant_bucket], ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            include_revocation_analysis=False
        )
        # Get the expected count for the non-relevant revocation bucket with revocation analysis metrics
        expected_combinations_count_not_relevant = expected_metric_combos_count(
            person, [not_relevant_bucket], ALL_INCLUSIONS_DICT,
            include_revocation_analysis=True
        )
        expected_combinations_count += expected_combinations_count_not_relevant
        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_revocation(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id='SID1341',
            id_type='US_MO_DOC',
            state_code='US_MO'
        )

        person.external_ids = [person_external_id]

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionType.PAROLE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            include_revocation_analysis=True
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_duplicates(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionType.PAROLE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS)
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    # pylint:disable=line-too-long
    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_duplicate_success(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                supervision_type=StateSupervisionType.PAROLE,
                successful_completion=True
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                supervision_type=StateSupervisionType.PAROLE,
                successful_completion=False
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS)
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(not value for _combination, value
                   in supervision_combinations
                   if _combination.get('methodology') ==
                   MetricMethodologyType.PERSON)

    # pylint:disable=line-too-long
    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_supervision_types(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id='SID1341',
            id_type='US_MO_DOC',
            state_code='US_MO'
        )

        person.external_ids = [person_external_id]

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionType.PAROLE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            duplicated_months_different_supervision_types=True,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS)
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)


    # pylint:disable=line-too-long
    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_duplicate_termination(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            SupervisionTerminationBucket(
                state_code='US_ND',
                year=2010,
                month=1,
                supervision_type=StateSupervisionType.PAROLE,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=12,
                assessment_score_change=-3
            ),
            SupervisionTerminationBucket(
                state_code='US_ND',
                year=2010,
                month=1,
                supervision_type=StateSupervisionType.PAROLE,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=12,
                assessment_score_change=-3
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS)
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == -3 for _combination, value
                   in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_all_metrics(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id='SID1341',
            id_type='US_MO_DOC',
            state_code='US_MO'
        )

        person.external_ids = [person_external_id]

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionType.PAROLE),
            SupervisionTerminationBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                supervision_type=StateSupervisionType.PAROLE,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                assessment_level=StateAssessmentLevel.LOW,
                assessment_score=12,
                assessment_score_change=-3
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                supervision_type=StateSupervisionType.PAROLE,
                successful_completion=True
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            include_revocation_analysis=True
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations
                   if not
                   combo_has_enum_value_for_key(
                       _combination,
                       'metric_type', SupervisionMetricType.ASSESSMENT_CHANGE))
        assert all(value == -3 for _combination, value
                   in supervision_combinations
                   if
                   combo_has_enum_value_for_key(
                       _combination,
                       'metric_type', SupervisionMetricType.ASSESSMENT_CHANGE))

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_termination_bucket(self):
        """Tests the map_supervision_combinations when there are
        SupervisionTerminationBuckets sent to the calculator."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_ND',
            year=2000,
            month=1,
            supervision_type=StateSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [termination_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == termination_bucket.assessment_score_change
                   for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_termination_buckets_no_score_change(self):
        """Tests the map_supervision_combinations when there are
        SupervisionTerminationBuckets sent to the calculator, but the bucket
        doesn't have an assessment_score_change."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=None
        )

        supervision_time_buckets = [termination_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = 0

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == termination_bucket.assessment_score_change
                   for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_termination_buckets(self):
        """Tests the map_supervision_combinations when there are
        SupervisionTerminationBuckets sent to the calculator that end in
        the same month."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        first_termination_bucket = SupervisionTerminationBucket(
            state_code='US_ND',
            year=2000,
            month=1,
            supervision_type=StateSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        second_termination_bucket = SupervisionTerminationBucket(
            state_code='US_ND',
            year=2000,
            month=1,
            supervision_type=StateSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.REVOCATION,
            assessment_score_change=-9
        )

        supervision_time_buckets = [first_termination_bucket,
                                    second_termination_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == first_termination_bucket.assessment_score_change
                   for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_only_assessment_change(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionType.PAROLE,
            assessment_score=11,
            assessment_level=StateAssessmentLevel.LOW,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2018, month=3, supervision_type=StateSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 2, StateSupervisionType.PAROLE)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: True,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.ASSESSMENT_CHANGE
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == termination_bucket.assessment_score_change
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.ASSESSMENT_CHANGE)
            for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_only_success(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2018, month=3, supervision_type=StateSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 2, StateSupervisionType.PAROLE)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: True,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUCCESS
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.SUCCESS)
            for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_only_revocation(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2018, month=3, supervision_type=StateSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                supervision_type=StateSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010, month=2,
                supervision_type=StateSupervisionType.PAROLE)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.REVOCATION.value: True,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.REVOCATION)
            for _combination, value in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_only_revocation_analysis(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id='SID1341',
            id_type='US_MO_DOC',
            state_code='US_MO'
        )

        person.external_ids = [person_external_id]

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(state_code='US_MO', ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2018, month=3, supervision_type=StateSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                supervision_type=StateSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010, month=2,
                supervision_type=StateSupervisionType.PAROLE)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: True,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            include_revocation_analysis=True,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            metric_to_include=SupervisionMetricType.REVOCATION_ANALYSIS
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.REVOCATION_ANALYSIS)
            for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_only_population(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code='US_MO', year=2018, month=3, supervision_type=StateSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 2, StateSupervisionType.PROBATION)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: True
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.POPULATION
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.POPULATION)
            for _combination, value in supervision_combinations)


class TestCharacteristicCombinations(unittest.TestCase):
    """Tests the characteristic_combinations function."""

    def test_characteristic_combinations(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            supervision_type=StateSupervisionType.PAROLE)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT)

        # 32 combinations of demographics and supervision type
        assert len(combinations) == 32

    def test_characteristic_combinations_year_bucket(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, None, StateSupervisionType.PAROLE)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT)

        # 32 combinations of demographics and supervision type
        assert len(combinations) == 32

    def test_characteristic_combinations_exclude_age(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            supervision_type=StateSupervisionType.PAROLE)

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'age_bucket': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, inclusions)

        # 16 combinations of demographics and supervision type
        assert len(combinations) == 16

        for combo in combinations:
            assert combo.get('age_bucket') is None

    def test_characteristic_combinations_exclude_gender(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            supervision_type=StateSupervisionType.PAROLE)

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'gender': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, inclusions)

        # 16 combinations of demographics and supervision type
        assert len(combinations) == 16

        for combo in combinations:
            assert combo.get('gender') is None

    def test_characteristic_combinations_exclude_race(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            supervision_type=StateSupervisionType.PAROLE)

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'race': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, inclusions)

        # 16 combinations of demographics and supervision type
        assert len(combinations) == 16

        for combo in combinations:
            assert combo.get('race') is None

    def test_characteristic_combinations_exclude_ethnicity(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            supervision_type=StateSupervisionType.PAROLE)

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'ethnicity': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, inclusions)

        # 16 combinations of demographics and supervision type
        assert len(combinations) == 16

        for combo in combinations:
            assert combo.get('ethnicity') is None

    def test_characteristic_combinations_exclude_multiple(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            supervision_type=StateSupervisionType.PAROLE)

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'age_bucket': False,
            'ethnicity': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, inclusions)

        # 8 combinations of demographics and supervision type
        assert len(combinations) == 8

        for combo in combinations:
            assert combo.get('age_bucket') is None
            assert combo.get('ethnicity') is None

    def test_characteristic_combinations_revocation(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            supervision_type=StateSupervisionType.PAROLE,
            revocation_type=RevocationType.SHOCK_INCARCERATION,
            source_violation_type=ViolationType.FELONY)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT,
            with_revocation_dimensions=True)

        # 128 combinations of: 4 demographic dimensions
        # + supervision type + revocation type + violation type
        expected_combinations_count = expected_metric_combos_count(
            person, [supervision_time_bucket], ALL_INCLUSIONS_DICT,
            with_methodologies=False, include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION)

        self.assertEqual(expected_combinations_count, len(combinations))

    def test_characteristic_combinations_revocation_with_types_disabled(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            supervision_type=StateSupervisionType.PAROLE,
            revocation_type=RevocationType.SHOCK_INCARCERATION,
            source_violation_type=ViolationType.FELONY)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT,
            with_revocation_dimensions=False)

        # 32 combinations of: 4 demographic dimensions + supervision type
        expected_combinations_count = expected_metric_combos_count(
            person, [supervision_time_bucket], ALL_INCLUSIONS_DICT,
            with_revocation_dimensions=False, with_methodologies=False,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION)

        self.assertEqual(expected_combinations_count, len(combinations))

    def test_characteristic_combinations_revocation_no_revocation_type(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            supervision_type=StateSupervisionType.PAROLE,
            source_violation_type=ViolationType.FELONY)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT,
            with_revocation_dimensions=True)

        # 64 combinations of: 4 demographic dimensions
        # + supervision type + violation type
        expected_combinations_count = expected_metric_combos_count(
            person, [supervision_time_bucket], ALL_INCLUSIONS_DICT,
            with_methodologies=False, include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION)

        self.assertEqual(expected_combinations_count, len(combinations))

        for combo in combinations:
            assert combo.get('revocation_type') is None

    def test_characteristic_combinations_revocation_no_violation_type(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            supervision_type=StateSupervisionType.PAROLE,
            revocation_type=RevocationType.SHOCK_INCARCERATION)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT,
            with_revocation_dimensions=True)

        # 64 combinations of: 4 demographic dimensions
        # + supervision type + revocation type
        expected_combinations_count = expected_metric_combos_count(
            person, [supervision_time_bucket], ALL_INCLUSIONS_DICT,
            with_methodologies=False, include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION)

        self.assertEqual(expected_combinations_count, len(combinations))

        for combo in combinations:
            assert combo.get('violation_type') is None

    def test_characteristic_combinations_revocation_no_extra_types(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO',
                                                 race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 3, StateSupervisionType.PROBATION, None, None)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT,
            with_revocation_dimensions=True)

        # 32 combinations of: 4 demographic dimensions + supervision type
        expected_combinations_count = expected_metric_combos_count(
            person, [supervision_time_bucket], ALL_INCLUSIONS_DICT,
            with_methodologies=False, include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION)

        self.assertEqual(expected_combinations_count, len(combinations))

        for combo in combinations:
            assert combo.get('revocation_type') is None
            assert combo.get('violation_type') is None


class TestIncludeSupervisionInCount(unittest.TestCase):
    """Tests the include_non_revocation_bucket function."""
    def test_include_supervision_in_count(self):
        """Tests that the revocation bucket is included and the non-revocation
        bucket is not."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION
        }

        revocation_bucket = RevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionType.PAROLE, None, None,
            None, None,
            RevocationType.SHOCK_INCARCERATION, ViolationType.FELONY)
        non_revocation_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionType.PAROLE)

        supervision_time_buckets = [revocation_bucket, non_revocation_bucket]

        include_revocation_bucket = calculator.include_supervision_in_count(
            combo, revocation_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertTrue(include_revocation_bucket)

        include_non_revocation_bucket = calculator.include_supervision_in_count(
            combo, non_revocation_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertFalse(include_non_revocation_bucket)

    def test_include_supervision_in_count_revocation(self):
        """Tests that the revocation probation bucket is included and the
        non-revocation parole bucket is not."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION
        }

        revocation_bucket = RevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionType.PROBATION, None, None,
            None, None, None,
            RevocationType.SHOCK_INCARCERATION, ViolationType.FELONY)
        non_revocation_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionType.PAROLE)

        supervision_time_buckets = [revocation_bucket, non_revocation_bucket]

        include_revocation_bucket = calculator.include_supervision_in_count(
            combo, revocation_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertTrue(include_revocation_bucket)

        include_non_revocation_bucket = calculator.include_supervision_in_count(
            combo, non_revocation_bucket,
            supervision_time_buckets, SupervisionMetricType.POPULATION)

        self.assertFalse(include_non_revocation_bucket)

    def test_include_supervision_in_count_last_probation(self):
        """Tests that the last probation bucket is included."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION
        }

        first_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionType.PROBATION)
        second_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionType.PROBATION)

        supervision_time_buckets = [first_bucket, second_bucket]

        include_first_bucket = calculator.include_supervision_in_count(
            combo, first_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertFalse(include_first_bucket)

        include_second_bucket = calculator.include_supervision_in_count(
            combo, second_bucket,
            supervision_time_buckets, SupervisionMetricType.POPULATION)

        self.assertTrue(include_second_bucket)

    def test_include_supervision_in_count_supervision_type_set_parole(self):
        """Tests that the bucket is included when the supervision type is
        set in the combo."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION,
            'supervision_type': StateSupervisionType.PAROLE
        }

        first_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionType.PAROLE)
        second_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionType.PROBATION)

        supervision_time_buckets = [first_bucket, second_bucket]

        include_first_bucket = calculator.include_supervision_in_count(
            combo, first_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertTrue(include_first_bucket)

    def test_include_supervision_in_count_supervision_type_set_probation(self):
        """Tests that the bucket is included when the supervision type is
        set in the combo."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION,
            'supervision_type': StateSupervisionType.PROBATION
        }

        first_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionType.PAROLE)
        second_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionType.PROBATION)

        supervision_time_buckets = [first_bucket, second_bucket]

        include_second_bucket = calculator.include_supervision_in_count(
            combo, second_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertTrue(include_second_bucket)


def demographic_metric_combos_count_for_person_supervision(
        person: StatePerson,
        inclusions: Dict[str, bool]) -> int:
    """Returns the number of possible demographic metric combinations for a
    given person, given the metric inclusions list."""

    metric_inclusions = []

    for metric_type in SupervisionMetricType:
        metric_inclusions.append(metric_type.value)

    demographic_inclusions = {}

    for key, value in inclusions.items():
        if key not in metric_inclusions:
            demographic_inclusions[key] = value

    total_metric_combos = demographic_metric_combos_count_for_person(
        person, demographic_inclusions
    )

    # Supervision type is always included
    total_metric_combos *= 2

    return total_metric_combos


def expected_metric_combos_count(
        person: StatePerson,
        supervision_time_buckets: List[SupervisionTimeBucket],
        inclusions: Dict[str, bool],
        with_revocation_dimensions: bool = True,
        with_methodologies: bool = True,
        include_all_metrics: bool = True,
        metric_to_include: SupervisionMetricType = None,
        duplicated_months_different_supervision_types: bool = False,
        num_relevant_periods: int = 0,
        include_revocation_analysis: bool = False) -> int:
    """Calculates the expected number of characteristic combinations given
    the person, the supervision time buckets, and the dimensions that should
    be included in the explosion of feature combinations."""

    demographic_metric_combos = \
        demographic_metric_combos_count_for_person_supervision(
            person, inclusions)

    # Some test cases above use a different call that doesn't take methodology
    # into account as a dimension
    methodology_multiplier = 1
    if with_methodologies:
        methodology_multiplier *= CALCULATION_METHODOLOGIES

    projected_completion_buckets = [
        bucket for bucket in supervision_time_buckets
        if isinstance(bucket, ProjectedSupervisionCompletionBucket)
    ]

    population_buckets = [
        bucket for bucket in supervision_time_buckets
        if isinstance(bucket, (RevocationReturnSupervisionTimeBucket,
                               NonRevocationReturnSupervisionTimeBucket))
    ]

    termination_buckets = [
        bucket for bucket in supervision_time_buckets
        if isinstance(bucket, SupervisionTerminationBucket)
    ]

    num_duplicated_population_buckets = 0
    population_months: Set[Tuple[int, int]] = set()

    for bucket in population_buckets:
        year = bucket.year
        month = 0 if bucket.month is None else bucket.month
        if (year, month) in population_months:
            num_duplicated_population_buckets += 1
        if month is not None:
            population_months.add((year, month))

    num_duplicated_projected_completion_months = 0
    completion_months: Set[Tuple[int, int]] = set()

    for projected_completion_bucket in projected_completion_buckets:
        if (projected_completion_bucket.year,
                projected_completion_bucket.month) in completion_months:
            num_duplicated_projected_completion_months += 1
        if projected_completion_bucket.month:
            completion_months.add((projected_completion_bucket.year,
                                   projected_completion_bucket.month))

    num_projected_completion_buckets = len(projected_completion_buckets)

    # Calculate total combos for supervision population
    num_population_buckets = len(population_buckets)

    num_duplicated_termination_buckets = 0
    termination_months: Set[Tuple[int, int]] = set()

    for termination_bucket in termination_buckets:
        year = termination_bucket.year
        month = 0 if termination_bucket.month is None else \
            termination_bucket.month
        if (year, month) in termination_months:
            num_duplicated_termination_buckets += 1
        if month is not None:
            termination_months.add((year, month))

    revocation_buckets = [
        bucket for bucket in supervision_time_buckets
        if isinstance(bucket, RevocationReturnSupervisionTimeBucket)
    ]
    num_revocation_buckets = len(revocation_buckets)

    population_dimension_multiplier = 1
    if population_buckets:
        first_population_bucket = population_buckets[0]

        population_dimensions = [
            'case_type',
            'assessment_score',
            'assessment_type',
            'supervising_officer_external_id',
            'supervising_district_external_id'
        ]

        for dimension in population_dimensions:
            if getattr(first_population_bucket, dimension):
                population_dimension_multiplier *= 2

    supervision_population_combos = \
        demographic_metric_combos * methodology_multiplier * \
        num_population_buckets * population_dimension_multiplier

    # Supervision population metrics removed in person-based de-duplication
    duplicated_population_combos = \
        int(demographic_metric_combos *
            num_duplicated_population_buckets *
            population_dimension_multiplier)

    if duplicated_months_different_supervision_types:
        # If the duplicated months have different supervision types, then
        # don't remove the supervision-type-specific combos
        duplicated_population_combos = int(duplicated_population_combos / 2)

    supervision_population_combos -= duplicated_population_combos

    # Calculate total combos for supervision revocation
    revocation_dimension_multiplier = 1
    if with_revocation_dimensions and revocation_buckets:
        first_revocation_bucket = revocation_buckets[0]

        revocation_dimensions = [
            'case_type',
            'revocation_type',
            'source_violation_type',
            'assessment_score',
            'assessment_type',
            'supervising_officer_external_id',
            'supervising_district_external_id'
        ]

        for dimension in revocation_dimensions:
            if getattr(first_revocation_bucket, dimension):
                revocation_dimension_multiplier *= 2

    supervision_revocation_combos = \
        demographic_metric_combos * methodology_multiplier * \
        num_revocation_buckets * revocation_dimension_multiplier

    revocation_analysis_dimension_multiplier = revocation_dimension_multiplier

    if with_revocation_dimensions and revocation_buckets:
        first_revocation_analysis_bucket = revocation_buckets[0]

        revocation_analysis_dimensions = [
            'most_severe_violation_type',
            'most_severe_violation_type_subtype',
            'most_severe_response_decision'
        ]

        for dimension in revocation_analysis_dimensions:
            if getattr(first_revocation_analysis_bucket, dimension):
                revocation_analysis_dimension_multiplier *= 2

        if first_revocation_analysis_bucket.response_count is not None:
            revocation_analysis_dimension_multiplier *= 2

    supervision_revocation_analysis_combos = 0

    if include_revocation_analysis:
        supervision_revocation_analysis_combos += \
            (demographic_metric_combos *
             methodology_multiplier *
             num_revocation_buckets *
             revocation_analysis_dimension_multiplier)

    projected_completion_dimension_multiplier = 1
    if projected_completion_buckets:
        first_projected_completion_bucket = projected_completion_buckets[0]

        projected_completion_dimensions = [
            'case_type',
            'supervising_officer_external_id',
            'supervising_district_external_id'
        ]

        for dimension in projected_completion_dimensions:
            if getattr(first_projected_completion_bucket, dimension):
                projected_completion_dimension_multiplier *= 2

    supervision_success_combos = \
        demographic_metric_combos * methodology_multiplier * \
        num_projected_completion_buckets * \
        projected_completion_dimension_multiplier

    # Success metrics removed in person-based de-duplication that don't
    # specify supervision type
    duplicated_success_combos = \
        int(demographic_metric_combos *
            num_duplicated_projected_completion_months *
            projected_completion_dimension_multiplier)

    if duplicated_months_different_supervision_types:
        # If the duplicated months have different supervision types, then
        # don't remove the supervision-type-specific combos
        duplicated_success_combos = int(duplicated_success_combos / 2)

    # Remove the combos that don't specify supervision type for the duplicated
    # projected completion months
    supervision_success_combos -= duplicated_success_combos

    # Calculate total combos for supervision termination
    termination_dimension_multiplier = 1
    if termination_buckets:
        first_termination_bucket = termination_buckets[0]

        termination_dimensions = [
            'case_type',
            'termination_reason',
            'assessment_score',
            'assessment_type',
            'supervising_officer_external_id',
            'supervising_district_external_id'
        ]

        for dimension in termination_dimensions:
            if getattr(first_termination_bucket, dimension):
                termination_dimension_multiplier *= 2

    num_termination_buckets = len(termination_buckets)

    supervision_termination_combos = \
        demographic_metric_combos * methodology_multiplier * \
        num_termination_buckets * termination_dimension_multiplier

    # Termination metrics removed in person-based de-duplication that don't
    # specify supervision type
    duplicated_termination_combos = \
        int(demographic_metric_combos *
            num_duplicated_termination_buckets *
            termination_dimension_multiplier)

    supervision_termination_combos -= duplicated_termination_combos

    if num_relevant_periods > 0:
        # Population combos
        supervision_population_combos += \
            demographic_metric_combos * \
            (len(population_buckets) - num_duplicated_population_buckets) * \
            population_dimension_multiplier * \
            num_relevant_periods

        if duplicated_months_different_supervision_types:
            supervision_population_combos += \
                int(demographic_metric_combos *
                    num_duplicated_population_buckets *
                    population_dimension_multiplier *
                    num_relevant_periods / 2)

        # Revocation combos
        supervision_revocation_combos += \
            demographic_metric_combos * \
            (len(revocation_buckets)) * \
            revocation_dimension_multiplier * \
            num_relevant_periods

        if include_revocation_analysis:
            # Other metric buckets
            supervision_revocation_analysis_combos += \
                demographic_metric_combos * \
                (len(revocation_buckets)) * \
                num_relevant_periods * \
                revocation_analysis_dimension_multiplier

            # Person-level metrics
            supervision_revocation_analysis_combos += (num_relevant_periods + 1)

        # Success combos
        supervision_success_combos += \
            demographic_metric_combos * \
            (num_projected_completion_buckets -
             num_duplicated_projected_completion_months) * \
            projected_completion_dimension_multiplier * \
            num_relevant_periods

        # Termination combos
        supervision_termination_combos += \
            demographic_metric_combos * \
            (len(termination_buckets) - num_duplicated_termination_buckets) * \
            termination_dimension_multiplier * \
            num_relevant_periods

    # Pick which one is relevant for the test case: some tests above use a
    # different call that only looks at combos for either population or
    # revocation, but not both
    if include_all_metrics:
        return supervision_population_combos + \
               supervision_revocation_combos + \
               supervision_success_combos + \
               supervision_termination_combos + \
               supervision_revocation_analysis_combos

    if metric_to_include:
        if metric_to_include == SupervisionMetricType.ASSESSMENT_CHANGE:
            return supervision_termination_combos
        if metric_to_include == SupervisionMetricType.SUCCESS:
            return supervision_success_combos
        if metric_to_include == SupervisionMetricType.REVOCATION:
            return supervision_revocation_combos
        if metric_to_include == SupervisionMetricType.REVOCATION_ANALYSIS:
            return supervision_revocation_analysis_combos
        if metric_to_include == SupervisionMetricType.POPULATION:
            return supervision_population_combos

    return 0
