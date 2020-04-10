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
# pylint: disable=unused-import,wrong-import-order,protected-access
import unittest
from datetime import date
from typing import Dict, List, Tuple, Set

from freezegun import freeze_time

from recidiviz.calculator.pipeline.supervision import calculator
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetricType
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    NonRevocationReturnSupervisionTimeBucket, SupervisionTimeBucket, \
    RevocationReturnSupervisionTimeBucket, ProjectedSupervisionCompletionBucket, SupervisionTerminationBucket
from recidiviz.calculator.pipeline.utils import calculator_utils
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodTerminationReason, StateSupervisionPeriodSupervisionType, StateSupervisionLevel
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
        SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED.value: True,
        SupervisionMetricType.REVOCATION.value: True,
        SupervisionMetricType.REVOCATION_ANALYSIS.value: True,
        SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS.value: True,
        SupervisionMetricType.POPULATION.value: True,
    }

CALCULATION_METHODOLOGIES = len(MetricMethodologyType)


# TODO(2732): Implement more full test coverage of the officer, district, the supervision success functionality
class TestMapSupervisionCombinations(unittest.TestCase):
    """Tests the map_supervision_combinations function."""

    # Freezing time to before the events so none of them fall into the relevant metric periods
    @freeze_time('1900-01-01')
    def test_map_supervision_combinations(self):
        """Tests the map_supervision_combinations function."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_assessment(self):
        """Tests the map_supervision_combinations function when there is assessment data present."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

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
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_exclude_assessment(self):
        """Tests the map_supervision_combinations function when there is assessment data present, but it should not
        be included for this state and pipeline type."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

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
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        # 2 person-level event and 2 person-level person based metrics that aren't affected by the assessment explosion
        expected_combinations_count -= 4

        # Remove the dimensional explosions for assessment_type (assessment_score_bucket remains as `NOT_ASSESSED`)
        expected_combinations_count = expected_combinations_count / 2

        # Add them back
        expected_combinations_count += 4

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_supervising_officer_district(self):
        """Tests the map_supervision_combinations function when there is supervising officer and district data
        present."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

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
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.VERY_HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                supervising_officer_external_id='143',
                supervising_district_external_id='DISTRICT X'),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.VERY_HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                supervising_officer_external_id='143',
                supervising_district_external_id='DISTRICT X')
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_revocation_combinations(self):
        """Tests the map_supervision_combinations function for a revocation month."""
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

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2018,
                month=3,
                revocation_admission_date=date(2018, 3, 1),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=12,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                revocation_type=RevocationType.SHOCK_INCARCERATION,
                source_violation_type=ViolationType.FELONY,
                most_severe_violation_type=ViolationType.FELONY,
                most_severe_violation_type_subtype='SUBTYPE',
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=10,
                supervision_level=StateSupervisionLevel.MINIMUM,
                supervision_level_raw_text='MIN'
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_revocation_analysis=True)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)
        assert all(combo.get('supervision_level_raw_text') is not None for combo, value in supervision_combinations
                   if combo.get('person_id') is not None
                   and combo_has_enum_value_for_key(combo, 'metric_type', SupervisionMetricType.POPULATION))

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_supervision_success(self):
        """Tests the map_supervision_combinations function when there is a ProjectedSupervisionCompletionBucket."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=998,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations
                   if not combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED))
        assert all(value == 998 for _combination, value in supervision_combinations
                   if combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED))

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_supervision_unsuccessful(self):
        """Tests the map_supervision_combinations function when there is a ProjectedSupervisionCompletionBucket
        and the supervision is not successfully completed."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=False,
                incarcerated_during_sentence=True,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations if not
                   combo_has_enum_value_for_key(_combination, 'metric_type', SupervisionMetricType.SUCCESS))
        assert all(value == 0 for _combination, value
                   in supervision_combinations if
                   combo_has_enum_value_for_key(_combination, 'metric_type', SupervisionMetricType.SUCCESS))
        assert all(not combo_has_enum_value_for_key(
            _combination, 'metric_type', SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED)
                   for _combination, _ in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_supervision_mixed_success(self):
        """Tests the map_supervision_combinations function when there is a ProjectedSupervisionCompletionBucket and the
        supervision is not successfully completed."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_ND', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=False,
                incarcerated_during_sentence=True,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_ND', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=199,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            duplicated_months_different_supervision_types=True,
            duplicated_months_mixed_success=True
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations
                   if not combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED)
                   and (not combo_has_enum_value_for_key(_combination, 'metric_type', SupervisionMetricType.SUCCESS)
                        or combo_has_enum_value_for_key(_combination, 'supervision_type',
                                                        StateSupervisionPeriodSupervisionType.PROBATION)))
        assert all(value == 0 for _combination, value
                   in supervision_combinations if
                   combo_has_enum_value_for_key(_combination, 'metric_type', SupervisionMetricType.SUCCESS) and not
                   combo_has_enum_value_for_key(_combination, 'supervision_type',
                                                StateSupervisionPeriodSupervisionType.PROBATION) and not
                   _combination.get('supervision_type') is None)
        assert all(not combo_has_enum_value_for_key(_combination, 'metric_type',
                                                    SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED)
                   for _combination, value in supervision_combinations
                   if _combination.get('supervision_type' is None
                                       or _combination.get('supervision_type') ==
                                       StateSupervisionPeriodSupervisionType.PROBATION))

    @freeze_time('2020-02-01')
    def test_map_supervision_combinations_supervision_with_district_officer(self):
        """Tests the map_supervision_combinations function when there is a mix of missing & non-null district/officer
        data for one person over many ProjectedSupervisionCompletionBuckets."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1993, 4, 2),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2017, month=6,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False,
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2020, month=1,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False,
                supervising_officer_external_id='officer45', supervising_district_external_id='district5'
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=12,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
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

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=2,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=13,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                supervising_officer_external_id='OFFICER',
                supervising_district_external_id='DISTRICT',
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                response_count=19
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=13,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                supervising_officer_external_id='OFFICER',
                supervising_district_external_id='DISTRICT',
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                response_count=19
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=4,
                revocation_admission_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
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
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_revocation_analysis=True)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_multiple_months(self):
        """Tests the map_supervision_combinations function where the person was on supervision for multiple months."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 5, StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 6, StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 7, StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=True),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_overlapping_months(self):
        """Tests the map_supervision_combinations function where the person was serving multiple supervision sentences
        simultaneously in a given month."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 3, StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 3, StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=True)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_overlapping_months_types(self):
        """Tests the map_supervision_combinations function where the person was serving multiple supervision sentences
        simultaneously in a given month, but the supervisions are of different types."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=5,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            duplicated_months_different_supervision_types=True
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_overlapping_months_types_dual(self):
        """Tests the map_supervision_combinations function where the person was serving multiple supervision sentences
        simultaneously in a given month, but the supervisions are of different types."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=3,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=4,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            duplicated_months_different_supervision_types=True
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)
        assert all(_combination.get('supervision_type') == StateSupervisionPeriodSupervisionType.DUAL
                   for _combination, _ in supervision_combinations
                   if _combination.get('person_id') is not None
                   and _combination.get('methodology') == MetricMethodologyType.PERSON)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_two_revocations_in_month_sort_date(self):
        """Tests the map_supervision_combinations function where the person was revoked twice in a given month. Asserts
        that the revocation with the latest date is chosen."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_ND', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=3,
                revocation_admission_date=date(2018, 3, 13),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervising_officer_external_id='FAKE_ID_1'
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=3,
                revocation_admission_date=date(2018, 3, 29),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervising_officer_external_id='FAKE_ID_2'
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_revocation_analysis=True,
            duplicated_months_different_supervision_types=False
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)
        assert all(_combination.get('supervising_officer_external_id') == 'FAKE_ID_2'
                   or _combination.get('supervising_officer_external_id') is None
                   for _combination, _ in supervision_combinations
                   if _combination.get('methodology') == MetricMethodologyType.PERSON)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=True)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            include_revocation_analysis=True
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_one_non_relevant_period(self):
        person = StatePerson.new_with_defaults(person_id=12345, birthdate=date(1984, 8, 31), gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(state_code='US_MO', ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        relevant_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO',
            year=2010,
            month=1,
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        not_relevant_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO',
            year=1980,
            month=1,
            revocation_admission_date=date(1980, 1, 1),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)

        supervision_time_buckets = [relevant_bucket, not_relevant_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1)

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

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                revocation_admission_date=date(1980, 1, 1),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            include_revocation_analysis=True
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_duplicates(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=True)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS)
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    # pylint:disable=line-too-long
    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_duplicate_month_mixed_success(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

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
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=190
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=False,
                incarcerated_during_sentence=False,
                sentence_days_served=195
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            duplicated_months_mixed_success=True
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(not value for _combination, value in supervision_combinations
                   if _combination.get('methodology') == MetricMethodologyType.PERSON)
        assert all(_combination.get('methodology') == MetricMethodologyType.EVENT
                   and value == 190
                   for _combination, value in supervision_combinations
                   if combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED))

    # pylint:disable=line-too-long
    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_supervision_types(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id='SID1341',
            id_type='US_ND_DOC',
            state_code='US_ND'
        )

        person.external_ids = [person_external_id]

        race = StatePersonRace.new_with_defaults(state_code='US_ND', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_ND',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                'US_ND', 2010, 1, StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_ND', 2010, 1, StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_ND', 2010, 1, StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=True)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            duplicated_months_different_supervision_types=True,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS)
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)
        assert all(_combination.get('supervision_type') == StateSupervisionPeriodSupervisionType.DUAL
                   for _combination, _ in supervision_combinations
                   if _combination.get('person_id') is not None
                   and _combination.get('methodology') == MetricMethodologyType.PERSON)

    # pylint:disable=line-too-long
    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_duplicate_termination(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

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
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=12,
                assessment_score_change=-3
            ),
            SupervisionTerminationBucket(
                state_code='US_ND',
                year=2010,
                month=1,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=12,
                assessment_score_change=-3
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS)
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
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

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                revocation_admission_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=3,
                violation_history_description='1fel;2misd',
                violation_type_frequency_counter=[
                    ['FELONY', 'LAW'],
                    ['MISD', 'WEA', 'EMP'],
                    ['MISD', 'DRG', 'ASC']
                ]
            ),
            SupervisionTerminationBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                assessment_level=StateAssessmentLevel.LOW,
                assessment_score=12,
                assessment_score_change=-3
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=234
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            include_revocation_analysis=True,
            include_revocation_violation_type_analysis_dimensions=True
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations
                   if not combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.ASSESSMENT_CHANGE)
                   and not combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED))
        assert all(value == -3 for _combination, value
                   in supervision_combinations
                   if combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.ASSESSMENT_CHANGE))
        assert all(value == 234 for _combination, value
                   in supervision_combinations
                   if combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED))

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_termination_bucket(self):
        """Tests the map_supervision_combinations when there are SupervisionTerminationBuckets sent to the
        calculator."""
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
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [termination_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == termination_bucket.assessment_score_change
                   for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_termination_buckets_no_score_change(self):
        """Tests the map_supervision_combinations when there are SupervisionTerminationBuckets sent to the calculator,
        but the bucket doesn't have an assessment_score_change."""
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=None
        )

        supervision_time_buckets = [termination_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = 0

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == termination_bucket.assessment_score_change
                   for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_termination_buckets(self):
        """Tests the map_supervision_combinations when there are SupervisionTerminationBuckets sent to the calculator
        that end in the same month."""
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
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
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
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.REVOCATION,
            assessment_score_change=-9
        )

        supervision_time_buckets = [first_termination_bucket,
                                    second_termination_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == first_termination_bucket.assessment_score_change
                   for _combination, value
                   in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_only_assessment_change(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_level=StateAssessmentLevel.LOW,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, incarcerated_during_sentence=False,
                sentence_days_served=398,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionPeriodSupervisionType.PROBATION, is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 2, StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=True)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: True,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED.value: False,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.ASSESSMENT_CHANGE
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
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

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionPeriodSupervisionType.PROBATION, is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 2, StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=True)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: True,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED.value: False,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUCCESS
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.SUCCESS)
            for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_only_successful_sentence_length(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, incarcerated_during_sentence=False,
                sentence_days_served=500,
                supervising_officer_external_id='officer45', supervising_district_external_id='district5'
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, StateSupervisionPeriodSupervisionType.PROBATION, is_on_supervision_last_day_of_month=True),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 2, StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=True)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED.value: True,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 500 for _combination, value in supervision_combinations)
        assert all(combo_has_enum_value_for_key(_combination, 'metric_type',
                                                SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED)
                   for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_only_revocation(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                revocation_admission_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010, month=2,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED.value: False,
            SupervisionMetricType.REVOCATION.value: True,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
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
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                revocation_admission_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010, month=2,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED.value: False,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: True,
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            include_revocation_analysis=True,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            metric_to_include=SupervisionMetricType.REVOCATION_ANALYSIS
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.REVOCATION_ANALYSIS)
            for _combination, value in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_only_revocation_violation_type_analysis(self):
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
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                revocation_admission_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=3,
                violation_history_description='1fel;2misd',
                violation_type_frequency_counter=[
                    ['FELONY', 'LAW'],
                    ['MISD', 'WEA', 'EMP'],
                    ['MISD', 'DRG', 'ASC']
                ]
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010, month=2,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED.value: False,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS.value: True,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            include_revocation_analysis=True,
            include_revocation_violation_type_analysis_dimensions=True,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            metric_to_include=SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)
        assert all(combo_has_enum_value_for_key(_combination, 'metric_type',
                                                SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS)
                   for _combination, value in supervision_combinations)
        assert all(_combination.get('violation_count_type') is not None
                   for _combination, value in supervision_combinations)
        assert all(_combination.get('person_id') is None for _combination, value in supervision_combinations)
        assert all('person_id' not in _combination.keys() for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_only_population(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code='US_MO',
            year=2000,
            month=1,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                revocation_admission_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010,
                month=2,
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION)
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED.value: False,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: True
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.POPULATION
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.POPULATION)
            for _combination, value in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_calculation_month_limit_12(self):
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

        included_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2010, 1, StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=True)

        not_included_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2005, 1, StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=True
        )

        supervision_time_buckets = [included_bucket, not_included_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=12
        )

        expected_combinations_count = expected_metric_combos_count(
            person, [included_bucket], ALL_INCLUSIONS_DICT,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            include_revocation_analysis=True
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2010-12-31')
    def test_map_supervision_combinations_relevant_periods_calculation_month_limit_37(self):
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

        # This bucket will be included in the MoM metrics, but does not fall in any of the metric_period_months metrics
        included_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2007, 12, StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=True)

        supervision_time_buckets = [included_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=37
        )

        expected_combinations_count = expected_metric_combos_count(
            person, [included_bucket], ALL_INCLUSIONS_DICT,
            include_revocation_analysis=True
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2010-12-31')
    def test_map_supervision_combinations_relevant_periods_calculation_month_no_output(self):
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

        # This bucket will be included in the MoM metrics, but does not fall in any of the metric_period_months metrics
        included_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2007, 12, StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=True)

        supervision_time_buckets = [included_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT, calculation_month_limit=12
        )

        self.assertEqual(0, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    @freeze_time('1900-01-01')
    def test_map_supervision_combinations_only_successful_sentence_length_duplicate_month_longer_sentence(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, incarcerated_during_sentence=False,
                sentence_days_served=500,
                supervising_officer_external_id='officer45', supervising_district_external_id='district5'
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, incarcerated_during_sentence=False,
                sentence_days_served=501,
                supervising_officer_external_id='officer45', supervising_district_external_id='district5'
            )
        ]

        inclusions_dict = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            SupervisionMetricType.ASSESSMENT_CHANGE.value: False,
            SupervisionMetricType.SUCCESS.value: False,
            SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED.value: True,
            SupervisionMetricType.REVOCATION.value: False,
            SupervisionMetricType.REVOCATION_ANALYSIS.value: False,
            SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS.value: False,
            SupervisionMetricType.POPULATION.value: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict, calculation_month_limit=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            person, supervision_time_buckets, ALL_INCLUSIONS_DICT,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 501 for _combination, value in supervision_combinations
                   if _combination.get('methodology') == MetricMethodologyType.PERSON)
        assert all(combo_has_enum_value_for_key(_combination, 'metric_type',
                                                SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED)
                   for _combination, value in supervision_combinations)


class TestCharacteristicCombinations(unittest.TestCase):
    """Tests the characteristic_combinations function."""

    def test_characteristic_combinations(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            response_count=None
        )

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT, metric_type=SupervisionMetricType.POPULATION)

        # 64 combinations of demographics and supervision type + 1 person-level metric
        self.assertEqual(len(combinations), 65)

    def test_characteristic_combinations_exclude_age(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            most_severe_violation_type=StateSupervisionViolationType.ABSCONDED,
            response_count=2
        )

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'age_bucket': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, inclusions, metric_type=SupervisionMetricType.POPULATION)

        # 128 combinations of demographics, supervision type, and response count + 1 person-level metric
        self.assertEqual(len(combinations), 129)

        for combo in combinations:
            assert combo.get('age_bucket') is None

    def test_characteristic_combinations_exclude_gender(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype='SUBTYPE',
            response_count=1
        )

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'gender': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, inclusions, metric_type=SupervisionMetricType.POPULATION)

        # 256 combinations of demographics, supervision type, and violations
        self.assertEqual(len(combinations), 257)

        for combo in combinations:
            assert combo.get('gender') is None

    def test_characteristic_combinations_exclude_race(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            response_count=1
        )

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'race': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, inclusions, metric_type=SupervisionMetricType.POPULATION)

        # 128 combinations of demographics, supervision type, violation type, and response count
        self.assertEqual(len(combinations), 129)

        for combo in combinations:
            assert combo.get('race') is None

    def test_characteristic_combinations_exclude_ethnicity(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            response_count=3
        )

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'ethnicity': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, inclusions, metric_type=SupervisionMetricType.POPULATION)

        # 64 combinations of demographics, supervision type, and response count
        self.assertEqual(len(combinations), 65)

        for combo in combinations:
            assert combo.get('ethnicity') is None

    def test_characteristic_combinations_exclude_multiple(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            response_count=1
        )

        inclusions = {
            **ALL_INCLUSIONS_DICT,
            'age_bucket': False,
            'ethnicity': False,
        }

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, inclusions, metric_type=SupervisionMetricType.POPULATION)

        # 32 combinations of demographics, supervision type, and response count
        self.assertEqual(len(combinations), 33)

        for combo in combinations:
            assert combo.get('age_bucket') is None
            assert combo.get('ethnicity') is None

    def test_characteristic_combinations_revocation(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            revocation_admission_date=date(2010, 3, 1),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            revocation_type=RevocationType.SHOCK_INCARCERATION,
            source_violation_type=ViolationType.FELONY)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT,
            metric_type=SupervisionMetricType.REVOCATION)

        expected_combinations_count = expected_metric_combos_count(
            person, [supervision_time_bucket], ALL_INCLUSIONS_DICT,
            with_methodologies=False, include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION)

        # Subtract the event-based metric dict from the combo count
        expected_combinations_count -= 1

        self.assertEqual(expected_combinations_count, len(combinations))

    def test_characteristic_combinations_revocation_for_population_metric(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            revocation_admission_date=date(2010, 3, 1),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            revocation_type=RevocationType.SHOCK_INCARCERATION,
            source_violation_type=ViolationType.FELONY)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT,
            metric_type=SupervisionMetricType.POPULATION)

        expected_combinations_count = expected_metric_combos_count(
            person, [supervision_time_bucket], ALL_INCLUSIONS_DICT,
            with_revocation_dimensions=False, with_methodologies=False,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.POPULATION)

        # Subtract the event-based metric dict from the combo count
        expected_combinations_count -= 1

        self.assertEqual(expected_combinations_count, len(combinations))

    def test_characteristic_combinations_revocation_no_revocation_type(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            revocation_admission_date=date(2010, 3, 1),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            source_violation_type=ViolationType.FELONY)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT,
            metric_type=SupervisionMetricType.REVOCATION)

        expected_combinations_count = expected_metric_combos_count(
            person, [supervision_time_bucket], ALL_INCLUSIONS_DICT,
            with_methodologies=False, include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION)

        # Subtract the event-based metric dict from the combo count
        expected_combinations_count -= 1

        self.assertEqual(expected_combinations_count, len(combinations))

        for combo in combinations:
            assert combo.get('revocation_type') is None

    def test_characteristic_combinations_revocation_no_violation_type(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            revocation_admission_date=date(2010, 3, 1),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            revocation_type=RevocationType.SHOCK_INCARCERATION)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT,
            metric_type=SupervisionMetricType.REVOCATION)

        expected_combinations_count = expected_metric_combos_count(
            person, [supervision_time_bucket], ALL_INCLUSIONS_DICT,
            with_methodologies=False, include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION)

        # Subtract the event-based metric dict from the combo count
        expected_combinations_count -= 1

        self.assertEqual(expected_combinations_count, len(combinations))

        for combo in combinations:
            assert combo.get('violation_count_type') is None

    def test_characteristic_combinations_revocation_no_extra_types(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=3,
            revocation_admission_date=date(2010, 3, 1),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION)

        combinations = calculator.characteristic_combinations(
            person, supervision_time_bucket, ALL_INCLUSIONS_DICT,
            metric_type=SupervisionMetricType.REVOCATION)

        # 32 combinations of: 4 demographic dimensions + supervision type
        expected_combinations_count = expected_metric_combos_count(
            person, [supervision_time_bucket], ALL_INCLUSIONS_DICT,
            with_methodologies=False, include_all_metrics=False,
            metric_to_include=SupervisionMetricType.REVOCATION)

        # Subtract the event-based metric dict from the combo count
        expected_combinations_count -= 1

        self.assertEqual(expected_combinations_count, len(combinations))

        for combo in combinations:
            assert combo.get('revocation_type') is None
            assert combo.get('violation_count_type') is None


class TestIncludeSupervisionInCount(unittest.TestCase):
    """Tests the include_non_revocation_bucket function."""
    def test_include_supervision_in_count(self):
        """Tests that the revocation bucket is included and the non-revocation bucket is not."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION
        }

        revocation_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=4,
            revocation_admission_date=date(2010, 4, 1),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            revocation_type=RevocationType.SHOCK_INCARCERATION, most_severe_violation_type=ViolationType.FELONY)
        non_revocation_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=True)

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
        """Tests that the revocation probation bucket is included and the non-revocation parole bucket is not."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION
        }

        revocation_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=4,
            revocation_admission_date=date(2010, 4, 1),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            revocation_type=RevocationType.SHOCK_INCARCERATION, most_severe_violation_type=ViolationType.FELONY)
        non_revocation_bucket = NonRevocationReturnSupervisionTimeBucket(
            state_code='US_MO', year=2018, month=4, is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)

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
            'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PROBATION, is_on_supervision_last_day_of_month=True)
        second_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PROBATION, is_on_supervision_last_day_of_month=True)

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
        """Tests that the bucket is included when the supervision type is set in the combo."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION,
            'supervision_type': StateSupervisionPeriodSupervisionType.PAROLE
        }

        first_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_XX', 2018, 4, StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=True)
        second_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_XX', 2018, 4, StateSupervisionPeriodSupervisionType.PROBATION, is_on_supervision_last_day_of_month=True)

        supervision_time_buckets = [first_bucket, second_bucket]

        include_first_bucket = calculator.include_supervision_in_count(
            combo, first_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertTrue(include_first_bucket)

    def test_include_supervision_in_count_supervision_type_set_probation(self):
        """Tests that the bucket is included when the supervision type is set in the combo."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION,
            'supervision_type': StateSupervisionPeriodSupervisionType.PROBATION
        }

        first_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=True)
        second_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PROBATION, is_on_supervision_last_day_of_month=True)

        supervision_time_buckets = [first_bucket, second_bucket]

        include_second_bucket = calculator.include_supervision_in_count(
            combo, second_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertTrue(include_second_bucket)

    def test_include_supervision_in_count_test_all_metrics(self):
        """Tests that the revocation bucket is included and the non-revocation bucket is not."""
        for metric_type in SupervisionMetricType:
            combo = {
                'metric_type': metric_type
            }

            if metric_type == SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED:
                bucket = ProjectedSupervisionCompletionBucket(
                    'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PAROLE,
                    successful_completion=True, incarcerated_during_sentence=False, sentence_days_served=100)

                # Assert that this does not raise an error
                _ = calculator.include_supervision_in_count(combo, bucket, [bucket], metric_type)
            elif metric_type in (SupervisionMetricType.REVOCATION,
                                 SupervisionMetricType.REVOCATION_ANALYSIS,
                                 SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS):
                revocation_bucket = RevocationReturnSupervisionTimeBucket(
                    state_code='XX',
                    year=2018,
                    month=4,
                    revocation_admission_date=date(2018, 4, 1),
                    is_on_supervision_last_day_of_month=False
                )

                # Assert that this does not raise an error
                _ = calculator.include_supervision_in_count(
                    combo, revocation_bucket, [revocation_bucket], metric_type)
            else:
                non_revocation_bucket = NonRevocationReturnSupervisionTimeBucket(
                    'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PAROLE,
                    is_on_supervision_last_day_of_month=True)

                # Assert that this does not raise an error
                _ = calculator.include_supervision_in_count(
                    combo, non_revocation_bucket, [non_revocation_bucket], metric_type)

    def test_include_supervision_in_count_is_on_supervision_last_day_of_month_no_revocation(self):
        """Tests that the bucket with is_on_supervision_last_day_of_month=True is included over others."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION,
            'supervision_type': StateSupervisionPeriodSupervisionType.PROBATION
        }

        first_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PAROLE,
            is_on_supervision_last_day_of_month=True)
        second_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PROBATION,
            is_on_supervision_last_day_of_month=False)

        supervision_time_buckets = [first_bucket, second_bucket]

        include_first_bucket = calculator.include_supervision_in_count(
            combo, first_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertTrue(include_first_bucket)

        include_second_bucket = calculator.include_supervision_in_count(
            combo, second_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertFalse(include_second_bucket)

    def test_include_supervision_in_count_is_on_supervision_last_day_of_month_with_revocation(self):
        """Tests that the bucket with is_on_supervision_last_day_of_month=True is included over others, unless there is
        a revocation bucket present."""
        combo = {
            'metric_type': SupervisionMetricType.POPULATION,
            'supervision_type': StateSupervisionPeriodSupervisionType.PROBATION
        }

        first_bucket = RevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PAROLE,
            is_on_supervision_last_day_of_month=False)
        second_bucket = NonRevocationReturnSupervisionTimeBucket(
            'US_MO', 2018, 4, StateSupervisionPeriodSupervisionType.PROBATION,
            is_on_supervision_last_day_of_month=True)

        supervision_time_buckets = [first_bucket, second_bucket]

        include_first_bucket = calculator.include_supervision_in_count(
            combo, first_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertTrue(include_first_bucket)

        include_second_bucket = calculator.include_supervision_in_count(
            combo, second_bucket, supervision_time_buckets,
            SupervisionMetricType.POPULATION)

        self.assertFalse(include_second_bucket)


def demographic_metric_combos_count_for_person_supervision(
        person: StatePerson,
        inclusions: Dict[str, bool]) -> int:
    """Returns the number of possible demographic metric combinations for a given person, given the metric inclusions
    list."""

    metric_inclusions = []

    for metric_type in SupervisionMetricType:
        metric_inclusions.append(metric_type.value)

    demographic_inclusions = {}

    for key, value in inclusions.items():
        if key not in metric_inclusions:
            demographic_inclusions[key] = value

    total_metric_combos = demographic_metric_combos_count_for_person(person, demographic_inclusions)

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
        duplicated_months_mixed_success: bool = False,
        num_relevant_periods: int = 0,
        include_revocation_analysis: bool = False,
        include_revocation_violation_type_analysis_dimensions: bool = False) -> int:
    """Calculates the expected number of characteristic combinations given the person, the supervision time buckets,
    and the dimensions that should be included in the explosion of feature combinations."""

    combos_for_person = \
        demographic_metric_combos_count_for_person_supervision(person, inclusions)

    # Some test cases above use a different call that doesn't take methodology into account as a dimension
    methodology_multiplier = 1
    if with_methodologies:
        methodology_multiplier *= CALCULATION_METHODOLOGIES

    projected_completion_buckets = [
        bucket for bucket in supervision_time_buckets if isinstance(bucket, ProjectedSupervisionCompletionBucket)
    ]

    successful_completion_sentence_length_buckets = [
        bucket for bucket in projected_completion_buckets
        if bucket.sentence_days_served is not None
        and bucket.successful_completion
        and not bucket.incarcerated_during_sentence
    ]

    population_buckets = [
        bucket for bucket in supervision_time_buckets
        if isinstance(bucket, (RevocationReturnSupervisionTimeBucket, NonRevocationReturnSupervisionTimeBucket))
    ]

    termination_buckets = [
        bucket for bucket in supervision_time_buckets if isinstance(bucket, SupervisionTerminationBucket)
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
        if (projected_completion_bucket.year, projected_completion_bucket.month) in completion_months:
            num_duplicated_projected_completion_months += 1
        if projected_completion_bucket.month:
            completion_months.add((projected_completion_bucket.year, projected_completion_bucket.month))

    num_projected_completion_buckets = len(projected_completion_buckets)

    num_duplicated_projected_completion_sentence_length_months = 0
    completion_sentence_length_months: Set[Tuple[int, int]] = set()

    for completion_sentence_bucket in successful_completion_sentence_length_buckets:
        if (completion_sentence_bucket.year, completion_sentence_bucket.month) in completion_sentence_length_months:
            num_duplicated_projected_completion_sentence_length_months += 1
        if completion_sentence_bucket.month:
            completion_sentence_length_months.add((completion_sentence_bucket.year, completion_sentence_bucket.month))

    num_projected_completion_sentence_length_buckets = len(successful_completion_sentence_length_buckets)

    # Calculate total combos for supervision population
    num_population_buckets = len(population_buckets)

    num_duplicated_termination_buckets = 0
    termination_months: Set[Tuple[int, int]] = set()

    for termination_bucket in termination_buckets:
        year = termination_bucket.year
        month = 0 if termination_bucket.month is None else termination_bucket.month
        if (year, month) in termination_months:
            num_duplicated_termination_buckets += 1
        if month is not None:
            termination_months.add((year, month))

    revocation_buckets = [
        bucket for bucket in supervision_time_buckets
        if isinstance(bucket, RevocationReturnSupervisionTimeBucket)
    ]
    num_revocation_buckets = len(revocation_buckets)

    num_duplicated_revocation_buckets = 0
    revocation_months: Set[Tuple[int, int]] = set()

    for bucket in revocation_buckets:
        year = bucket.year
        month = 0 if bucket.month is None else bucket.month
        if (year, month) in revocation_months:
            num_duplicated_revocation_buckets += 1
        if month is not None:
            revocation_months.add((year, month))

    # Calculates how many dimensions are included in the dimensional explosion by seeing which fields are set on the
    # buckets that populate the characteristic dictionary that gets exploded
    population_dimension_multiplier = 1
    if population_buckets:
        first_population_bucket = population_buckets[0]

        population_dimensions = [
            'supervision_type',
            'case_type',
            'assessment_type',
            'most_severe_violation_type',
            'most_severe_violation_type_subtype',
            'response_count',
            'supervising_officer_external_id',
            'supervising_district_external_id',
            'supervision_level'
        ]

        for dimension in population_dimensions:
            if getattr(first_population_bucket, dimension) is not None:
                # Every dimension that is set on the bucket increases the number of combinations by a factor of 2 (one
                # set of combinations where that dimension is included, and one where it is not)
                population_dimension_multiplier *= 2

        # assessment_score_bucket is always filled out (set to 'NOT_ASSESSED' if assessment data is missing), so it
        # is always included in the dimensional explosion
        population_dimension_multiplier *= 2

    supervision_population_combos = (combos_for_person * methodology_multiplier * num_population_buckets
                                     * population_dimension_multiplier)

    # Supervision population metrics removed in person-based de-duplication
    duplicated_population_combos = int(combos_for_person * num_duplicated_population_buckets
                                       * population_dimension_multiplier)

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
            'supervision_type',
            'case_type',
            'revocation_type',
            'source_violation_type',
            'assessment_type',
            'supervising_officer_external_id',
            'supervising_district_external_id'
        ]

        for dimension in revocation_dimensions:
            if getattr(first_revocation_bucket, dimension) is not None:
                # Every dimension that is set on the bucket increases the number of combinations by a factor of 2 (one
                # set of combinations where that dimension is included, and one where it is not)
                revocation_dimension_multiplier *= 2

        # assessment_score_bucket is always filled out (set to 'NOT_ASSESSED' if assessment data is missing), so it
        # is always included in the dimensional explosion
        revocation_dimension_multiplier *= 2

    supervision_revocation_combos = (combos_for_person * methodology_multiplier * num_revocation_buckets
                                     * revocation_dimension_multiplier)

    duplicated_revocation_combos = int(combos_for_person * num_duplicated_revocation_buckets
                                       * revocation_dimension_multiplier)

    supervision_revocation_combos -= duplicated_revocation_combos

    revocation_analysis_dimension_multiplier = revocation_dimension_multiplier

    if with_revocation_dimensions and revocation_buckets:
        first_revocation_analysis_bucket = revocation_buckets[0]

        revocation_analysis_dimensions = [
            'most_severe_violation_type',
            'most_severe_violation_type_subtype',
            'most_severe_response_decision',
            'response_count'
        ]

        for dimension in revocation_analysis_dimensions:
            if getattr(first_revocation_analysis_bucket, dimension) is not None:
                revocation_analysis_dimension_multiplier *= 2

    supervision_revocation_analysis_combos = 0

    if include_revocation_analysis:
        supervision_revocation_analysis_combos += (combos_for_person * methodology_multiplier
                                                   * num_revocation_buckets * revocation_analysis_dimension_multiplier)

        duplicated_revocation_analysis_combos = int(combos_for_person * num_duplicated_revocation_buckets
                                                    * revocation_analysis_dimension_multiplier)

        supervision_revocation_analysis_combos -= duplicated_revocation_analysis_combos

    revocation_violation_type_analysis_dimension_multiplier = 1
    num_violation_types = 0
    if with_revocation_dimensions and include_revocation_violation_type_analysis_dimensions and revocation_buckets:
        first_revocation_bucket = revocation_buckets[0]

        if first_revocation_bucket.violation_type_frequency_counter:
            violation_type_analysis_dimensions = [
                'supervision_type',
                'case_type',
                'supervision_level',
                'assessment_type',
                'most_severe_violation_type',
            ]

            for dimension in violation_type_analysis_dimensions:
                if getattr(first_revocation_bucket, dimension) is not None:
                    # Every dimension that is set on the bucket increases the number of combinations by a factor of 2
                    # (one set of combinations where that dimension is included, and one where it is not)
                    revocation_violation_type_analysis_dimension_multiplier *= 2

            # assessment_score_bucket is always filled out (set to 'NOT_ASSESSED' if assessment data is missing), so it
            # is always included in the dimensional explosion
            revocation_violation_type_analysis_dimension_multiplier *= 2

            if first_revocation_bucket.response_count is not None:
                revocation_violation_type_analysis_dimension_multiplier *= 2

            for violation_type_list in first_revocation_bucket.violation_type_frequency_counter:
                num_violation_types += len(violation_type_list) + 1

    revocation_violation_type_analysis_combos = 0

    if include_revocation_violation_type_analysis_dimensions:
        revocation_violation_type_analysis_combos += (
            combos_for_person * methodology_multiplier *
            num_revocation_buckets * revocation_violation_type_analysis_dimension_multiplier *
            num_violation_types
        )

    projected_completion_dimension_multiplier = 1
    if projected_completion_buckets:
        first_projected_completion_bucket = projected_completion_buckets[0]

        projected_completion_dimensions = [
            'supervision_type',
            'supervision_level',
            'case_type',
            'supervising_officer_external_id',
            'supervising_district_external_id'
        ]

        for dimension in projected_completion_dimensions:
            if getattr(first_projected_completion_bucket, dimension):
                projected_completion_dimension_multiplier *= 2

    supervision_success_combos = (combos_for_person * methodology_multiplier
                                  * num_projected_completion_buckets * projected_completion_dimension_multiplier)

    # Success metrics removed in person-based de-duplication that don't
    # specify supervision type
    duplicated_success_combos = int(combos_for_person * num_duplicated_projected_completion_months
                                    * projected_completion_dimension_multiplier)

    if duplicated_months_different_supervision_types:
        # If the duplicated months have different supervision types, then
        # don't remove the supervision-type-specific combos
        duplicated_success_combos = int(duplicated_success_combos / 2)

    # Remove the combos that don't specify supervision type for the duplicated
    # projected completion months
    supervision_success_combos -= duplicated_success_combos

    supervision_successful_sentence_length_combos = (combos_for_person * methodology_multiplier
                                                     * num_projected_completion_sentence_length_buckets *
                                                     projected_completion_dimension_multiplier)

    # Successful sentence length metrics removed in person-based de-duplication that don't specify supervision type
    duplicated_successful_sentence_length_combos = int(combos_for_person
                                                       * num_duplicated_projected_completion_sentence_length_months
                                                       * projected_completion_dimension_multiplier)

    if duplicated_months_different_supervision_types and not duplicated_months_mixed_success:
        # If the duplicated months have different supervision types, then don't remove the supervision-type-specific
        # combos
        duplicated_successful_sentence_length_combos = int(duplicated_successful_sentence_length_combos / 2)

    # If the duplicated months have a mix of failure and successful supervision, and the duplicated months are not
    # of different supervision types, then we will not have any person-based successful sentence length combos
    exclude_all_person_based_successful_sentence_length_combos = (
        duplicated_months_mixed_success and not duplicated_months_different_supervision_types
        and num_projected_completion_buckets == 2
    )

    if exclude_all_person_based_successful_sentence_length_combos:
        # Remove all person-based combos
        supervision_successful_sentence_length_combos = int(supervision_successful_sentence_length_combos / 2)

    # Remove the combos that don't specify supervision type for the duplicated projected completion successful
    # supervision months
    supervision_successful_sentence_length_combos -= duplicated_successful_sentence_length_combos

    # Calculate total combos for supervision termination
    termination_dimension_multiplier = 1
    if termination_buckets:
        first_termination_bucket = termination_buckets[0]

        termination_dimensions = [
            'case_type',
            'supervision_type',
            'termination_reason',
            'assessment_type',
            'supervising_officer_external_id',
            'supervising_district_external_id',
            'supervision_level'
        ]

        for dimension in termination_dimensions:
            if getattr(first_termination_bucket, dimension):
                # Every dimension that is set on the bucket increases the number of combinations by a factor of 2
                # (one set of combinations where that dimension is included, and one where it is not)
                termination_dimension_multiplier *= 2

        # assessment_score_bucket is always filled out (set to 'NOT_ASSESSED' if assessment data is missing), so it
        # is always included in the dimensional explosion
        termination_dimension_multiplier *= 2

    num_termination_buckets = len(termination_buckets)

    supervision_termination_combos = (combos_for_person * methodology_multiplier
                                      * num_termination_buckets * termination_dimension_multiplier)

    # Termination metrics removed in person-based de-duplication that don't
    # specify supervision type
    duplicated_termination_combos = int(combos_for_person * num_duplicated_termination_buckets
                                        * termination_dimension_multiplier)

    supervision_termination_combos -= duplicated_termination_combos

    if num_relevant_periods > 0:
        # Population combos
        supervision_population_combos += (combos_for_person
                                          * (len(population_buckets) - num_duplicated_population_buckets)
                                          * population_dimension_multiplier * num_relevant_periods)

        if duplicated_months_different_supervision_types:
            supervision_population_combos += int(combos_for_person * num_duplicated_population_buckets
                                                 * population_dimension_multiplier * num_relevant_periods / 2)

        # Revocation combos
        supervision_revocation_combos += (combos_for_person * len(revocation_buckets)
                                          * revocation_dimension_multiplier * num_relevant_periods)

        if include_revocation_analysis:
            # Other metric buckets
            supervision_revocation_analysis_combos += (combos_for_person * len(revocation_buckets)
                                                       * num_relevant_periods
                                                       * revocation_analysis_dimension_multiplier)

        if include_revocation_violation_type_analysis_dimensions:
            revocation_violation_type_analysis_combos += (
                combos_for_person * num_revocation_buckets *
                revocation_violation_type_analysis_dimension_multiplier * num_violation_types * num_relevant_periods
            )

        # Success combos
        supervision_success_combos += (combos_for_person
                                       * (num_projected_completion_buckets - num_duplicated_projected_completion_months)
                                       * projected_completion_dimension_multiplier * num_relevant_periods)

        if not exclude_all_person_based_successful_sentence_length_combos:
            # Successful sentence length combos
            supervision_successful_sentence_length_combos += (
                combos_for_person
                * (num_projected_completion_sentence_length_buckets -
                   num_duplicated_projected_completion_sentence_length_months)
                * projected_completion_dimension_multiplier * num_relevant_periods)

        # Termination combos
        supervision_termination_combos += (combos_for_person
                                           * (len(termination_buckets) - num_duplicated_termination_buckets)
                                           * termination_dimension_multiplier * num_relevant_periods)

    # Adding person-level metrics
    if supervision_population_combos > 0:
        supervision_population_combos += (num_population_buckets +
                                          (num_population_buckets - num_duplicated_population_buckets)*(
                                              num_relevant_periods + 1))
    if supervision_revocation_combos > 0:
        supervision_revocation_combos += (num_revocation_buckets +
                                          (num_revocation_buckets - num_duplicated_revocation_buckets)*(
                                              num_relevant_periods + 1))
    if supervision_success_combos > 0:
        supervision_success_combos += (num_projected_completion_buckets +
                                       (num_projected_completion_buckets - num_duplicated_projected_completion_months)*(
                                           num_relevant_periods + 1))

    if supervision_successful_sentence_length_combos > 0:
        if duplicated_months_mixed_success:
            # Only add event-based counts for each successful completion bucket
            supervision_successful_sentence_length_combos += len(successful_completion_sentence_length_buckets)
        else:
            supervision_successful_sentence_length_combos += (
                num_projected_completion_sentence_length_buckets +
                (num_projected_completion_sentence_length_buckets -
                 num_duplicated_projected_completion_sentence_length_months) *
                (num_relevant_periods + 1))

    if supervision_termination_combos > 0:
        supervision_termination_combos += (num_termination_buckets +
                                           (num_termination_buckets - num_duplicated_termination_buckets)*(
                                               num_relevant_periods + 1))
    if supervision_revocation_analysis_combos > 0:
        supervision_revocation_analysis_combos += (num_revocation_buckets +
                                                   (num_revocation_buckets - num_duplicated_revocation_buckets)*(
                                                       num_relevant_periods + 1))

    if duplicated_months_mixed_success and duplicated_months_different_supervision_types:
        # If the duplicated months are of mixed success and the duplicated months are of different supervision types,
        # then that means we want to exclude all of the person-based metrics that don't specify supervision type
        # because successful sentence length metrics that don't specify supervision type will not include this person
        # for this month
        supervision_successful_sentence_length_combos -= int(combos_for_person
                                                             * num_projected_completion_sentence_length_buckets *
                                                             (projected_completion_dimension_multiplier / 2))

    # Pick which one is relevant for the test case: some tests above use a different call that only looks at combos for
    # either population or revocation, but not both
    if include_all_metrics:
        return int(supervision_population_combos +
                   supervision_revocation_combos +
                   supervision_success_combos +
                   supervision_successful_sentence_length_combos +
                   supervision_termination_combos +
                   supervision_revocation_analysis_combos +
                   revocation_violation_type_analysis_combos)

    if metric_to_include:
        if metric_to_include == SupervisionMetricType.ASSESSMENT_CHANGE:
            return int(supervision_termination_combos)
        if metric_to_include == SupervisionMetricType.SUCCESS:
            return int(supervision_success_combos)
        if metric_to_include == SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED:
            return int(supervision_successful_sentence_length_combos)
        if metric_to_include == SupervisionMetricType.REVOCATION:
            return int(supervision_revocation_combos)
        if metric_to_include == SupervisionMetricType.REVOCATION_ANALYSIS:
            return int(supervision_revocation_analysis_combos)
        if metric_to_include == SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS:
            return int(revocation_violation_type_analysis_combos)
        if metric_to_include == SupervisionMetricType.POPULATION:
            return int(supervision_population_combos)

    return 0


class TestIncludeDimensionsFunctions(unittest.TestCase):
    """Tests the various functions that determine which dimensions should be included for the given metric."""

    def test_include_assessment_dimensions_for_metric(self):
        for metric in SupervisionMetricType:
            # Assert this does not fail for all possible metric types
            _ = calculator._include_assessment_dimensions_for_metric(metric)

    def test_include_person_level_dimensions_for_metric(self):
        for metric in SupervisionMetricType:
            # Assert this does not fail for all possible metric types
            _ = calculator._include_person_level_dimensions_for_metric(metric)

    def test_include_revocation_dimensions_for_metric(self):
        for metric in SupervisionMetricType:
            # Assert this does not fail for all possible metric types
            _ = calculator._include_revocation_dimensions_for_metric(metric)
