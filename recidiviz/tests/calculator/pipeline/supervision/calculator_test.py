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
from typing import List, Tuple, Set, Sequence

from freezegun import freeze_time

from recidiviz.calculator.pipeline.supervision import calculator
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetricType, SupervisionRevocationMetric, SupervisionCaseComplianceMetric, SupervisionPopulationMetric, \
    SupervisionRevocationAnalysisMetric, SupervisionRevocationViolationTypeAnalysisMetric, SupervisionSuccessMetric, \
    SuccessfulSupervisionSentenceDaysServedMetric, SupervisionTerminationMetric
from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import SupervisionCaseCompliance
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
from recidiviz.tests.calculator.calculator_test_utils import combo_has_enum_value_for_key

ALL_METRICS_INCLUSIONS_DICT = {
    metric_type: True for metric_type in SupervisionMetricType
}

CALCULATION_METHODOLOGIES = len(MetricMethodologyType)


# TODO(#2732): Implement more full test coverage of the officer, district, the supervision success functionality
class TestMapSupervisionCombinations(unittest.TestCase):
    """Tests the map_supervision_combinations function."""

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
                bucket_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=4,
                bucket_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

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
                bucket_date=date(2018, 3, 31),
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
                bucket_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

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
                bucket_date=date(2018, 3, 31),
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
                bucket_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

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
                bucket_date=date(2018, 3, 31),
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
                bucket_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.VERY_HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                supervising_officer_external_id='143',
                supervising_district_external_id='DISTRICT X')
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

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
                bucket_date=date(2018, 3, 31),
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
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-03',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)
        assert all(combo.get('supervision_level_raw_text') is not None for combo, value in supervision_combinations
                   if combo.get('person_id') is not None
                   and combo_has_enum_value_for_key(combo, 'metric_type', SupervisionMetricType.SUPERVISION_POPULATION))

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
                bucket_date=date(2018, 3, 31),
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
                bucket_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=4,
                bucket_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations
                   if not combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED))
        assert all(value == 998 for _combination, value in supervision_combinations
                   if combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED))

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
                bucket_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=False,
                incarcerated_during_sentence=True,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=3,
                bucket_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=4,
                bucket_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH'
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations if not
                   combo_has_enum_value_for_key(_combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESS))
        assert all(value == 0 for _combination, value
                   in supervision_combinations if
                   combo_has_enum_value_for_key(_combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESS))
        assert all(not combo_has_enum_value_for_key(
            _combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED)
                   for _combination, _ in supervision_combinations)

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
                state_code='US_ND', year=2010, month=2,
                bucket_date=date(2010, 2, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=False,
                incarcerated_during_sentence=True,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_ND', year=2010, month=2,
                bucket_date=date(2010, 2, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=199,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2010, month=2,
                bucket_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=4,
                bucket_date=date(2010, 4, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            duplicated_months_mixed_success=True
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations
                   if not combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED)
                   and (not combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESS)
                        or combo_has_enum_value_for_key(_combination, 'supervision_type',
                                                        StateSupervisionPeriodSupervisionType.PROBATION)))
        assert all(value == 0 for _combination, value
                   in supervision_combinations if
                   combo_has_enum_value_for_key(_combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESS)
                   and not
                   combo_has_enum_value_for_key(_combination, 'supervision_type',
                                                StateSupervisionPeriodSupervisionType.PROBATION) and not
                   _combination.get('supervision_type') is None)
        assert all(not combo_has_enum_value_for_key(_combination, 'metric_type',
                                                    SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED)
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
                state_code='US_MO', year=2017, month=6, bucket_date=date(2017, 6, 30),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False,
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2020, month=1, bucket_date=date(2020, 1, 30),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False,
                supervising_officer_external_id='officer45', supervising_district_external_id='district5'
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=12, bucket_date=date(2018, 12, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1
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
                bucket_date=date(2018, 2, 1),
                is_on_supervision_last_day_of_month=False,
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
                bucket_date=date(2018, 3, 31),
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
                bucket_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
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
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

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
                bucket_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 4, date(2018, 4, 1), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 4, date(2018, 4, 2), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 4, date(2018, 4, 3), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 4, date(2018, 4, 4), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-07',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

    def test_map_supervision_combinations_overlapping_days(self):
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
                'US_MO', 2018, 3, date(2018, 3, 1), StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 3, date(2018, 3, 1), StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2018, 4, date(2018, 4, 1), StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

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
                bucket_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2010, month=3,
                bucket_date=date(2010, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=4,
                bucket_date=date(2010, 4, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=5,
                bucket_date=date(2010, 5, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-05',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

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
                state_code='US_ND', year=2010, month=2,
                bucket_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2010, month=2,
                bucket_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2010, month=2,
                bucket_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=4,
                bucket_date=date(2010, 4, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=4,
                bucket_date=date(2010, 4, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=4,
                bucket_date=date(2010, 4, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)
        assert all(_combination.get('supervision_type') == StateSupervisionPeriodSupervisionType.DUAL
                   for _combination, _ in supervision_combinations
                   if _combination.get('person_id') is not None
                   and _combination.get('methodology') == MetricMethodologyType.PERSON)

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
                bucket_date=date(2018, 3, 13),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervising_officer_external_id='FAKE_ID_1'
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code='US_ND', year=2018, month=3,
                bucket_date=date(2018, 3, 29),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervising_officer_external_id='FAKE_ID_2'
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-03',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)
        assert all(_combination.get('supervising_officer_external_id') == 'FAKE_ID_2'
                   or _combination.get('supervising_officer_external_id') is None
                   for _combination, _ in supervision_combinations
                   if _combination.get('methodology') == MetricMethodologyType.PERSON
                   and _combination.get('metric_type') != SupervisionMetricType.SUPERVISION_POPULATION)

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
                'US_MO', 2010, 1, date(2010, 1, 1), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
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
            bucket_date=date(2010, 1, 1),
            is_on_supervision_last_day_of_month=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        not_relevant_bucket = RevocationReturnSupervisionTimeBucket(
            state_code='US_MO',
            year=1980,
            month=1,
            bucket_date=date(1980, 1, 1),
            is_on_supervision_last_day_of_month=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)

        supervision_time_buckets = [relevant_bucket, not_relevant_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1)

        # Get the expected count for the relevant bucket without revocation analysis metrics
        expected_combinations_count = expected_metric_combos_count(
            [relevant_bucket], num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
        )
        # Get the expected count for the non-relevant revocation bucket with revocation analysis metrics
        expected_combinations_count_not_relevant = expected_metric_combos_count([not_relevant_bucket])

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
                bucket_date=date(1980, 1, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
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
                'US_MO', 2010, 1, date(2010, 1, 1), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, date(2010, 1, 1), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS)
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

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
                bucket_date=date(2010, 1, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=190
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                bucket_date=date(2010, 1, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=False,
                incarcerated_during_sentence=False,
                sentence_days_served=195
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
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
                       _combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED))

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
                'US_ND', 2010, 1, date(2010, 1, 1), StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_ND', 2010, 1, date(2010, 1, 1), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_ND', 2010, 1, date(2010, 1, 1), StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False)
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets, num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS)
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)
        assert all(_combination.get('supervision_type') == StateSupervisionPeriodSupervisionType.DUAL
                   for _combination, _ in supervision_combinations
                   if _combination.get('person_id') is not None
                   and _combination.get('methodology') == MetricMethodologyType.PERSON)

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
                bucket_date=date(2010, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=12,
                assessment_score_change=-3
            ),
            SupervisionTerminationBucket(
                state_code='US_ND',
                year=2010,
                month=1,
                bucket_date=date(2010, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=12,
                assessment_score_change=-3
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS)
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(_combination['assessment_score_change'] == -3 for _combination, _
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
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                bucket_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
                case_compliance=SupervisionCaseCompliance(
                    date_of_evaluation=date(2010, 1, 31),
                    assessment_up_to_date=True,
                    face_to_face_frequency_sufficient=True
                )
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                bucket_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
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
                bucket_date=date(2010, 1, 16),
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
                bucket_date=date(2010, 1, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=234
            )
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
            include_revocation_violation_type_analysis_dimensions=True
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations
                   if not combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED))
        assert all(_combination['assessment_score_change'] == -3 for _combination, value
                   in supervision_combinations
                   if combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUPERVISION_TERMINATION))
        assert all(value == 234 for _combination, value
                   in supervision_combinations
                   if combo_has_enum_value_for_key(
                       _combination, 'metric_type', SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED))

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
            bucket_date=date(2000, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [termination_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2000-01',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(_combination['assessment_score_change'] == -9 for _combination, _
                   in supervision_combinations)

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
            bucket_date=date(2000, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=None
        )

        supervision_time_buckets = [termination_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2000-01',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(
            [termination_bucket]
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(_combination.get('assessment_score_change') is None for _combination, _
                   in supervision_combinations)

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
            bucket_date=date(2000, 1, 13),
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
            bucket_date=date(2000, 1, 13),
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
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2000-01',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(_combination['assessment_score_change'] == -9 for _combination, _
                   in supervision_combinations)

    def test_map_supervision_combinations_only_terminations(self):
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
            year=2010,
            month=1,
            bucket_date=date(2010, 1, 13),
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
                state_code='US_MO', year=2010, month=3,
                bucket_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, incarcerated_during_sentence=False,
                sentence_days_served=398,
                supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, date(2010, 1, 1), StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, date(2010, 1, 2), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False)
        ]

        inclusions_dict = {
            SupervisionMetricType.SUPERVISION_TERMINATION: True,
            SupervisionMetricType.SUPERVISION_SUCCESS: False,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED: False,
            SupervisionMetricType.SUPERVISION_REVOCATION: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_POPULATION: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict,
            calculation_end_month='2010-12',
            calculation_month_count=12
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_TERMINATION
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(_combination['assessment_score_change'] == termination_bucket.assessment_score_change
                   for _combination, _
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.SUPERVISION_TERMINATION)
            for _combination, value in supervision_combinations)

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
            year=2010,
            month=1,
            bucket_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2010, month=3,
                bucket_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, date(2010, 1, 1), StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, date(2010, 1, 2), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False)
        ]

        inclusions_dict = {
            SupervisionMetricType.SUPERVISION_TERMINATION: False,
            SupervisionMetricType.SUPERVISION_SUCCESS: True,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED: False,
            SupervisionMetricType.SUPERVISION_REVOCATION: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_POPULATION: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict,
            calculation_end_month='2010-12',
            calculation_month_count=12
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_SUCCESS
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.SUPERVISION_SUCCESS)
            for _combination, value in supervision_combinations)

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
            year=2010,
            month=1,
            bucket_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2010, month=3,
                bucket_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, incarcerated_during_sentence=False,
                sentence_days_served=500,
                supervising_officer_external_id='officer45', supervising_district_external_id='district5'
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, date(2010, 1, 1), StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False),
            NonRevocationReturnSupervisionTimeBucket(
                'US_MO', 2010, 1, date(2010, 1, 2), StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False)
        ]

        inclusions_dict = {
            SupervisionMetricType.SUPERVISION_TERMINATION: False,
            SupervisionMetricType.SUPERVISION_SUCCESS: False,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED: True,
            SupervisionMetricType.SUPERVISION_REVOCATION: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_POPULATION: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict,
            calculation_end_month='2010-12',
            calculation_month_count=12
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 500 for _combination, value in supervision_combinations)
        assert all(combo_has_enum_value_for_key(_combination, 'metric_type',
                                                SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED)
                   for _combination, value in supervision_combinations)

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
            year=2010,
            month=1,
            bucket_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2010, month=3,
                bucket_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                bucket_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010, month=2,
                bucket_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        ]

        inclusions_dict = {
            SupervisionMetricType.SUPERVISION_TERMINATION: False,
            SupervisionMetricType.SUPERVISION_SUCCESS: False,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED: False,
            SupervisionMetricType.SUPERVISION_REVOCATION: True,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_POPULATION: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict,
            calculation_end_month='2010-12',
            calculation_month_count=12
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_REVOCATION
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.SUPERVISION_REVOCATION)
            for _combination, value in supervision_combinations)

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
            year=2010,
            month=1,
            bucket_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2010, month=3,
                bucket_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                bucket_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010, month=2,
                bucket_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        ]

        inclusions_dict = {
            SupervisionMetricType.SUPERVISION_TERMINATION: False,
            SupervisionMetricType.SUPERVISION_SUCCESS: False,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED: False,
            SupervisionMetricType.SUPERVISION_REVOCATION: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: True,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_POPULATION: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict,
            calculation_end_month='2010-12',
            calculation_month_count=12
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS)
            for _combination, value in supervision_combinations)

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
            year=2010,
            month=1,
            bucket_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2010, month=3,
                bucket_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2010, month=1,
                bucket_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
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
                bucket_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)
        ]

        inclusions_dict = {
            SupervisionMetricType.SUPERVISION_TERMINATION: False,
            SupervisionMetricType.SUPERVISION_SUCCESS: False,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED: False,
            SupervisionMetricType.SUPERVISION_REVOCATION: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS: True,
            SupervisionMetricType.SUPERVISION_POPULATION: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict,
            calculation_end_month='2010-12',
            calculation_month_count=12
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            include_all_metrics=False,
            include_revocation_violation_type_analysis_dimensions=True,
            metric_to_include=SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)
        assert all(combo_has_enum_value_for_key(_combination, 'metric_type',
                                                SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS)
                   for _combination, value in supervision_combinations)
        assert all(_combination.get('violation_count_type') is not None
                   for _combination, value in supervision_combinations)
        assert all(_combination.get('person_id') is None for _combination, value in supervision_combinations)
        assert all('person_id' not in _combination.keys() for _combination, value in supervision_combinations)

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
            year=2010,
            month=1,
            bucket_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2010, month=3, bucket_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, supervising_officer_external_id='officer45',
                supervising_district_external_id='district5'
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010,
                month=1,
                bucket_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=2010,
                month=2,
                bucket_date=date(2010, 2, 22),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION)
        ]

        inclusions_dict = {
            SupervisionMetricType.SUPERVISION_TERMINATION: False,
            SupervisionMetricType.SUPERVISION_SUCCESS: False,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED: False,
            SupervisionMetricType.SUPERVISION_REVOCATION: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_POPULATION: True
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict,
            calculation_end_month='2010-12',
            calculation_month_count=12
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_POPULATION
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1
                   for _combination, value
                   in supervision_combinations)
        assert all(
            combo_has_enum_value_for_key(
                _combination, 'metric_type',
                SupervisionMetricType.SUPERVISION_POPULATION)
            for _combination, value in supervision_combinations)

    @freeze_time('2010-01-31')
    def test_map_supervision_combinations_relevant_periods_calculation_month_count_12(self):
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

        included_bucket = RevocationReturnSupervisionTimeBucket(
            'US_MO', 2010, 1, date(2010, 1, 1),
            StateSupervisionPeriodSupervisionType.PAROLE,
            is_on_supervision_last_day_of_month=False
        )

        not_included_bucket = RevocationReturnSupervisionTimeBucket(
            'US_MO', 2005, 1, date(2010, 1, 3),
            StateSupervisionPeriodSupervisionType.PAROLE,
            is_on_supervision_last_day_of_month=False
        )

        supervision_time_buckets = [included_bucket, not_included_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=12
        )

        expected_combinations_count = expected_metric_combos_count(
            [included_bucket], num_relevant_periods=len(calculator_utils.METRIC_PERIOD_MONTHS),
        )

        self.assertEqual(expected_combinations_count,
                         len(supervision_combinations))
        assert all(value == 1 for _combination, value
                   in supervision_combinations)

    @freeze_time('2010-12-31')
    def test_map_supervision_combinations_relevant_periods_calculation_month_count_37(self):
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
        included_bucket = RevocationReturnSupervisionTimeBucket(
            'US_MO', 2007, 12, date(2007, 12, 1),
            StateSupervisionPeriodSupervisionType.PAROLE,
            is_on_supervision_last_day_of_month=False)

        supervision_time_buckets = [included_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=37
        )

        expected_combinations_count = expected_metric_combos_count([included_bucket])

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
        included_bucket = RevocationReturnSupervisionTimeBucket(
            'US_MO', 2007, 12, date(2007, 12, 8),
            StateSupervisionPeriodSupervisionType.PAROLE, is_on_supervision_last_day_of_month=False)

        supervision_time_buckets = [included_bucket]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=12
        )

        self.assertEqual(0, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)

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
                bucket_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, incarcerated_during_sentence=False,
                sentence_days_served=500,
                supervising_officer_external_id='officer45', supervising_district_external_id='district5'
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO', year=2018, month=3,
                bucket_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True, incarcerated_during_sentence=False,
                sentence_days_served=501,
                supervising_officer_external_id='officer45', supervising_district_external_id='district5'
            )
        ]

        inclusions_dict = {
            SupervisionMetricType.SUPERVISION_TERMINATION: False,
            SupervisionMetricType.SUPERVISION_SUCCESS: False,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED: True,
            SupervisionMetricType.SUPERVISION_REVOCATION: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS: False,
            SupervisionMetricType.SUPERVISION_POPULATION: False
        }

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, inclusions_dict,
            calculation_end_month='2018-03',
            calculation_month_count=1
        )

        expected_combinations_count = expected_metric_combos_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
        )

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 501 for _combination, value in supervision_combinations
                   if _combination.get('methodology') == MetricMethodologyType.PERSON)
        assert all(combo_has_enum_value_for_key(_combination, 'metric_type',
                                                SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED)
                   for _combination, value in supervision_combinations)

    def test_map_supervision_combinations_compliance_metrics(self):
        """Tests the map_supervision_combinations function when there are compliance metrics to be generated."""
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
                bucket_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH',
                case_compliance=SupervisionCaseCompliance(
                    date_of_evaluation=date(2018, 3, 31),
                    assessment_up_to_date=True,
                    face_to_face_frequency_sufficient=False
                )
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO', year=2018, month=4,
                bucket_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='HIGH',
                case_compliance=SupervisionCaseCompliance(
                    date_of_evaluation=date(2018, 4, 30),
                    assessment_up_to_date=False,
                    face_to_face_frequency_sufficient=False
                )
            ),
        ]

        supervision_combinations = calculator.map_supervision_combinations(
            person, supervision_time_buckets, ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month='2018-04',
            calculation_month_count=-1
        )

        expected_combinations_count = expected_metric_combos_count(supervision_time_buckets)

        self.assertEqual(expected_combinations_count, len(supervision_combinations))
        assert all(value == 1 for _combination, value in supervision_combinations)


class TestCharacteristicCombinations(unittest.TestCase):
    """Tests the characteristic_combinations function."""

    def test_characteristic_combinations_compliance(self):
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
            bucket_date=date(2018, 3, 1),
            is_on_supervision_last_day_of_month=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            response_count=5,
            most_severe_violation_type=StateSupervisionViolationType.ABSCONDED,
            case_compliance=SupervisionCaseCompliance(
                date_of_evaluation=date(2018, 3, 1),
                assessment_count=1,
                assessment_up_to_date=True,
                face_to_face_count=1,
                face_to_face_frequency_sufficient=True
            )
        )

        characteristics_dict = calculator.characteristics_dict(
            person, supervision_time_bucket, metric_class=SupervisionCaseComplianceMetric)

        expected_output = {
            'age_bucket': '30-34',
            'assessment_score_bucket': 'NOT_ASSESSED',
            'assessment_count': 1,
            'assessment_up_to_date': True,
            'ethnicity': [Ethnicity.NOT_HISPANIC],
            'face_to_face_count': 1,
            'face_to_face_frequency_sufficient': True,
            'gender': Gender.FEMALE,
            'most_severe_violation_type': StateSupervisionViolationType.ABSCONDED,
            'person_id': 12345,
            'race': [Race.WHITE],
            'response_count': 5,
            'supervision_type': StateSupervisionPeriodSupervisionType.PAROLE,
            'date_of_evaluation': date(2018, 3, 1),
            'date_of_supervision': date(2018, 3, 1),
            'is_on_supervision_last_day_of_month': False,
        }

        self.assertEqual(expected_output, characteristics_dict)

    def test_characteristic_combinations_population(self):
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
            bucket_date=date(2018, 3, 1),
            is_on_supervision_last_day_of_month=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            response_count=5,
            most_severe_violation_type=StateSupervisionViolationType.ABSCONDED,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH
        )

        characteristics_dict = calculator.characteristics_dict(
            person, supervision_time_bucket, metric_class=SupervisionPopulationMetric)

        expected_output = {
            'supervision_type': StateSupervisionPeriodSupervisionType.PAROLE,
            'assessment_score_bucket': 'HIGH',
            'assessment_type': StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            'age_bucket': '30-34',
            'gender': Gender.FEMALE,
            'race': [Race.WHITE],
            'ethnicity': [Ethnicity.NOT_HISPANIC],
            'person_id': 12345,
            'is_on_supervision_last_day_of_month': False,
            'date_of_supervision': date(2018, 3, 1),
            'response_count': 5,
            'most_severe_violation_type': StateSupervisionViolationType.ABSCONDED,
        }

        self.assertEqual(expected_output, characteristics_dict)

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
            bucket_date=date(2018, 3, 1),
            is_on_supervision_last_day_of_month=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            response_count=5,
            most_severe_violation_type=StateSupervisionViolationType.ABSCONDED
        )

        characteristics_dict = calculator.characteristics_dict(
            person, supervision_time_bucket, metric_class=SupervisionRevocationMetric)

        expected_output = {
            'revocation_admission_date': date(2018, 3, 1),
            'supervision_type': StateSupervisionPeriodSupervisionType.PAROLE,
            'assessment_score_bucket': 'NOT_ASSESSED',
            'age_bucket': '30-34',
            'gender': Gender.FEMALE,
            'race': [Race.WHITE],
            'ethnicity': [Ethnicity.NOT_HISPANIC],
            'person_id': 12345,
        }

        self.assertEqual(expected_output, characteristics_dict)

    def test_characteristic_combinations_revocation_analysis(self):
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
            bucket_date=date(2018, 3, 1),
            is_on_supervision_last_day_of_month=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            response_count=5,
            most_severe_violation_type=StateSupervisionViolationType.ABSCONDED,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION
        )

        characteristics_dict = calculator.characteristics_dict(
            person, supervision_time_bucket, metric_class=SupervisionRevocationAnalysisMetric)

        expected_output = {
            'revocation_admission_date': date(2018, 3, 1),
            'supervision_type': StateSupervisionPeriodSupervisionType.PAROLE,
            'assessment_score_bucket': 'NOT_ASSESSED',
            'age_bucket': '30-34',
            'gender': Gender.FEMALE,
            'race': [Race.WHITE],
            'ethnicity': [Ethnicity.NOT_HISPANIC],
            'person_id': 12345,
            'response_count': 5,
            'most_severe_violation_type': StateSupervisionViolationType.ABSCONDED,
            'most_severe_response_decision': StateSupervisionViolationResponseDecision.REVOCATION,
        }

        self.assertEqual(expected_output, characteristics_dict)

    def test_characteristic_combinations_revocation_violation_type_analysis(self):
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
            bucket_date=date(2018, 3, 1),
            is_on_supervision_last_day_of_month=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            response_count=5,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            violation_history_description='1fel;2misd',
            violation_type_frequency_counter=[
                ['FELONY', 'LAW'],
                ['MISD', 'WEA', 'EMP'],
                ['MISD', 'DRG', 'ASC']
            ]
        )

        characteristics_dict = calculator.characteristics_dict(
            person, supervision_time_bucket, metric_class=SupervisionRevocationViolationTypeAnalysisMetric)

        expected_output = {
            'assessment_score_bucket': 'NOT_ASSESSED',
            'supervision_type': StateSupervisionPeriodSupervisionType.PAROLE,
            'response_count': 5,
            'most_severe_violation_type': StateSupervisionViolationType.FELONY,
        }

        self.assertEqual(expected_output, characteristics_dict)

    def test_characteristic_combinations_supervision_success(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = ProjectedSupervisionCompletionBucket(
            state_code='US_MO', year=2018, month=3,
            bucket_date=date(2018, 3, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        characteristics_dict = calculator.characteristics_dict(
            person, supervision_time_bucket, metric_class=SupervisionSuccessMetric)

        expected_output = {
            'age_bucket': '30-34',
            'supervision_type': StateSupervisionPeriodSupervisionType.PAROLE,
            'ethnicity': [Ethnicity.NOT_HISPANIC],
            'gender': Gender.FEMALE,
            'person_id': 12345,
            'race': [Race.WHITE],
        }

        self.assertEqual(expected_output, characteristics_dict)

    def test_characteristic_combinations_supervision_successful_sentence_days_served(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = ProjectedSupervisionCompletionBucket(
            state_code='US_MO', year=2018, month=3,
            bucket_date=date(2018, 3, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        characteristics_dict = calculator.characteristics_dict(
            person, supervision_time_bucket, metric_class=SuccessfulSupervisionSentenceDaysServedMetric)

        expected_output = {
            'age_bucket': '30-34',
            'supervision_type': StateSupervisionPeriodSupervisionType.PAROLE,
            'ethnicity': [Ethnicity.NOT_HISPANIC],
            'gender': Gender.FEMALE,
            'person_id': 12345,
            'race': [Race.WHITE],
        }

        self.assertEqual(expected_output, characteristics_dict)

    def test_characteristic_combinations_termination(self):
        person = StatePerson.new_with_defaults(person_id=12345,
                                               birthdate=date(1984, 8, 31),
                                               gender=Gender.FEMALE)

        race = StatePersonRace.new_with_defaults(state_code='US_MO', race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code='US_MO',
            ethnicity=Ethnicity.NOT_HISPANIC)

        person.ethnicities = [ethnicity]

        supervision_time_bucket = SupervisionTerminationBucket(
            state_code='US_MO', year=2018, month=3,
            bucket_date=date(2018, 3, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            response_count=5,
            most_severe_violation_type=StateSupervisionViolationType.ABSCONDED,
        )

        characteristics_dict = calculator.characteristics_dict(
            person, supervision_time_bucket, metric_class=SupervisionTerminationMetric)

        expected_output = {
            'assessment_score_bucket': 'NOT_ASSESSED',
            'age_bucket': '30-34',
            'supervision_type': StateSupervisionPeriodSupervisionType.PAROLE,
            'ethnicity': [Ethnicity.NOT_HISPANIC],
            'gender': Gender.FEMALE,
            'person_id': 12345,
            'race': [Race.WHITE],
            'termination_date': date(2018, 3, 1),
            'response_count': 5,
            'most_severe_violation_type': StateSupervisionViolationType.ABSCONDED
        }

        self.assertEqual(expected_output, characteristics_dict)


def expected_metric_combos_count(
        supervision_time_buckets: List[SupervisionTimeBucket],
        with_revocation_dimensions: bool = True,
        with_methodologies: bool = True,
        include_all_metrics: bool = True,
        metric_to_include: SupervisionMetricType = None,
        duplicated_months_mixed_success: bool = False,
        num_relevant_periods: int = 0,
        include_revocation_violation_type_analysis_dimensions: bool = False) -> int:
    """Calculates the expected number of characteristic combinations given the supervision time buckets,
    and the metrics and methodologies that should be included in the counts."""
    combos_for_person = 1

    # Some test cases above use a different call that doesn't take methodology into account as a dimension
    methodology_multiplier = 1
    if with_methodologies:
        methodology_multiplier *= CALCULATION_METHODOLOGIES

    # Organize buckets by type of metrics they contribute to
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

    revocation_buckets = [
        bucket for bucket in supervision_time_buckets
        if isinstance(bucket, RevocationReturnSupervisionTimeBucket)
    ]

    compliance_buckets = [
        bucket for bucket in supervision_time_buckets
        if isinstance(bucket, NonRevocationReturnSupervisionTimeBucket)
        and bucket.case_compliance is not None
    ]

    termination_buckets = [
        bucket for bucket in supervision_time_buckets if isinstance(bucket, SupervisionTerminationBucket)
    ]

    # Count number of buckets and number of buckets in the same month
    num_projected_completion_sentence_length_buckets = len(successful_completion_sentence_length_buckets)
    num_duplicated_projected_completion_sentence_length_months = _duplicated_months(
        successful_completion_sentence_length_buckets)

    num_projected_completion_buckets = len(projected_completion_buckets)
    num_duplicated_projected_completion_months = _duplicated_months(projected_completion_buckets)

    num_population_buckets = len(population_buckets)
    num_duplicated_population_buckets = _duplicated_days(population_buckets)

    num_compliance_buckets = len(compliance_buckets)
    num_duplicated_compliance_buckets = _duplicated_months(compliance_buckets)

    num_revocation_buckets = len(revocation_buckets)
    num_duplicated_revocation_buckets = _duplicated_months(revocation_buckets)

    num_termination_buckets = len(termination_buckets)
    num_duplicated_termination_buckets = _duplicated_months(termination_buckets)

    revocation_violation_type_analysis_dimension_multiplier = 1
    num_violation_types = 0
    if with_revocation_dimensions and include_revocation_violation_type_analysis_dimensions and revocation_buckets:
        first_revocation_bucket = revocation_buckets[0]

        if first_revocation_bucket.violation_type_frequency_counter:
            for violation_type_list in first_revocation_bucket.violation_type_frequency_counter:
                num_violation_types += len(violation_type_list) + 1

    revocation_violation_type_analysis_combos = 0

    if include_revocation_violation_type_analysis_dimensions:
        revocation_violation_type_analysis_combos += (
            combos_for_person * methodology_multiplier *
            num_revocation_buckets * revocation_violation_type_analysis_dimension_multiplier *
            num_violation_types
        )

        if num_relevant_periods > 0:
            revocation_violation_type_analysis_combos += (
                combos_for_person * num_revocation_buckets *
                revocation_violation_type_analysis_dimension_multiplier * num_violation_types * num_relevant_periods
            )

    # Person-level metrics for the metric types that limit to only person-output
    supervision_population_combos = (num_population_buckets +
                                     (num_population_buckets - num_duplicated_population_buckets))
    supervision_compliance_combos = (num_compliance_buckets +
                                     (num_compliance_buckets - num_duplicated_compliance_buckets))
    supervision_revocation_combos = (num_revocation_buckets +
                                     (num_revocation_buckets - num_duplicated_revocation_buckets)*(
                                         num_relevant_periods + 1))
    supervision_success_combos = (num_projected_completion_buckets +
                                  (num_projected_completion_buckets - num_duplicated_projected_completion_months)*(
                                      num_relevant_periods + 1))

    if duplicated_months_mixed_success:
        # Only add event-based counts for each successful completion bucket
        supervision_successful_sentence_length_combos = len(successful_completion_sentence_length_buckets)
    else:
        supervision_successful_sentence_length_combos = (
            num_projected_completion_sentence_length_buckets +
            (num_projected_completion_sentence_length_buckets -
             num_duplicated_projected_completion_sentence_length_months) *
            (num_relevant_periods + 1))

    supervision_termination_combos = (num_termination_buckets +
                                      (num_termination_buckets - num_duplicated_termination_buckets)*(
                                          num_relevant_periods + 1))
    supervision_revocation_analysis_combos = (num_revocation_buckets +
                                              (num_revocation_buckets - num_duplicated_revocation_buckets)*(
                                                  num_relevant_periods + 1))

    # Pick which one is relevant for the test case: some tests above use a different call that only looks at combos for
    # either population or revocation, but not both
    if include_all_metrics:
        return int(supervision_population_combos +
                   supervision_revocation_combos +
                   supervision_success_combos +
                   supervision_successful_sentence_length_combos +
                   supervision_termination_combos +
                   supervision_revocation_analysis_combos +
                   revocation_violation_type_analysis_combos +
                   supervision_compliance_combos)

    if metric_to_include:
        if metric_to_include == SupervisionMetricType.SUPERVISION_TERMINATION:
            return int(supervision_termination_combos)
        if metric_to_include == SupervisionMetricType.SUPERVISION_COMPLIANCE:
            return int(supervision_compliance_combos)
        if metric_to_include == SupervisionMetricType.SUPERVISION_SUCCESS:
            return int(supervision_success_combos)
        if metric_to_include == SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED:
            return int(supervision_successful_sentence_length_combos)
        if metric_to_include == SupervisionMetricType.SUPERVISION_REVOCATION:
            return int(supervision_revocation_combos)
        if metric_to_include == SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS:
            return int(supervision_revocation_analysis_combos)
        if metric_to_include == SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS:
            return int(revocation_violation_type_analysis_combos)
        if metric_to_include == SupervisionMetricType.SUPERVISION_POPULATION:
            return int(supervision_population_combos)

    return 0


def _duplicated_months(supervision_time_buckets: Sequence[SupervisionTimeBucket]) -> int:
    duplicated_months = 0
    months: Set[Tuple[int, int]] = set()

    for bucket in supervision_time_buckets:
        year = bucket.year
        month = 0 if bucket.month is None else bucket.month
        if (year, month) in months:
            duplicated_months += 1
        if month is not None:
            months.add((year, month))

    return duplicated_months


def _duplicated_days(supervision_time_buckets: Sequence[SupervisionTimeBucket]) -> int:
    duplicated_days = 0
    days: Set[date] = set()

    for bucket in supervision_time_buckets:
        if bucket.bucket_date in days:
            duplicated_days += 1
        if bucket.bucket_date is not None:
            days.add(bucket.bucket_date)

    return duplicated_days
