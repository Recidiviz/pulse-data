# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
from collections import defaultdict
from datetime import date
from typing import List, Dict
from unittest import mock

from freezegun import freeze_time

from recidiviz.calculator.pipeline.supervision import metric_producer
from recidiviz.calculator.pipeline.supervision.identifier import BUCKET_TYPES_FOR_METRIC
from recidiviz.calculator.pipeline.supervision.metrics import (
    SupervisionMetricType,
    SupervisionPopulationMetric,
    SupervisionSuccessMetric,
    SupervisionRevocationMetric,
    SuccessfulSupervisionSentenceDaysServedMetric,
    SupervisionOutOfStatePopulationMetric,
    SupervisionTerminationMetric,
)
from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import (
    NonRevocationReturnSupervisionTimeBucket,
    SupervisionTimeBucket,
    RevocationReturnSupervisionTimeBucket,
    ProjectedSupervisionCompletionBucket,
    SupervisionTerminationBucket,
    SupervisionStartBucket,
)
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.common.constants.person_characteristics import Gender, Race, Ethnicity
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentType,
    StateAssessmentLevel,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType as ViolationType,
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseRevocationType as RevocationType,
    StateSupervisionViolationResponseDecision,
)
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonRace,
    StatePersonEthnicity,
    StatePersonExternalId,
)


ALL_METRICS_INCLUSIONS_DICT = {
    metric_type: True for metric_type in SupervisionMetricType
}

_DEFAULT_PERSON_METADATA = PersonMetadata(prioritized_race_or_ethnicity="BLACK")
_PIPELINE_JOB_ID = "TEST_JOB_ID"


# TODO(#2732): Implement more full test coverage of the officer, district, the supervision success functionality
class TestProduceSupervisionMetrics(unittest.TestCase):
    """Tests the produce_supervision_metrics function."""

    def test_produce_supervision_metrics(self):
        """Tests the produce_supervision_metrics function."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_assessment(self):
        """Tests the produce_supervision_metrics function when there is assessment data present."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_exclude_assessment(self):
        """Tests the produce_supervision_metrics function when there is assessment data present, but it should not
        be included for this state and pipeline type."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE,
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_supervising_officer_district(self):
        """Tests the produce_supervision_metrics function when there is supervising officer and district data
        present."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.VERY_HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                supervising_officer_external_id="143",
                supervising_district_external_id="DISTRICT X",
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.VERY_HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                supervising_officer_external_id="143",
                supervising_district_external_id="DISTRICT X",
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))

    def test_map_supervision_revocation_combinations(self):
        """Tests the produce_supervision_metrics function for a revocation month."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id="SID1341", id_type="US_MO_DOC", state_code="US_XX"
        )

        person.external_ids = [person_external_id]

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=12,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                revocation_type=RevocationType.SHOCK_INCARCERATION,
                most_severe_violation_type=ViolationType.FELONY,
                most_severe_violation_type_subtype="SUBTYPE",
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=10,
                supervision_level=StateSupervisionLevel.MINIMUM,
                supervision_level_raw_text="MIN",
                projected_end_date=None,
            )
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-03",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))
        assert all(
            metric.supervision_level_raw_text is not None
            for metric in metrics
            if isinstance(metric, SupervisionPopulationMetric)
            and metric.person_id is not None
        )

    def test_produce_supervision_metrics_supervision_success(self):
        """Tests the produce_supervision_metrics function when there is a ProjectedSupervisionCompletionBucket."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=998,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))
        assert any(
            isinstance(metric, SuccessfulSupervisionSentenceDaysServedMetric)
            and metric.days_served == 998
            for metric in metrics
        )

    def test_produce_supervision_metrics_supervision_unsuccessful(self):
        """Tests the produce_supervision_metrics function when there is a ProjectedSupervisionCompletionBucket
        and the supervision is not successfully completed."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=False,
                incarcerated_during_sentence=True,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))
        assert all(
            not metric.successful_completion
            for metric in metrics
            if isinstance(metric, SupervisionSuccessMetric)
        )
        assert all(
            not isinstance(metric, SuccessfulSupervisionSentenceDaysServedMetric)
            for metric in metrics
        )

    def test_produce_supervision_metrics_supervision_mixed_success(self):
        """Tests the produce_supervision_metrics function when there is a ProjectedSupervisionCompletionBucket and the
        supervision is not successfully completed."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_ND", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code="US_ND",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=False,
                incarcerated_during_sentence=True,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            ProjectedSupervisionCompletionBucket(
                state_code="US_ND",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=199,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2018,
                month=4,
                event_date=date(2010, 4, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))
        self.assertTrue(
            all(
                not metric.successful_completion
                for metric in metrics
                if isinstance(metric, SupervisionSuccessMetric)
                and metric.supervision_type
                == StateSupervisionPeriodSupervisionType.PAROLE
            )
        )
        self.assertTrue(
            all(
                metric.successful_completion
                for metric in metrics
                if isinstance(metric, SupervisionSuccessMetric)
                and metric.supervision_type
                == StateSupervisionPeriodSupervisionType.PROBATION
            )
        )

    @freeze_time("2020-02-01")
    def test_produce_supervision_metrics_supervision_with_district_officer(self):
        """Tests the produce_supervision_metrics function when there is a mix of missing & non-null district/officer
        data for one person over many ProjectedSupervisionCompletionBuckets."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1993, 4, 2),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2017,
                month=6,
                event_date=date(2017, 6, 30),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False,
            ),
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2020,
                month=1,
                event_date=date(2020, 1, 30),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2018,
                month=12,
                event_date=date(2018, 12, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))
        assert any(metric.supervising_officer_external_id for metric in metrics)

    def test_produce_supervision_metrics_revocation_and_not(self):
        """Tests the produce_supervision_metrics function."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        person_external_id = StatePersonExternalId.new_with_defaults(
            external_id="SID1341", id_type="US_MO_DOC", state_code="US_XX"
        )

        person.external_ids = [person_external_id]

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=2,
                event_date=date(2018, 2, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=13,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                supervising_officer_external_id="OFFICER",
                supervising_district_external_id="DISTRICT",
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                response_count=19,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=13,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                supervising_officer_external_id="OFFICER",
                supervising_district_external_id="DISTRICT",
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                response_count=19,
                projected_end_date=None,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=13,
                assessment_level=StateAssessmentLevel.MEDIUM,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                supervising_officer_external_id="OFFICER",
                supervising_district_external_id="DISTRICT",
                revocation_type=RevocationType.SHOCK_INCARCERATION,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=19,
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_multiple_months(self):
        """Tests the produce_supervision_metrics function where the person was on supervision for multiple months."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                event_date=date(2018, 4, 1),
                year=2018,
                month=4,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                event_date=date(2018, 4, 2),
                year=2018,
                month=4,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                event_date=date(2018, 4, 3),
                year=2018,
                month=4,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                event_date=date(2018, 4, 4),
                year=2018,
                month=4,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-07",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_overlapping_days(self):
        """Tests the produce_supervision_metrics function where the person was serving multiple supervision sentences
        simultaneously in a given month."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=2018,
                month=3,
                event_date=date(2018, 3, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=2018,
                month=3,
                event_date=date(2018, 3, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_overlapping_months_types(self):
        """Tests the produce_supervision_metrics function where the person was serving multiple supervision sentences
        simultaneously in a given month, but the supervisions are of different types."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_ND", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2010,
                month=3,
                event_date=date(2010, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2018,
                month=4,
                event_date=date(2010, 4, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2018,
                month=5,
                event_date=date(2010, 5, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-05",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
        )

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_overlapping_months_types_dual(self):
        """Tests the produce_supervision_metrics function where the person was serving multiple supervision sentences
        simultaneously in a given month, but the supervisions are of different types."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_ND", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2018,
                month=4,
                event_date=date(2010, 4, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2018,
                month=4,
                event_date=date(2010, 4, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2018,
                month=4,
                event_date=date(2010, 4, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
        )

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_two_revocations_in_month_sort_date(self):
        """Tests the produce_supervision_metrics function where the person was revoked twice in a given month. Asserts
        that the revocation with the latest date is chosen."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_ND", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2018,
                month=3,
                event_date=date(2018, 3, 13),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervising_officer_external_id="FAKE_ID_1",
                projected_end_date=None,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code="US_ND",
                year=2018,
                month=3,
                event_date=date(2018, 3, 29),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervising_officer_external_id="FAKE_ID_2",
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-03",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
        )

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_start_bucket(self):
        """Tests the produce_supervision_metrics when there are SupervisionStartBuckets sent to the metric_producer."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )
        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)
        person.races = [race]
        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )
        person.ethnicities = [ethnicity]

        start_bucket = SupervisionStartBucket(
            state_code="US_XX",
            year=2000,
            month=1,
            event_date=date(2000, 1, 13),
            in_incarceration_population_on_date=False,
            in_supervision_population_on_date=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )

        supervision_time_buckets = [start_bucket]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2000-01",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_termination_bucket(self):
        """Tests the produce_supervision_metrics when there are SupervisionTerminationBuckets sent to the
        metric_producer."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_ND", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code="US_ND",
            year=2000,
            month=1,
            event_date=date(2000, 1, 13),
            in_incarceration_population_on_date=True,
            in_supervision_population_on_date=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9,
        )

        supervision_time_buckets = [termination_bucket]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2000-01",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))
        assert all(metric.assessment_score_change == -9 for metric in metrics)

    def test_produce_supervision_metrics_termination_buckets_no_score_change(self):
        """Tests the produce_supervision_metrics when there are SupervisionTerminationBuckets sent to the metric_producer,
        but the bucket doesn't have an assessment_score_change."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code="US_XX",
            year=2000,
            month=1,
            event_date=date(2000, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=None,
        )

        supervision_time_buckets = [termination_bucket]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2000-01",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count([termination_bucket])

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SupervisionTerminationMetric)
            and metric.assessment_score_change is None
            for metric in metrics
        )

    def test_produce_supervision_metrics_termination_buckets(self):
        """Tests the produce_supervision_metrics when there are SupervisionTerminationBuckets sent to the metric_producer
        that end in the same month."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_ND", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        first_termination_bucket = SupervisionTerminationBucket(
            state_code="US_ND",
            year=2000,
            month=1,
            event_date=date(2000, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9,
        )

        second_termination_bucket = SupervisionTerminationBucket(
            state_code="US_ND",
            year=2000,
            month=1,
            event_date=date(2000, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            assessment_score_change=-9,
        )

        supervision_time_buckets = [first_termination_bucket, second_termination_bucket]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2000-01",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))
        assert all(
            metric.assessment_score_change == -9
            for metric in metrics
            if isinstance(metric, SupervisionTerminationMetric)
        )

    def test_produce_supervision_metrics_only_terminations(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code="US_XX",
            year=2010,
            month=1,
            event_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_level=StateAssessmentLevel.LOW,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9,
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2010,
                month=3,
                event_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=398,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (metric_type == SupervisionMetricType.SUPERVISION_TERMINATION)
            for metric_type in SupervisionMetricType
        }

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_TERMINATION,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SupervisionTerminationMetric)
            and metric.assessment_score_change
            == termination_bucket.assessment_score_change
            for metric in metrics
        )

    def test_produce_supervision_metrics_only_success(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code="US_XX",
            year=2010,
            month=1,
            event_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9,
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2010,
                month=3,
                event_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=2010,
                month=1,
                event_date=date(2010, 1, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (metric_type == SupervisionMetricType.SUPERVISION_SUCCESS)
            for metric_type in SupervisionMetricType
        }

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_SUCCESS,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            metric.metric_type == SupervisionMetricType.SUPERVISION_SUCCESS
            for metric in metrics
        )

    def test_produce_supervision_metrics_only_successful_sentence_length(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code="US_XX",
            year=2010,
            month=1,
            event_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9,
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2010,
                month=3,
                event_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=500,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            termination_bucket,
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=2010,
                month=1,
                event_date=date(2010, 1, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (
                metric_type
                == SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
            )
            for metric_type in SupervisionMetricType
        }

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SuccessfulSupervisionSentenceDaysServedMetric)
            and metric.days_served == 500
            for metric in metrics
        )

    def test_produce_supervision_metrics_only_revocation(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code="US_XX",
            year=2010,
            month=1,
            event_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9,
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2010,
                month=3,
                event_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (metric_type == SupervisionMetricType.SUPERVISION_REVOCATION)
            for metric_type in SupervisionMetricType
        }

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_REVOCATION,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SupervisionRevocationMetric) for metric in metrics
        )

    def test_produce_supervision_metrics_only_population(self):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_bucket = SupervisionTerminationBucket(
            state_code="US_XX",
            year=2010,
            month=1,
            event_date=date(2010, 1, 13),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            assessment_score_change=-9,
        )

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2010,
                month=3,
                event_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            termination_bucket,
            RevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2010,
                month=2,
                event_date=date(2010, 2, 22),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (metric_type == SupervisionMetricType.SUPERVISION_POPULATION)
            for metric_type in SupervisionMetricType
        }

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_POPULATION,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SupervisionPopulationMetric) for metric in metrics
        )

    def test_produce_supervision_metrics_only_successful_sentence_length_duplicate_month_longer_sentence(
        self,
    ):
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=500,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            ProjectedSupervisionCompletionBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=501,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
        ]

        inclusions_dict = {
            metric_type: (
                metric_type
                == SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
            )
            for metric_type in SupervisionMetricType
        }

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            inclusions_dict,
            calculation_end_month="2018-03",
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SuccessfulSupervisionSentenceDaysServedMetric)
            and metric.days_served in (500, 501)
            for metric in metrics
        )

    def test_produce_supervision_metrics_compliance_metrics(self):
        """Tests the produce_supervision_metrics function when there are compliance metrics to be generated."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                case_compliance=SupervisionCaseCompliance(
                    date_of_evaluation=date(2018, 3, 31),
                    num_days_assessment_overdue=0,
                    face_to_face_frequency_sufficient=False,
                ),
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                case_compliance=SupervisionCaseCompliance(
                    date_of_evaluation=date(2018, 4, 30),
                    num_days_assessment_overdue=11,
                    face_to_face_frequency_sufficient=False,
                ),
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_US_ID_supervision_out_of_state_population_metrics_is_out_of_state(
        self,
    ):
        person = StatePerson.new_with_defaults(
            state_code="US_ID",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_ID", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ID", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code="US_ID",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervising_district_external_id="INTERSTATE PROBATION - 123",
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (
                metric_type == SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION
            )
            for metric_type in SupervisionMetricType
        }

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION,
            out_of_state_population=True,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SupervisionOutOfStatePopulationMetric)
            for metric in metrics
        )

    def test_produce_supervision_metrics_US_ID_supervision_out_of_state_population_metrics_is_out_of_state_by_authority(
        self,
    ):
        person = StatePerson.new_with_defaults(
            state_code="US_ID",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_ID", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ID", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code="US_ID",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                custodial_authority=StateCustodialAuthority.FEDERAL,
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (
                metric_type == SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION
            )
            for metric_type in SupervisionMetricType
        }

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_time_buckets,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION,
            out_of_state_population=True,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SupervisionOutOfStatePopulationMetric)
            for metric in metrics
        )

    def test_produce_supervision_metrics_US_ID_supervision_out_of_state_population_metrics_not_out_of_state(
        self,
    ):
        person = StatePerson.new_with_defaults(
            state_code="US_ID",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_ID", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ID", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code="US_ID",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervising_district_external_id="INVALID - 123",
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (
                metric_type == SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION
            )
            for metric_type in SupervisionMetricType
        }

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = 0

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_US_ID_supervision_out_of_state_population_metrics_not_out_of_state_by_authority(
        self,
    ):
        person = StatePerson.new_with_defaults(
            state_code="US_ID",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_ID", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ID", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code="US_ID",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (
                metric_type == SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION
            )
            for metric_type in SupervisionMetricType
        }

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = 0

        self.assertEqual(expected_count, len(metrics))

    def test_map_supervision_downgrade_metrics(self):
        """Tests the produce_supervision_metrics function when there are supervision downgrade metrics to be
        generated."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                is_on_supervision_last_day_of_month=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                is_on_supervision_last_day_of_month=False,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.MINIMUM,
                supervision_level_raw_text="MINIMUM",
                supervision_level_downgrade_occurred=True,
                previous_supervision_level=StateSupervisionLevel.HIGH,
                projected_end_date=None,
            ),
        ]

        metrics = metric_producer.produce_supervision_metrics(
            person,
            supervision_time_buckets,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_time_buckets)

        self.assertEqual(expected_count, len(metrics))


class TestIncludeEventInMetric(unittest.TestCase):
    """Tests the include_event_in_metric function."""

    def test_include_event_in_metric_compliance_no_compliance(self):
        event = NonRevocationReturnSupervisionTimeBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            projected_end_date=None,
        )

        self.assertFalse(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_COMPLIANCE
            )
        )

    def test_include_event_in_metric_compliance_with_compliance(self):
        event = NonRevocationReturnSupervisionTimeBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            case_compliance=SupervisionCaseCompliance(
                date_of_evaluation=date(2018, 3, 31),
                num_days_assessment_overdue=0,
                face_to_face_frequency_sufficient=False,
            ),
            projected_end_date=None,
        )

        self.assertTrue(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_COMPLIANCE
            )
        )

    def test_include_event_in_metric_downgrade_no_downgrade(self):
        event = NonRevocationReturnSupervisionTimeBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            supervision_level_downgrade_occurred=False,
            projected_end_date=None,
        )

        self.assertFalse(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_DOWNGRADE
            )
        )

    def test_include_event_in_metric_downgrade_with_downgrade(self):
        event = NonRevocationReturnSupervisionTimeBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            supervision_level_downgrade_occurred=True,
            projected_end_date=None,
        )

        self.assertTrue(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_DOWNGRADE
            )
        )

    @mock.patch(
        "recidiviz.calculator.pipeline.supervision.metric_producer.supervision_period_is_out_of_state"
    )
    def test_include_event_in_metric_not_out_of_state(self, mock_is_out_of_state):
        mock_is_out_of_state.return_value = False

        event = NonRevocationReturnSupervisionTimeBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            projected_end_date=None,
        )

        self.assertFalse(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION
            )
        )

    @mock.patch(
        "recidiviz.calculator.pipeline.supervision.metric_producer.supervision_period_is_out_of_state"
    )
    def test_include_event_in_metric_out_of_state(self, mock_is_out_of_state):
        mock_is_out_of_state.return_value = True

        event = NonRevocationReturnSupervisionTimeBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            supervision_level_downgrade_occurred=True,
            projected_end_date=None,
        )

        self.assertTrue(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION
            )
        )

    @mock.patch(
        "recidiviz.calculator.pipeline.supervision.metric_producer.supervision_period_is_out_of_state"
    )
    def test_include_event_in_metric_not_in_state(self, mock_is_out_of_state):
        mock_is_out_of_state.return_value = True

        event = NonRevocationReturnSupervisionTimeBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            projected_end_date=None,
        )

        self.assertFalse(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_POPULATION
            )
        )

    @mock.patch(
        "recidiviz.calculator.pipeline.supervision.metric_producer.supervision_period_is_out_of_state"
    )
    def test_include_event_in_metric_in_state(self, mock_is_out_of_state):
        mock_is_out_of_state.return_value = False

        event = NonRevocationReturnSupervisionTimeBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            is_on_supervision_last_day_of_month=True,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            supervision_level_downgrade_occurred=True,
            projected_end_date=None,
        )

        self.assertTrue(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_POPULATION
            )
        )

    def test_include_in_metric_supervision_successful_days_served(self):
        event = ProjectedSupervisionCompletionBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            successful_completion=True,
            incarcerated_during_sentence=False,
            sentence_days_served=998,
            supervising_officer_external_id="officer45",
            supervising_district_external_id="district5",
        )

        self.assertTrue(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
            )
        )

    def test_include_in_metric_supervision_successful_days_served_unsuccessful(self):
        event = ProjectedSupervisionCompletionBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            successful_completion=False,
            incarcerated_during_sentence=False,
            sentence_days_served=998,
            supervising_officer_external_id="officer45",
            supervising_district_external_id="district5",
        )

        self.assertFalse(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
            )
        )

    def test_include_in_metric_supervision_successful_days_served_incarcerated_in_sentence(
        self,
    ):
        event = ProjectedSupervisionCompletionBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            successful_completion=True,
            incarcerated_during_sentence=True,
            sentence_days_served=998,
            supervising_officer_external_id="officer45",
            supervising_district_external_id="district5",
        )

        self.assertFalse(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
            )
        )

    def test_include_in_metric_supervision_successful_days_served_no_days_served(self):
        event = ProjectedSupervisionCompletionBucket(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            successful_completion=True,
            incarcerated_during_sentence=False,
            supervising_officer_external_id="officer45",
            supervising_district_external_id="district5",
        )

        self.assertFalse(
            metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
            )
        )


def expected_metrics_count(
    supervision_time_buckets: List[SupervisionTimeBucket],
    include_all_metrics: bool = True,
    metric_to_include: SupervisionMetricType = None,
    out_of_state_population: bool = False,
) -> int:
    """Calculates the expected number of characteristic combinations given the supervision time buckets
    and the metrics that should be included in the counts."""
    output_count_by_metric_type: Dict[SupervisionMetricType, int] = defaultdict(int)

    for metric_type in SupervisionMetricType:
        if not include_all_metrics and metric_type != metric_to_include:
            continue

        if metric_type == SupervisionMetricType.SUPERVISION_COMPLIANCE:
            output_count_by_metric_type[metric_type] = len(
                [
                    bucket
                    for bucket in supervision_time_buckets
                    if isinstance(bucket, NonRevocationReturnSupervisionTimeBucket)
                    and bucket.case_compliance is not None
                ]
            )
        elif metric_type == SupervisionMetricType.SUPERVISION_DOWNGRADE:
            output_count_by_metric_type[metric_type] = len(
                [
                    bucket
                    for bucket in supervision_time_buckets
                    if isinstance(bucket, NonRevocationReturnSupervisionTimeBucket)
                    and bucket.supervision_level_downgrade_occurred
                ]
            )
        elif (
            metric_type
            == SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
        ):
            output_count_by_metric_type[metric_type] = len(
                [
                    bucket
                    for bucket in supervision_time_buckets
                    if isinstance(bucket, ProjectedSupervisionCompletionBucket)
                    and bucket.sentence_days_served is not None
                    and bucket.successful_completion
                    and not bucket.incarcerated_during_sentence
                ]
            )
        elif metric_type == SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION:
            output_count_by_metric_type[metric_type] = len(
                [
                    bucket
                    for bucket in supervision_time_buckets
                    if isinstance(
                        bucket,
                        (
                            RevocationReturnSupervisionTimeBucket,
                            NonRevocationReturnSupervisionTimeBucket,
                        ),
                    )
                    and out_of_state_population
                ]
            )
        elif metric_type == SupervisionMetricType.SUPERVISION_POPULATION:
            output_count_by_metric_type[metric_type] = len(
                [
                    bucket
                    for bucket in supervision_time_buckets
                    if isinstance(
                        bucket,
                        (
                            RevocationReturnSupervisionTimeBucket,
                            NonRevocationReturnSupervisionTimeBucket,
                        ),
                    )
                    and not out_of_state_population
                ]
            )
        else:
            for bucket_type in BUCKET_TYPES_FOR_METRIC[metric_type]:
                output_count_by_metric_type[metric_type] += len(
                    [
                        bucket
                        for bucket in supervision_time_buckets
                        if isinstance(bucket, bucket_type)
                    ]
                )

    if include_all_metrics:
        return sum(value for value in output_count_by_metric_type.values())

    if metric_to_include not in output_count_by_metric_type:
        raise ValueError(
            f"Metric {metric_to_include} not a valid metric type for pipeline."
        )

    return output_count_by_metric_type[metric_to_include]
