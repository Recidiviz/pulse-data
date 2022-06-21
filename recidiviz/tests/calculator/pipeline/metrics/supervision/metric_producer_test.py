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
import unittest
from collections import defaultdict
from datetime import date
from typing import Dict, List
from unittest import mock

from freezegun import freeze_time

from recidiviz.calculator.pipeline.metrics.supervision import (
    identifier,
    metric_producer,
    pipeline,
)
from recidiviz.calculator.pipeline.metrics.supervision.events import (
    ProjectedSupervisionCompletionEvent,
    SupervisionEvent,
    SupervisionPopulationEvent,
    SupervisionStartEvent,
    SupervisionTerminationEvent,
)
from recidiviz.calculator.pipeline.metrics.supervision.metrics import (
    SupervisionMetricType,
    SupervisionOutOfStatePopulationMetric,
    SupervisionPopulationMetric,
    SupervisionSuccessMetric,
    SupervisionTerminationMetric,
)
from recidiviz.calculator.pipeline.metrics.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import PersonMetadata
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonEthnicity,
    StatePersonRace,
)

ALL_METRICS_INCLUSIONS_DICT = {
    metric_type: True for metric_type in SupervisionMetricType
}

_DEFAULT_PERSON_METADATA = PersonMetadata(prioritized_race_or_ethnicity="BLACK")
_PIPELINE_JOB_ID = "TEST_JOB_ID"


class TestProduceSupervisionMetrics(unittest.TestCase):
    """Tests the produce_supervision_metrics function."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.SupervisionMetricProducer()
        self.identifier = identifier.SupervisionIdentifier()
        self.pipeline_config = (
            pipeline.SupervisionMetricsPipelineRunDelegate.pipeline_config()
        )
        self.supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.metrics.supervision"
            ".metric_producer.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.supervision_delegate_patcher.stop()

    def test_produce_supervision_metrics(self) -> None:
        """Tests the produce_supervision_metrics function."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_assessment(self) -> None:
        """Tests the produce_supervision_metrics function when there is assessment data present."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_exclude_assessment(self) -> None:
        """Tests the produce_supervision_metrics function when there is assessment data present, but it should not
        be included for this state and pipeline type."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE,
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_supervising_officer_district(self) -> None:
        """Tests the produce_supervision_metrics function when there is supervising officer and district data
        present."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.VERY_HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                supervising_officer_external_id="143",
                supervising_district_external_id="DISTRICT X",
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=31,
                assessment_level=StateAssessmentLevel.VERY_HIGH,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                supervising_officer_external_id="143",
                supervising_district_external_id="DISTRICT X",
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_supervision_success(self) -> None:
        """Tests the produce_supervision_metrics function when there is a ProjectedSupervisionCompletionEvent."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            ProjectedSupervisionCompletionEvent(
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
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_supervision_unsuccessful(self) -> None:
        """Tests the produce_supervision_metrics function when there is a ProjectedSupervisionCompletionEvent
        and the supervision is not successfully completed."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            ProjectedSupervisionCompletionEvent(
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
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))
        assert all(
            not metric.successful_completion
            for metric in metrics
            if isinstance(metric, SupervisionSuccessMetric)
        )

    def test_produce_supervision_metrics_supervision_mixed_success(self) -> None:
        """Tests the produce_supervision_metrics function when there is a ProjectedSupervisionCompletionEvent and the
        supervision is not successfully completed."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_ND", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            ProjectedSupervisionCompletionEvent(
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
            ProjectedSupervisionCompletionEvent(
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
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2018,
                month=4,
                event_date=date(2010, 4, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

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
    def test_produce_supervision_metrics_supervision_with_district_officer(
        self,
    ) -> None:
        """Tests the produce_supervision_metrics function when there is a mix of missing & non-null district/officer
        data for one person over many ProjectedSupervisionCompletionEvents."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1993, 4, 2),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            ProjectedSupervisionCompletionEvent(
                state_code="US_XX",
                year=2017,
                month=6,
                event_date=date(2017, 6, 30),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False,
            ),
            ProjectedSupervisionCompletionEvent(
                state_code="US_XX",
                year=2020,
                month=1,
                event_date=date(2020, 1, 30),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            ProjectedSupervisionCompletionEvent(
                state_code="US_XX",
                year=2018,
                month=12,
                event_date=date(2018, 12, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=False,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))
        assert any(metric.supervising_officer_external_id for metric in metrics)

    def test_produce_supervision_metrics_multiple_months(self) -> None:
        """Tests the produce_supervision_metrics function where the person was on supervision for multiple months."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                event_date=date(2018, 4, 1),
                year=2018,
                month=4,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                event_date=date(2018, 4, 2),
                year=2018,
                month=4,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                event_date=date(2018, 4, 3),
                year=2018,
                month=4,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                event_date=date(2018, 4, 4),
                year=2018,
                month=4,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-07",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_overlapping_days(self) -> None:
        """Tests the produce_supervision_metrics function where the person was serving multiple supervision sentences
        simultaneously in a given month."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=2018,
                month=3,
                event_date=date(2018, 3, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=2018,
                month=3,
                event_date=date(2018, 3, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_overlapping_months_types(self) -> None:
        """Tests the produce_supervision_metrics function where the person was serving multiple supervision sentences
        simultaneously in a given month, but the supervisions are of different types."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_ND", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2010,
                month=3,
                event_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2018,
                month=4,
                event_date=date(2010, 4, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2018,
                month=5,
                event_date=date(2010, 5, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-05",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_events,
        )

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_overlapping_months_types_dual(self) -> None:
        """Tests the produce_supervision_metrics function where the person was serving multiple supervision sentences
        simultaneously in a given month, but the supervisions are of different types."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_ND", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2010,
                month=2,
                event_date=date(2010, 2, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2018,
                month=4,
                event_date=date(2010, 4, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2018,
                month=4,
                event_date=date(2010, 4, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_ND",
                year=2018,
                month=4,
                event_date=date(2010, 4, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_events,
        )

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_start_event(self) -> None:
        """Tests the produce_supervision_metrics when there are SupervisionStartEvents sent to the metric_producer."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )
        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )
        person.races = [race]
        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )
        person.ethnicities = [ethnicity]

        start_event = SupervisionStartEvent(
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

        supervision_events: List[SupervisionEvent] = [start_event]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2000-01",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_termination_event(self) -> None:
        """Tests the produce_supervision_metrics when there are SupervisionTerminationEvents sent to the
        metric_producer."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_ND", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_event = SupervisionTerminationEvent(
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

        supervision_events: List[SupervisionEvent] = [termination_event]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2000-01",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))
        assert all(
            getattr(metric, "assessment_score_change") == -9 for metric in metrics
        )

    def test_produce_supervision_metrics_termination_events_no_score_change(
        self,
    ) -> None:
        """Tests the produce_supervision_metrics when there are SupervisionTerminationEvents sent to the metric_producer,
        but the event doesn't have an assessment_score_change."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_event = SupervisionTerminationEvent(
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

        supervision_events: List[SupervisionEvent] = [termination_event]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2000-01",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count([termination_event])

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SupervisionTerminationMetric)
            and metric.assessment_score_change is None
            for metric in metrics
        )

    def test_produce_supervision_metrics_termination_events(self) -> None:
        """Tests the produce_supervision_metrics when there are SupervisionTerminationEvents sent to the metric_producer
        that end in the same month."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_ND", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ND", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        first_termination_event = SupervisionTerminationEvent(
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

        second_termination_event = SupervisionTerminationEvent(
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

        supervision_events: List[SupervisionEvent] = [
            first_termination_event,
            second_termination_event,
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2000-01",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))
        assert all(
            metric.assessment_score_change == -9
            for metric in metrics
            if isinstance(metric, SupervisionTerminationMetric)
        )

    def test_produce_supervision_metrics_only_terminations(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_event = SupervisionTerminationEvent(
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

        supervision_events: List[SupervisionEvent] = [
            ProjectedSupervisionCompletionEvent(
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
            termination_event,
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (metric_type == SupervisionMetricType.SUPERVISION_TERMINATION)
            for metric_type in SupervisionMetricType
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_events,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_TERMINATION,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SupervisionTerminationMetric)
            and metric.assessment_score_change
            == termination_event.assessment_score_change
            for metric in metrics
        )

    def test_produce_supervision_metrics_only_success(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_event = SupervisionTerminationEvent(
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

        supervision_events: List[SupervisionEvent] = [
            ProjectedSupervisionCompletionEvent(
                state_code="US_XX",
                year=2010,
                month=3,
                event_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            termination_event,
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=2010,
                month=1,
                event_date=date(2010, 1, 2),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (metric_type == SupervisionMetricType.SUPERVISION_SUCCESS)
            for metric_type in SupervisionMetricType
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_events,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_SUCCESS,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            metric.metric_type == SupervisionMetricType.SUPERVISION_SUCCESS
            for metric in metrics
        )

    def test_produce_supervision_metrics_only_population(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        termination_event = SupervisionTerminationEvent(
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

        supervision_events: List[SupervisionEvent] = [
            ProjectedSupervisionCompletionEvent(
                state_code="US_XX",
                year=2010,
                month=3,
                event_date=date(2010, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                successful_completion=True,
                supervising_officer_external_id="officer45",
                supervising_district_external_id="district5",
            ),
            termination_event,
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2010,
                month=2,
                event_date=date(2010, 2, 22),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
            ),
        ]

        inclusions_dict = {
            metric_type: (metric_type == SupervisionMetricType.SUPERVISION_POPULATION)
            for metric_type in SupervisionMetricType
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_events,
            include_all_metrics=False,
            metric_to_include=SupervisionMetricType.SUPERVISION_POPULATION,
        )

        self.assertEqual(expected_count, len(metrics))
        assert all(
            isinstance(metric, SupervisionPopulationMetric) for metric in metrics
        )

    def test_produce_supervision_metrics_compliance_metrics(self) -> None:
        """Tests the produce_supervision_metrics function when there are compliance metrics to be generated."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                case_compliance=SupervisionCaseCompliance(
                    date_of_evaluation=date(2018, 3, 31),
                    next_recommended_assessment_date=None,
                    next_recommended_face_to_face_date=None,
                ),
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                case_compliance=SupervisionCaseCompliance(
                    date_of_evaluation=date(2018, 4, 30),
                    next_recommended_assessment_date=date(2018, 4, 19),
                    next_recommended_face_to_face_date=None,
                ),
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_supervision_metrics_US_ID_supervision_out_of_state_population_metrics_is_out_of_state(
        self,
    ) -> None:
        self._stop_state_specific_delegate_patchers()
        person = StatePerson.new_with_defaults(
            state_code="US_ID",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_ID", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ID", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_ID",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
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

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_events,
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
    ) -> None:
        self._stop_state_specific_delegate_patchers()
        person = StatePerson.new_with_defaults(
            state_code="US_ID",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_ID", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ID", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_ID",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
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

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(
            supervision_events,
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
    ) -> None:
        self._stop_state_specific_delegate_patchers()
        person = StatePerson.new_with_defaults(
            state_code="US_ID",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_ID", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ID", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_ID",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
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

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
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
    ) -> None:
        self._stop_state_specific_delegate_patchers()
        person = StatePerson.new_with_defaults(
            state_code="US_ID",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_ID", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_ID", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_ID",
                year=2010,
                month=1,
                event_date=date(2010, 1, 1),
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

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            inclusions_dict,
            calculation_end_month="2010-12",
            calculation_month_count=12,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = 0

        self.assertEqual(expected_count, len(metrics))

    def test_map_supervision_downgrade_metrics(self) -> None:
        """Tests the produce_supervision_metrics function when there are supervision downgrade metrics to be
        generated."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(
            state_code="US_XX", race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=3,
                event_date=date(2018, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text="HIGH",
                projected_end_date=None,
            ),
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2018,
                month=4,
                event_date=date(2018, 4, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=StateSupervisionLevel.MINIMUM,
                supervision_level_raw_text="MINIMUM",
                supervision_level_downgrade_occurred=True,
                previous_supervision_level=StateSupervisionLevel.HIGH,
                projected_end_date=None,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            supervision_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2018-04",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=_PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(supervision_events)

        self.assertEqual(expected_count, len(metrics))


class TestIncludeEventInMetric(unittest.TestCase):
    """Tests the include_event_in_metric function."""

    def setUp(self) -> None:
        self.supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.metrics.supervision"
            ".metric_producer.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()
        self.metric_producer = metric_producer.SupervisionMetricProducer()

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.supervision_delegate_patcher.stop()

    def test_include_event_in_metric_compliance_no_compliance(self) -> None:
        event = SupervisionPopulationEvent(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            projected_end_date=None,
        )

        self.assertFalse(
            self.metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_COMPLIANCE
            )
        )

    def test_include_event_in_metric_compliance_with_compliance(self) -> None:
        event = SupervisionPopulationEvent(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            case_compliance=SupervisionCaseCompliance(
                date_of_evaluation=date(2018, 3, 31),
                next_recommended_assessment_date=None,
                next_recommended_face_to_face_date=None,
            ),
            projected_end_date=None,
        )

        self.assertTrue(
            self.metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_COMPLIANCE
            )
        )

    def test_include_event_in_metric_downgrade_no_downgrade(self) -> None:
        event = SupervisionPopulationEvent(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            supervision_level_downgrade_occurred=False,
            projected_end_date=None,
        )

        self.assertFalse(
            self.metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_DOWNGRADE
            )
        )

    def test_include_event_in_metric_downgrade_with_downgrade(self) -> None:
        event = SupervisionPopulationEvent(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            supervision_level_downgrade_occurred=True,
            projected_end_date=None,
        )

        self.assertTrue(
            self.metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_DOWNGRADE
            )
        )

    def test_include_event_in_metric_not_out_of_state(self) -> None:

        event = SupervisionPopulationEvent(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            projected_end_date=None,
        )

        self.assertFalse(
            self.metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION
            )
        )

    class OutOfStateDelegate(UsXxSupervisionDelegate):
        def is_supervision_location_out_of_state(
            self, _: SupervisionPopulationEvent
        ) -> bool:
            return True

    def test_include_event_in_metric_out_of_state(self) -> None:
        self._stop_state_specific_delegate_patchers()
        mock_supervision_delegate = self.supervision_delegate_patcher.start()
        mock_supervision_delegate.return_value = self.OutOfStateDelegate()

        event = SupervisionPopulationEvent(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            supervision_level_downgrade_occurred=True,
            projected_end_date=None,
        )

        self.assertTrue(
            self.metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION
            )
        )

    def test_include_event_in_metric_not_in_state(self) -> None:
        self._stop_state_specific_delegate_patchers()
        mock_supervision_delegate = self.supervision_delegate_patcher.start()
        mock_supervision_delegate.return_value = self.OutOfStateDelegate()

        event = SupervisionPopulationEvent(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            projected_end_date=None,
        )

        self.assertFalse(
            self.metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_POPULATION
            )
        )

    def test_include_event_in_metric_in_state(self) -> None:
        event = SupervisionPopulationEvent(
            state_code="US_XX",
            year=2018,
            month=3,
            event_date=date(2018, 3, 31),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            supervision_level_downgrade_occurred=True,
            projected_end_date=None,
        )

        self.assertTrue(
            self.metric_producer.include_event_in_metric(
                event, SupervisionMetricType.SUPERVISION_POPULATION
            )
        )


def expected_metrics_count(
    supervision_events: List[SupervisionEvent],
    include_all_metrics: bool = True,
    metric_to_include: SupervisionMetricType = None,
    out_of_state_population: bool = False,
) -> int:
    """Calculates the expected number of characteristic combinations given the supervision time events
    and the metrics that should be included in the counts."""
    output_count_by_metric_type: Dict[SupervisionMetricType, int] = defaultdict(int)

    for metric_type in SupervisionMetricType:
        if not include_all_metrics and metric_type != metric_to_include:
            continue

        if metric_type == SupervisionMetricType.SUPERVISION_COMPLIANCE:
            output_count_by_metric_type[metric_type] = len(
                [
                    event
                    for event in supervision_events
                    if isinstance(event, SupervisionPopulationEvent)
                    and event.case_compliance is not None
                ]
            )
        elif metric_type == SupervisionMetricType.SUPERVISION_DOWNGRADE:
            output_count_by_metric_type[metric_type] = len(
                [
                    event
                    for event in supervision_events
                    if isinstance(event, SupervisionPopulationEvent)
                    and event.supervision_level_downgrade_occurred
                ]
            )
        elif metric_type == SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION:
            output_count_by_metric_type[metric_type] = len(
                [
                    event
                    for event in supervision_events
                    if isinstance(
                        event,
                        (SupervisionPopulationEvent,),
                    )
                    and out_of_state_population
                ]
            )
        elif metric_type == SupervisionMetricType.SUPERVISION_POPULATION:
            output_count_by_metric_type[metric_type] = len(
                [
                    event
                    for event in supervision_events
                    if isinstance(
                        event,
                        (SupervisionPopulationEvent,),
                    )
                    and not out_of_state_population
                ]
            )
        else:
            for event_type in identifier.SupervisionIdentifier().EVENT_TYPES_FOR_METRIC[
                metric_type
            ]:
                output_count_by_metric_type[metric_type] += len(
                    [
                        event
                        for event in supervision_events
                        if isinstance(event, event_type)
                    ]
                )

    if include_all_metrics:
        return sum(value for value in output_count_by_metric_type.values())

    if metric_to_include not in output_count_by_metric_type:
        raise ValueError(
            f"Metric {metric_to_include} not a valid metric type for pipeline."
        )

    return output_count_by_metric_type[metric_to_include]
