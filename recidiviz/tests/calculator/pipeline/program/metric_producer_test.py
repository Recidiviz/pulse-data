# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for program/metric_producer.py."""
import unittest
from collections import defaultdict
from datetime import date
from typing import Dict, List, Type

from freezegun import freeze_time

from recidiviz.calculator.pipeline.program import metric_producer, pipeline
from recidiviz.calculator.pipeline.program.events import (
    ProgramEvent,
    ProgramParticipationEvent,
    ProgramReferralEvent,
)
from recidiviz.calculator.pipeline.program.metrics import ProgramMetricType
from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonEthnicity,
    StatePersonRace,
)

ALL_METRICS_INCLUSIONS_DICT = {metric_type: True for metric_type in ProgramMetricType}

_DEFAULT_PERSON_METADATA = PersonMetadata(prioritized_race_or_ethnicity="BLACK")

PIPELINE_JOB_ID = "TEST_JOB_ID"


class TestProduceProgramMetrics(unittest.TestCase):
    """Tests the produce_program_metrics function."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.ProgramMetricProducer()
        self.pipeline_config = pipeline.ProgramPipeline().pipeline_config

    @freeze_time("2030-11-02")
    def test_produce_program_metrics(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_ND",
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

        program_events = [
            ProgramReferralEvent(
                state_code="US_ND", event_date=date(2019, 10, 10), program_id="XXX"
            ),
            ProgramParticipationEvent(
                state_code="US_ND", event_date=date(2019, 2, 2), program_id="ZZZ"
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_program_metrics_full_info(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_ND",
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

        event_date = date(2009, 10, 31)

        program_events = [
            ProgramReferralEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id="OFFICERZ",
                supervising_district_external_id="135",
            ),
            ProgramParticipationEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                program_location_id="YYY",
                supervision_type=StateSupervisionType.PAROLE,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2009-10",
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_program_metrics_full_info_probation(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_ND",
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

        event_date = date(2009, 10, 31)

        program_events = [
            ProgramReferralEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                supervision_type=StateSupervisionType.PROBATION,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id="OFFICERZ",
                supervising_district_external_id="135",
            ),
            ProgramParticipationEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                program_location_id="YYY",
                supervision_type=StateSupervisionType.PROBATION,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2009-10",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_program_metrics_multiple_supervision_types(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_ND",
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

        event_date = date(2009, 10, 7)

        program_events = [
            ProgramReferralEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                supervision_type=StateSupervisionType.PAROLE,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id="OFFICERZ",
                supervising_district_external_id="135",
            ),
            ProgramReferralEvent(
                state_code="US_ND",
                event_date=date(2009, 10, 7),
                program_id="XXX",
                supervision_type=StateSupervisionType.PROBATION,
                assessment_score=22,
                assessment_type=StateAssessmentType.LSIR,
                supervising_officer_external_id="OFFICERZ",
                supervising_district_external_id="135",
            ),
            ProgramParticipationEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                program_location_id="YYY",
                supervision_type=StateSupervisionType.PAROLE,
            ),
            ProgramParticipationEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                program_location_id="YYY",
                supervision_type=StateSupervisionType.PROBATION,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2009-10",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    @freeze_time("2012-11-30")
    def test_produce_program_metrics_calculation_month_count_1(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_ND",
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

        included_event = ProgramReferralEvent(
            state_code="US_ND", event_date=date(2012, 11, 10), program_id="XXX"
        )

        not_included_event = ProgramReferralEvent(
            state_code="US_ND", event_date=date(2000, 2, 2), program_id="ZZZ"
        )

        program_events: List[ProgramEvent] = [
            included_event,
            not_included_event,
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = expected_metrics_count([included_event])

        self.assertEqual(expected_count, len(metrics))

    @freeze_time("2012-12-31")
    def test_produce_program_metrics_calculation_month_count_36(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_ND",
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

        included_event = ProgramReferralEvent(
            state_code="US_ND", event_date=date(2012, 12, 10), program_id="XXX"
        )

        not_included_event = ProgramReferralEvent(
            state_code="US_ND", event_date=date(2009, 12, 10), program_id="ZZZ"
        )

        program_events: List[ProgramEvent] = [included_event, not_included_event]

        metrics = self.metric_producer.produce_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=36,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = expected_metrics_count([included_event])

        self.assertEqual(expected_count, len(metrics))


def expected_metrics_count(program_events: List[ProgramEvent]) -> int:
    """Calculates the expected number of metrics given the incarceration events."""
    output_count_by_metric_class: Dict[
        Type[RecidivizMetric[ProgramMetricType]], int
    ] = defaultdict(int)

    for event in program_events:
        metric_classes = (
            metric_producer.ProgramMetricProducer().event_to_metric_classes[type(event)]
        )

        for metric_class in metric_classes:
            output_count_by_metric_class[metric_class] += 1

    return sum(value for value in output_count_by_metric_class.values())
