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

from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStatePersonRace,
)
from recidiviz.pipelines.metrics.program import metric_producer, pipeline
from recidiviz.pipelines.metrics.program.events import (
    ProgramEvent,
    ProgramParticipationEvent,
)
from recidiviz.pipelines.metrics.program.metrics import ProgramMetricType
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetric

ALL_METRICS_INCLUSIONS = set(ProgramMetricType)


PIPELINE_JOB_ID = "TEST_JOB_ID"


class TestProduceProgramMetrics(unittest.TestCase):
    """Tests the produce_program_metrics function."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.ProgramMetricProducer()
        self.pipeline_class = pipeline.ProgramMetricsPipeline

    @freeze_time("2030-11-02 00:00:00-05:00")
    def test_produce_program_metrics(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_ND",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        race = NormalizedStatePersonRace(
            state_code="US_ND", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        program_events: List[ProgramEvent] = [
            ProgramParticipationEvent(
                state_code="US_ND", event_date=date(2019, 2, 2), program_id="ZZZ"
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS,
            calculation_month_count=-1,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    @freeze_time("2009-10-01 00:00:00-05:00")
    def test_produce_program_metrics_full_info(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_ND",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        race = NormalizedStatePersonRace(
            state_code="US_ND", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        event_date = date(2009, 10, 31)

        program_events: List[ProgramEvent] = [
            ProgramParticipationEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                program_location_id="YYY",
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS,
            calculation_month_count=1,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_program_metrics_full_info_probation(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_ND",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        race = NormalizedStatePersonRace(
            state_code="US_ND", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        event_date = date(2009, 10, 31)

        program_events: List[ProgramEvent] = [
            ProgramParticipationEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                program_location_id="YYY",
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS,
            calculation_month_count=-1,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_program_metrics_multiple_supervision_types(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_ND",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        race = NormalizedStatePersonRace(
            state_code="US_ND", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        event_date = date(2009, 10, 7)

        program_events: List[ProgramEvent] = [
            ProgramParticipationEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                program_location_id="YYY",
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            ProgramParticipationEvent(
                state_code="US_ND",
                event_date=event_date,
                program_id="XXX",
                program_location_id="YYY",
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS,
            calculation_month_count=-1,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))


def expected_metrics_count(program_events: List[ProgramEvent]) -> int:
    """Calculates the expected number of metrics given the incarceration events."""
    output_count_by_metric_class: Dict[
        Type[RecidivizMetric[ProgramMetricType]], int
    ] = defaultdict(int)

    for event in program_events:
        metric_classes = metric_producer.ProgramMetricProducer().result_class_to_metric_classes_mapping[
            type(event)
        ]

        for metric_class in metric_classes:
            output_count_by_metric_class[metric_class] += 1

    return sum(value for value in output_count_by_metric_class.values())
