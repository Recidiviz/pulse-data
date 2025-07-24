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
"""Tests for violation/metric_producer.py."""


import unittest
from datetime import date
from typing import List

from freezegun.api import freeze_time

from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStatePersonEthnicity,
    NormalizedStatePersonRace,
)
from recidiviz.pipelines.metrics.violation import metric_producer, pipeline
from recidiviz.pipelines.metrics.violation.events import (
    ViolationEvent,
    ViolationWithResponseEvent,
)
from recidiviz.pipelines.metrics.violation.metrics import (
    ViolationMetric,
    ViolationMetricType,
)

ALL_METRICS_INCLUSIONS = set(ViolationMetricType)


PIPELINE_JOB_ID = "TEST_JOB_ID"


class TestProduceViolationMetrics(unittest.TestCase):
    """Tests the produce_violation_metrics function."""

    def setUp(self) -> None:
        self.state_code = "US_ND"
        self.person = NormalizedStatePerson(
            state_code=self.state_code,
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        self.race = NormalizedStatePersonRace(
            state_code=self.state_code, person_race_id=12345, race=StateRace.WHITE
        )

        self.person.races = [self.race]

        self.ethnicity = NormalizedStatePersonEthnicity(
            state_code=self.state_code,
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        self.person.ethnicities = [self.ethnicity]
        self.metric_producer = metric_producer.ViolationMetricProducer()
        self.pipeline_class = pipeline.ViolationMetricsPipeline

    @freeze_time("2030-11-02 00:00:00-05:00")
    def test_produce_violation_metrics(self) -> None:
        violation_events: List[ViolationEvent] = [
            ViolationWithResponseEvent(
                state_code=self.state_code,
                supervision_violation_id=23456,
                event_date=date(2019, 10, 10),
                violation_date=date(2019, 10, 9),
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_subtype=None,
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            self.person,
            violation_events,
            ALL_METRICS_INCLUSIONS,
            calculation_month_count=-1,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(1, len(metrics))

    @freeze_time("2020-05-30 00:00:00-05:00")
    def test_produce_violation_metrics_calculation_month_count_1(self) -> None:
        included_event = ViolationWithResponseEvent(
            state_code=self.state_code,
            supervision_violation_id=23456,
            event_date=date(2020, 5, 20),
            violation_date=None,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_subtype=None,
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
        not_included_event = ViolationWithResponseEvent(
            state_code=self.state_code,
            supervision_violation_id=35467,
            event_date=date(2020, 4, 20),
            violation_date=None,
            violation_type=StateSupervisionViolationType.ABSCONDED,
            violation_type_subtype=None,
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.DELAYED_ACTION,
        )
        violation_events: List[ViolationEvent] = [included_event, not_included_event]

        metrics: List[ViolationMetric] = self.metric_producer.produce_metrics(
            self.person,
            violation_events,
            ALL_METRICS_INCLUSIONS,
            calculation_month_count=1,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(1, len(metrics))
        metric = metrics[0]
        self.assertEqual(
            metric.supervision_violation_id, included_event.supervision_violation_id
        )

    @freeze_time("2020-05-30 00:00:00-05:00")
    def test_produce_violation_metrics_calculation_month_count_36(self) -> None:
        included_event = ViolationWithResponseEvent(
            state_code=self.state_code,
            supervision_violation_id=23456,
            event_date=date(2019, 5, 20),
            violation_date=None,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_subtype=None,
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
        not_included_event = ViolationWithResponseEvent(
            state_code=self.state_code,
            supervision_violation_id=35467,
            event_date=date(2017, 4, 20),
            violation_date=None,
            violation_type=StateSupervisionViolationType.ABSCONDED,
            violation_type_subtype=None,
            is_most_severe_violation_type=True,
            is_violent=False,
            is_sex_offense=False,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.DELAYED_ACTION,
        )
        violation_events: List[ViolationEvent] = [included_event, not_included_event]

        metrics = self.metric_producer.produce_metrics(
            self.person,
            violation_events,
            ALL_METRICS_INCLUSIONS,
            calculation_month_count=36,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(1, len(metrics))
        metric = metrics[0]
        self.assertEqual(metric.supervision_violation_id, 23456)
