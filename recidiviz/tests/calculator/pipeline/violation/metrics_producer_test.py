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

from recidiviz.calculator.pipeline.utils.metric_utils import PersonMetadata
from recidiviz.calculator.pipeline.violation import metric_producer, pipeline
from recidiviz.calculator.pipeline.violation.events import (
    ViolationEvent,
    ViolationWithResponseEvent,
)
from recidiviz.calculator.pipeline.violation.metrics import (
    ViolationMetric,
    ViolationMetricType,
)
from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonEthnicity,
    StatePersonRace,
)

ALL_METRICS_INCLUSIONS_DICT = {metric_type: True for metric_type in ViolationMetricType}

_DEFAULT_PERSON_METADATA = PersonMetadata(prioritized_race_or_ethnicity="BLACK")

PIPELINE_JOB_ID = "TEST_JOB_ID"


class TestProduceViolationMetrics(unittest.TestCase):
    """Tests the produce_violation_metrics function."""

    def setUp(self) -> None:
        self.state_code = "US_ND"
        self.person = StatePerson.new_with_defaults(
            state_code=self.state_code,
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        self.race = StatePersonRace.new_with_defaults(
            state_code=self.state_code, race=Race.WHITE
        )

        self.person.races = [self.race]

        self.ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code=self.state_code, ethnicity=Ethnicity.NOT_HISPANIC
        )

        self.person.ethnicities = [self.ethnicity]
        self.metric_producer = metric_producer.ViolationMetricProducer()
        self.pipeline_config = pipeline.ViolationPipelineRunDelegate.pipeline_config()

    @freeze_time("2030-11-02")
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
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        self.assertEqual(1, len(metrics))

    @freeze_time("2020-05-30")
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
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        self.assertEqual(1, len(metrics))
        metric = metrics[0]
        self.assertEqual(
            metric.supervision_violation_id, included_event.supervision_violation_id
        )

    @freeze_time("2020-05-30")
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
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=36,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        self.assertEqual(1, len(metrics))
        metric = metrics[0]
        self.assertEqual(metric.supervision_violation_id, 23456)
