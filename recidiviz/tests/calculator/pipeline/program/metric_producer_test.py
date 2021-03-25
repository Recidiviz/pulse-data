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
# pylint: disable=unused-import,wrong-import-order
import unittest
from collections import defaultdict
from datetime import date
from typing import List, Dict

from freezegun import freeze_time

from recidiviz.calculator.pipeline.program import metric_producer
from recidiviz.calculator.pipeline.program.metric_producer import (
    EVENT_TO_METRIC_TYPES,
)
from recidiviz.calculator.pipeline.program.metrics import ProgramMetricType
from recidiviz.calculator.pipeline.program.program_event import (
    ProgramReferralEvent,
    ProgramEvent,
    ProgramParticipationEvent,
)
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.common.constants.person_characteristics import Gender, Race, Ethnicity
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonRace,
    StatePersonEthnicity,
)

ALL_METRICS_INCLUSIONS_DICT = {metric_type: True for metric_type in ProgramMetricType}

_DEFAULT_PERSON_METADATA = PersonMetadata(prioritized_race_or_ethnicity="BLACK")

PIPELINE_JOB_ID = "TEST_JOB_ID"


class TestMapProgramCombinations(unittest.TestCase):
    """Tests the produce_program_metrics function."""

    @freeze_time("2030-11-02")
    def test_produce_program_metrics(self):
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

        metrics = metric_producer.produce_program_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_program_metrics_full_info(self):
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

        metrics = metric_producer.produce_program_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2009-10",
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_program_metrics_full_info_probation(self):
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

        metrics = metric_producer.produce_program_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2009-10",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_program_metrics_multiple_supervision_types(self):
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

        metrics = metric_producer.produce_program_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2009-10",
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count(program_events)

        self.assertEqual(expected_count, len(metrics))

    @freeze_time("2012-11-30")
    def test_produce_program_metrics_calculation_month_count_1(self):
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

        program_events = [included_event, not_included_event]

        metrics = metric_producer.produce_program_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count([included_event])

        self.assertEqual(expected_count, len(metrics))

    @freeze_time("2012-12-31")
    def test_produce_program_metrics_calculation_month_count_36(self):
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

        program_events = [included_event, not_included_event]

        metrics = metric_producer.produce_program_metrics(
            person,
            program_events,
            ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=36,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        expected_count = expected_metrics_count([included_event])

        self.assertEqual(expected_count, len(metrics))


def expected_metrics_count(program_events: List[ProgramEvent]) -> int:
    """Calculates the expected number of metrics given the incarceration events."""
    output_count_by_metric_type: Dict[ProgramMetricType, int] = defaultdict(int)

    for event_type, metric_type in EVENT_TO_METRIC_TYPES.items():
        output_count_by_metric_type[metric_type] += len(
            [event for event in program_events if isinstance(event, event_type)]
        )

    return sum(value for value in output_count_by_metric_type.values())
