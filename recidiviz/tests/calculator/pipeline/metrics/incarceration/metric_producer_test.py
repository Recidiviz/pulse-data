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
"""Tests for incarceration/metric_producer.py."""
# pylint: disable=unused-import,wrong-import-order
import unittest
from collections import defaultdict
from datetime import date
from typing import Dict, Sequence, Type

import mock
from freezegun import freeze_time

from recidiviz.calculator.pipeline.metrics.incarceration import (
    metric_producer,
    pipeline,
)
from recidiviz.calculator.pipeline.metrics.incarceration.events import (
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
    IncarcerationStayEvent,
)
from recidiviz.calculator.pipeline.metrics.incarceration.metrics import (
    IncarcerationCommitmentFromSupervisionMetric,
    IncarcerationMetricType,
)
from recidiviz.calculator.pipeline.utils.metric_utils import (
    PersonMetadata,
    RecidivizMetric,
)
from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason as AdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
)

ALL_METRICS_INCLUSIONS_DICT = {
    IncarcerationMetricType.INCARCERATION_ADMISSION: True,
    IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION: True,
    IncarcerationMetricType.INCARCERATION_POPULATION: True,
    IncarcerationMetricType.INCARCERATION_RELEASE: True,
}

_COUNTY_OF_RESIDENCE = "county"

_DEFAULT_PERSON_METADATA = PersonMetadata(prioritized_race_or_ethnicity="BLACK")
PIPELINE_JOB_ID = "TEST_JOB_ID"


class TestProduceIncarcerationMetrics(unittest.TestCase):
    """Tests the produce_incarceration_metrics function."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.IncarcerationMetricProducer()
        self.pipeline_config = (
            pipeline.IncarcerationPipelineRunDelegate.pipeline_config()
        )

    def test_produce_incarceration_metrics(self) -> None:
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

        incarceration_event = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NEW_ADMISSION",
            event_date=date(2000, 3, 12),
            facility="FACILITY X",
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [incarceration_event]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2000-03",
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

        for metric in metrics:
            assert metric.year == 2000

    def test_produce_incarceration_metrics_all_types(self) -> None:
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

        incarceration_events = [
            IncarcerationStayEvent(
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
                admission_reason_raw_text="NEW_ADMISSION",
                state_code="US_XX",
                event_date=date(2000, 3, 31),
                facility="FACILITY X",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStandardAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 12),
                facility="FACILITY X",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationReleaseEvent(
                state_code="US_XX",
                event_date=date(2003, 4, 12),
                facility="FACILITY X",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                supervision_type_at_release=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 12),
                facility="FACILITY X",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_incarceration_metrics_two_admissions_same_month(self) -> None:
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

        incarceration_events = [
            IncarcerationStandardAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 12),
                facility="FACILITY X",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.NEW_ADMISSION,
            ),
            IncarcerationStandardAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 17),
                facility="FACILITY X",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.NEW_ADMISSION,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_incarceration_metrics_commitment_from_supervision(self) -> None:
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

        incarceration_events = [
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 12),
                facility="FACILITY X",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        # Assert that the IncarcerationCommitmentFromSupervisionAdmissionEvent produces both an
        # INCARCERATION_ADMISSION and INCARCERATION_COMMITMENT_FROM_SUPERVISION metric
        self.assertTrue(
            IncarcerationMetricType.INCARCERATION_ADMISSION
            in [metric.metric_type for metric in metrics]
        )
        self.assertTrue(
            IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION
            in [metric.metric_type for metric in metrics]
        )
        # Assert that both metrics produce a supervision_type field with type PAROLE.
        self.assertTrue(
            StateSupervisionPeriodSupervisionType.PAROLE
            in [metric.supervision_type for metric in metrics]  # type: ignore[attr-defined]
        )

        self.assertEqual(expected_count, len(metrics))

    def test_produce_incarceration_metrics_two_releases_same_month(self) -> None:
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

        incarceration_events = [
            IncarcerationReleaseEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 12),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
            IncarcerationReleaseEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 24),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    @freeze_time("2020-01-01")
    def test_produce_incarceration_metrics_two_stays_same_month(self) -> None:
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

        incarceration_events = [
            IncarcerationStayEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 31),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStayEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 31),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_incarceration_metrics_two_stays_same_month_facility(self) -> None:
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

        incarceration_events = [
            IncarcerationStayEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 31),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStayEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 31),
                facility="FACILITY 18",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_incarceration_metrics_multiple_stays(self) -> None:
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

        incarceration_events = [
            IncarcerationStayEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 31),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStayEvent(
                state_code="US_XX",
                event_date=date(2010, 4, 30),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStayEvent(
                state_code="US_XX",
                event_date=date(2010, 5, 31),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_incarceration_metrics_multiple_stays_one_month(self) -> None:
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

        incarceration_events = [
            IncarcerationStayEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 13),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStayEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 14),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStayEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 15),
                facility="FACILITY 33",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_incarceration_metrics_multiple_overlapping_stays(self) -> None:
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

        incarceration_events = [
            IncarcerationStayEvent(
                state_code="US_ND",
                event_date=date(2019, 11, 30),
                facility="JRCC",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStayEvent(
                state_code="US_ND",
                event_date=date(2019, 11, 30),
                facility="JRCC",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStayEvent(
                state_code="US_ND",
                event_date=date(2019, 11, 30),
                facility="JRCC",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    @freeze_time("2000-03-30")
    def test_produce_incarceration_metrics_calculation_month_count(self) -> None:
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

        incarceration_event = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            event_date=date(2000, 3, 12),
            facility="FACILITY X",
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [incarceration_event]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month="2000-03",
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

        for metric in metrics:
            assert metric.year == 2000

    @freeze_time("2000-03-30")
    def test_produce_incarceration_metrics_calculation_month_count_exclude(
        self,
    ) -> None:
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

        incarceration_event = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            event_date=date(1990, 3, 12),
            facility="FACILITY X",
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [incarceration_event]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        self.assertEqual(0, len(metrics))

    @freeze_time("2000-03-30")
    def test_produce_incarceration_metrics_calculation_month_count_include_one(
        self,
    ) -> None:
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

        incarceration_event_include = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            event_date=date(2000, 3, 12),
            facility="FACILITY X",
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_event_exclude = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            event_date=date(1994, 3, 12),
            facility="FACILITY X",
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [
            incarceration_event_include,
            incarceration_event_exclude,
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=36,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count([incarceration_event_include])

        self.assertEqual(expected_count, len(metrics))

        for metric in metrics:
            assert metric.year == 2000

    @freeze_time("2010-12-31")
    def test_produce_incarceration_metrics_calculation_month_count_include_monthly(
        self,
    ) -> None:
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

        incarceration_event = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            event_date=date(2007, 12, 12),
            facility="FACILITY X",
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        incarceration_events = [incarceration_event]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=37,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

        for metric in metrics:
            assert metric.year == 2007

    @mock.patch(
        "recidiviz.calculator.pipeline.utils.calculator_utils"
        ".PRIMARY_PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE",
        {
            "incarceration": {
                StateCode.US_XX: "US_XX_DOC",
                StateCode.US_WW: "US_WW_DOC",
            },
            "other_pipeline": {
                StateCode.US_XX: "US_XX_SID",
                StateCode.US_WW: "US_WW_SID",
            },
        },
    )
    @mock.patch(
        "recidiviz.calculator.pipeline.utils.calculator_utils"
        ".SECONDARY_PERSON_EXTERNAL_ID_TYPES_TO_INCLUDE",
        {
            "incarceration": {
                StateCode.US_XX: "US_XX_SID",
                StateCode.US_WW: "US_WW_SID",
            },
            "other_pipeline": {
                StateCode.US_XX: "US_XX_DOC",
                StateCode.US_WW: "US_WW_DOC",
            },
        },
    )
    def test_produce_incarceration_metrics_secondary_person_external_id(self) -> None:
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
            races=[
                StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.WHITE)
            ],
            ethnicities=[
                StatePersonEthnicity.new_with_defaults(
                    state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
                )
            ],
            external_ids=[
                StatePersonExternalId.new_with_defaults(
                    external_id="DOC1341", id_type="US_XX_DOC", state_code="US_XX"
                ),
                StatePersonExternalId.new_with_defaults(
                    external_id="SID9889", id_type="US_XX_SID", state_code="US_XX"
                ),
            ],
        )

        incarceration_events = [
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 12),
                facility="FACILITY X",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_events=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            calculation_end_month=None,
            calculation_month_count=-1,
            person_metadata=_DEFAULT_PERSON_METADATA,
            pipeline_job_id=PIPELINE_JOB_ID,
            pipeline_type=self.pipeline_config.pipeline_type,
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))
        for metric in metrics:
            self.assertEqual("DOC1341", metric.person_external_id)

            if isinstance(metric, IncarcerationCommitmentFromSupervisionMetric):
                self.assertEqual("SID9889", metric.secondary_person_external_id)

    def expected_metrics_count(
        self, incarceration_events: Sequence[IncarcerationEvent]
    ) -> int:
        """Calculates the expected number of characteristic combinations given the
        incarceration events."""
        output_count_by_metric_class: Dict[
            Type[RecidivizMetric[IncarcerationMetricType]], int
        ] = defaultdict(int)

        for event in incarceration_events:
            metric_classes = self.metric_producer.event_to_metric_classes[type(event)]

            for metric_class in metric_classes:
                output_count_by_metric_class[metric_class] += 1

        return sum(value for value in output_count_by_metric_class.values())
