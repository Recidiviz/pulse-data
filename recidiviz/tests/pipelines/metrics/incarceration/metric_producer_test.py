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
import datetime

# pylint: disable=unused-import,wrong-import-order
import unittest
from collections import defaultdict
from datetime import date
from typing import Dict, Sequence, Type

from freezegun import freeze_time

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason as AdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
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
    NormalizedStatePersonEthnicity,
    NormalizedStatePersonExternalId,
    NormalizedStatePersonRace,
)
from recidiviz.pipelines.metrics.incarceration import metric_producer, pipeline
from recidiviz.pipelines.metrics.incarceration.events import (
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
)
from recidiviz.pipelines.metrics.incarceration.metrics import (
    IncarcerationCommitmentFromSupervisionMetric,
    IncarcerationMetricType,
)
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.pipelines.utils.state_utils.state_specific_incarceration_metrics_producer_delegate import (
    StateSpecificIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_incarceration_metrics_producer_delegate import (
    UsXxIncarcerationMetricsProducerDelegate,
)
from recidiviz.tests.pipelines.metrics.utils.calculator_utils_test import (
    UsXxIncarcerationMetricsProducerDelegateForTests,
)

ALL_METRICS_INCLUSIONS = set(IncarcerationMetricType)

PIPELINE_JOB_ID = "TEST_JOB_ID"


class TestProduceIncarcerationMetrics(unittest.TestCase):
    """Tests the produce_incarceration_metrics function."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.IncarcerationMetricProducer()
        self.pipeline_class = pipeline.IncarcerationMetricsPipeline

    @freeze_time("2000-03-01 00:00:00-05:00")
    def test_produce_incarceration_metrics(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NEW_ADMISSION",
            event_date=date(2000, 3, 12),
            facility="FACILITY X",
        )

        incarceration_events = [incarceration_event]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_results=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            calculation_month_count=1,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegateForTests()
            },
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

        for metric in metrics:
            assert metric.year == 2000

    def test_produce_incarceration_metrics_all_types(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationStandardAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 12),
                facility="FACILITY X",
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationReleaseEvent(
                state_code="US_XX",
                event_date=date(2003, 4, 12),
                facility="FACILITY X",
                supervision_type_at_release=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 12),
                facility="FACILITY X",
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_results=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            calculation_month_count=-1,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegateForTests()
            },
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_incarceration_metrics_two_admissions_same_month(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationStandardAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 12),
                facility="FACILITY X",
                admission_reason=AdmissionReason.NEW_ADMISSION,
            ),
            IncarcerationStandardAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 17),
                facility="FACILITY X",
                admission_reason=AdmissionReason.NEW_ADMISSION,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_results=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            calculation_month_count=-1,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegateForTests()
            },
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    def test_produce_incarceration_metrics_commitment_from_supervision(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 12),
                facility="FACILITY X",
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_results=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            calculation_month_count=-1,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegateForTests()
            },
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
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        incarceration_events = [
            IncarcerationReleaseEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 12),
                facility="FACILITY 33",
            ),
            IncarcerationReleaseEvent(
                state_code="US_XX",
                event_date=date(2010, 3, 24),
                facility="FACILITY 33",
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_results=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            calculation_month_count=-1,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegateForTests()
            },
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

    @freeze_time("2000-03-30 00:00:00-05:00")
    def test_produce_incarceration_metrics_calculation_month_count(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            event_date=date(2000, 3, 12),
            facility="FACILITY X",
        )

        incarceration_events = [incarceration_event]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_results=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            calculation_month_count=1,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegateForTests()
            },
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

        for metric in metrics:
            assert metric.year == 2000

    @freeze_time("2000-03-30 00:00:00-05:00")
    def test_produce_incarceration_metrics_calculation_month_count_exclude(
        self,
    ) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            event_date=date(1990, 3, 12),
            facility="FACILITY X",
        )

        incarceration_events = [incarceration_event]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_results=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            calculation_month_count=1,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegateForTests()
            },
        )

        self.assertEqual(0, len(metrics))

    @freeze_time("2000-03-30 00:00:00-05:00")
    def test_produce_incarceration_metrics_calculation_month_count_include_one(
        self,
    ) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        incarceration_event_include = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            event_date=date(2000, 3, 12),
            facility="FACILITY X",
        )

        incarceration_event_exclude = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            event_date=date(1994, 3, 12),
            facility="FACILITY X",
        )

        incarceration_events = [
            incarceration_event_include,
            incarceration_event_exclude,
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_results=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            calculation_month_count=36,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegateForTests()
            },
        )

        expected_count = self.expected_metrics_count([incarceration_event_include])

        self.assertEqual(expected_count, len(metrics))

        for metric in metrics:
            assert metric.year == 2000

    @freeze_time("2010-12-31 00:00:00-05:00")
    def test_produce_incarceration_metrics_calculation_month_count_include_monthly(
        self,
    ) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        incarceration_event = IncarcerationStandardAdmissionEvent(
            state_code="US_XX",
            event_date=date(2007, 12, 12),
            facility="FACILITY X",
        )

        incarceration_events = [incarceration_event]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_results=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            calculation_month_count=37,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegateForTests()
            },
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))

        for metric in metrics:
            assert metric.year == 2007

    def test_produce_incarceration_metrics_supervising_officer_staff_id(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            races=[
                NormalizedStatePersonRace(
                    state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
                )
            ],
            ethnicities=[
                NormalizedStatePersonEthnicity(
                    state_code="US_XX",
                    person_ethnicity_id=12345,
                    ethnicity=StateEthnicity.NOT_HISPANIC,
                )
            ],
            external_ids=[
                NormalizedStatePersonExternalId(
                    external_id="DOC1341",
                    id_type="US_XX_DOC",
                    state_code="US_XX",
                    person_external_id_id=12345,
                    is_current_display_id_for_type=True,
                    is_stable_id_for_type=True,
                    id_active_from_datetime=datetime.datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                ),
                NormalizedStatePersonExternalId(
                    external_id="SID9889",
                    id_type="US_XX_SID",
                    state_code="US_XX",
                    person_external_id_id=12345,
                    is_current_display_id_for_type=True,
                    is_stable_id_for_type=True,
                    id_active_from_datetime=datetime.datetime(2021, 1, 1),
                    id_active_to_datetime=None,
                ),
            ],
        )

        incarceration_events = [
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 3, 12),
                facility="FACILITY X",
                admission_reason=AdmissionReason.REVOCATION,
                admission_reason_raw_text="REVOCATION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervising_officer_staff_id=10000,
            ),
        ]

        metrics = self.metric_producer.produce_metrics(
            person=person,
            identifier_results=incarceration_events,
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            calculation_month_count=-1,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegateForTests()
            },
        )

        expected_count = self.expected_metrics_count(incarceration_events)

        self.assertEqual(expected_count, len(metrics))
        for metric in metrics:
            if isinstance(metric, IncarcerationCommitmentFromSupervisionMetric):
                self.assertEqual(10000, metric.supervising_officer_staff_id)

    def expected_metrics_count(
        self, incarceration_events: Sequence[IncarcerationEvent]
    ) -> int:
        """Calculates the expected number of characteristic combinations given the
        incarceration events."""
        output_count_by_metric_class: Dict[
            Type[RecidivizMetric[IncarcerationMetricType]], int
        ] = defaultdict(int)

        for event in incarceration_events:
            metric_classes = (
                self.metric_producer.result_class_to_metric_classes_mapping[type(event)]
            )

            for metric_class in metric_classes:
                output_count_by_metric_class[metric_class] += 1

        return sum(value for value in output_count_by_metric_class.values())
