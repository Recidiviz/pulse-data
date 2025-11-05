# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for population_spans/metric_producer.py."""
import unittest
from datetime import date, datetime

import attr
from freezegun import freeze_time

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodCustodyLevel,
    StateIncarcerationPeriodHousingUnitType,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStatePersonEthnicity,
    NormalizedStatePersonExternalId,
    NormalizedStatePersonRace,
)
from recidiviz.pipelines.metrics.population_spans import metric_producer
from recidiviz.pipelines.metrics.population_spans.metrics import (
    IncarcerationPopulationSpanMetric,
    PopulationSpanMetricType,
    SupervisionPopulationSpanMetric,
)
from recidiviz.pipelines.metrics.population_spans.spans import (
    IncarcerationPopulationSpan,
    SupervisionPopulationSpan,
)

ALL_METRICS_INCLUSIONS = set(PopulationSpanMetricType)

PIPELINE_JOB_ID = "TEST_JOB_ID"
CURRENT_DATETIME = datetime.fromisoformat("2020-01-01 00:00:00-05:00")


class TestProducePopulationSpanMetrics(unittest.TestCase):
    """Tests the produce_incarceration_population_spans_metrics function."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.PopulationSpanMetricProducer()
        self.person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1982, 8, 31),
            gender=StateGender.FEMALE,
            races=[
                NormalizedStatePersonRace(
                    state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
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
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                ),
                NormalizedStatePersonExternalId(
                    external_id="SID9889",
                    id_type="US_XX_SID",
                    state_code="US_XX",
                    person_external_id_id=12345,
                    is_current_display_id_for_type=True,
                    is_stable_id_for_type=True,
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                ),
            ],
            ethnicities=[
                NormalizedStatePersonEthnicity(
                    state_code="US_XX",
                    person_ethnicity_id=12345,
                    ethnicity=StateEthnicity.NOT_HISPANIC,
                )
            ],
        )

    @freeze_time(CURRENT_DATETIME)
    def test_produce_incarceration_span_metrics(self) -> None:
        incarceration_span = IncarcerationPopulationSpan(
            state_code="US_XX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            facility="FACILITY X",
            start_date_inclusive=date(2000, 3, 12),
            end_date_exclusive=date(2000, 7, 2),
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            included_in_state_population=True,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            housing_unit="COUNTY JAIL",
            housing_unit_type=StateIncarcerationPeriodHousingUnitType.GENERAL,
            housing_unit_type_raw_text="US XX COUNTY JAIL",
            custody_level=StateIncarcerationPeriodCustodyLevel.MEDIUM,
            custody_level_raw_text="MEDIUM",
        )

        metrics = self.metric_producer.produce_metrics(
            person=self.person,
            identifier_results=[incarceration_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(
            metrics,
            [
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=17,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2000, 3, 12),
                    end_date_exclusive=date(2000, 7, 2),
                    included_in_state_population=True,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                    custody_level=StateIncarcerationPeriodCustodyLevel.MEDIUM,
                    custody_level_raw_text="MEDIUM",
                    housing_unit="COUNTY JAIL",
                    housing_unit_type=StateIncarcerationPeriodHousingUnitType.GENERAL,
                    housing_unit_type_raw_text="US XX COUNTY JAIL",
                )
            ],
        )

    @freeze_time(CURRENT_DATETIME)
    def test_produce_incarceration_span_metrics_split_into_age_spans(self) -> None:
        incarceration_span = IncarcerationPopulationSpan(
            state_code="US_XX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            facility="FACILITY X",
            start_date_inclusive=date(2000, 3, 12),
            end_date_exclusive=date(2002, 1, 2),
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            included_in_state_population=True,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        metrics = self.metric_producer.produce_metrics(
            person=self.person,
            identifier_results=[incarceration_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(
            metrics,
            [
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=17,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2000, 3, 12),
                    end_date_exclusive=date(2000, 8, 31),
                    included_in_state_population=True,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                ),
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=18,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2000, 8, 31),
                    end_date_exclusive=date(2001, 8, 31),
                    included_in_state_population=True,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                ),
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=19,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2001, 8, 31),
                    end_date_exclusive=date(2002, 1, 2),
                    included_in_state_population=True,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                ),
            ],
        )

    @freeze_time(CURRENT_DATETIME)
    def test_produce_incarceration_span_metrics_no_birthdate(self) -> None:
        person_with_no_birthday = attr.evolve(self.person, birthdate=None)

        incarceration_span = IncarcerationPopulationSpan(
            state_code="US_XX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            facility="FACILITY X",
            start_date_inclusive=date(2000, 3, 12),
            end_date_exclusive=date(2002, 7, 2),
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            included_in_state_population=True,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        metrics = self.metric_producer.produce_metrics(
            person=person_with_no_birthday,
            identifier_results=[incarceration_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(
            metrics,
            [
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2000, 3, 12),
                    end_date_exclusive=date(2002, 7, 2),
                    included_in_state_population=True,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                )
            ],
        )

    @freeze_time(CURRENT_DATETIME)
    def test_produce_incarceration_span_metrics_open_span(self) -> None:
        incarceration_span = IncarcerationPopulationSpan(
            state_code="US_XX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            facility="FACILITY X",
            start_date_inclusive=date(2019, 3, 12),
            end_date_exclusive=None,
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            included_in_state_population=True,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        metrics = self.metric_producer.produce_metrics(
            person=self.person,
            identifier_results=[incarceration_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(
            metrics,
            [
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=36,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2019, 3, 12),
                    end_date_exclusive=date(2019, 8, 31),
                    included_in_state_population=True,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                ),
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=37,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2019, 8, 31),
                    end_date_exclusive=None,
                    included_in_state_population=True,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                ),
            ],
        )

    @freeze_time(CURRENT_DATETIME)
    def test_produce_supervision_span_metrics(self) -> None:
        supervision_span = SupervisionPopulationSpan(
            state_code="US_XX",
            included_in_state_population=True,
            level_1_supervision_location_external_id="site",
            start_date_inclusive=date(2000, 3, 12),
            end_date_exclusive=date(2000, 7, 12),
            supervision_type=StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
            supervision_level=StateSupervisionLevel.DIVERSION,
            supervision_level_raw_text="DIVERSION",
            case_type=StateSupervisionCaseType.GENERAL,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            supervising_officer_staff_id=10000,
        )

        metrics = self.metric_producer.produce_metrics(
            person=self.person,
            identifier_results=[supervision_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(
            metrics,
            [
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=17,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2000, 3, 12),
                    end_date_exclusive=date(2000, 7, 12),
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
                    supervision_level=StateSupervisionLevel.DIVERSION,
                    supervision_level_raw_text="DIVERSION",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                )
            ],
        )

    @freeze_time(CURRENT_DATETIME)
    def test_produce_supervision_span_metrics_split_into_age_spans(self) -> None:
        supervision_span = SupervisionPopulationSpan(
            state_code="US_XX",
            included_in_state_population=True,
            level_1_supervision_location_external_id="site",
            start_date_inclusive=date(2000, 3, 12),
            end_date_exclusive=date(2002, 1, 2),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDIUM",
            case_type=StateSupervisionCaseType.GENERAL,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            supervising_officer_staff_id=10000,
        )

        metrics = self.metric_producer.produce_metrics(
            person=self.person,
            identifier_results=[supervision_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(
            metrics,
            [
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=17,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2000, 3, 12),
                    end_date_exclusive=date(2000, 8, 31),
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    supervision_level=StateSupervisionLevel.MEDIUM,
                    supervision_level_raw_text="MEDIUM",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                ),
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=18,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2000, 8, 31),
                    end_date_exclusive=date(2001, 8, 31),
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    supervision_level=StateSupervisionLevel.MEDIUM,
                    supervision_level_raw_text="MEDIUM",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                ),
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=19,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2001, 8, 31),
                    end_date_exclusive=date(2002, 1, 2),
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    supervision_level=StateSupervisionLevel.MEDIUM,
                    supervision_level_raw_text="MEDIUM",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                ),
            ],
        )

    @freeze_time(CURRENT_DATETIME)
    def test_produce_supervision_span_metrics_no_birthdate(self) -> None:
        person_with_no_birthday = attr.evolve(self.person, birthdate=None)

        supervision_span = SupervisionPopulationSpan(
            state_code="US_XX",
            included_in_state_population=True,
            level_1_supervision_location_external_id="site",
            start_date_inclusive=date(2000, 3, 12),
            end_date_exclusive=date(2002, 7, 2),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDIUM",
            case_type=StateSupervisionCaseType.GENERAL,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            supervising_officer_staff_id=10000,
        )

        metrics = self.metric_producer.produce_metrics(
            person=person_with_no_birthday,
            identifier_results=[supervision_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(
            metrics,
            [
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2000, 3, 12),
                    end_date_exclusive=date(2002, 7, 2),
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    supervision_level=StateSupervisionLevel.MEDIUM,
                    supervision_level_raw_text="MEDIUM",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                ),
            ],
        )

    @freeze_time(CURRENT_DATETIME)
    def test_produce_supervision_span_metrics_open_span(self) -> None:
        supervision_span = SupervisionPopulationSpan(
            state_code="US_XX",
            included_in_state_population=True,
            level_1_supervision_location_external_id="site",
            start_date_inclusive=date(2019, 3, 12),
            end_date_exclusive=None,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDIUM",
            case_type=StateSupervisionCaseType.GENERAL,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            supervising_officer_staff_id=10000,
        )

        metrics = self.metric_producer.produce_metrics(
            person=self.person,
            identifier_results=[supervision_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(
            metrics,
            [
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    created_on=CURRENT_DATETIME,
                    age=36,
                    start_date_inclusive=date(2019, 3, 12),
                    end_date_exclusive=date(2019, 8, 31),
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    supervision_level=StateSupervisionLevel.MEDIUM,
                    supervision_level_raw_text="MEDIUM",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                ),
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    created_on=CURRENT_DATETIME,
                    age=37,
                    start_date_inclusive=date(2019, 8, 31),
                    end_date_exclusive=None,
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    supervision_level=StateSupervisionLevel.MEDIUM,
                    supervision_level_raw_text="MEDIUM",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                ),
            ],
        )

    @freeze_time(CURRENT_DATETIME)
    def test_produce_all_spans(self) -> None:
        incarceration_span = IncarcerationPopulationSpan(
            state_code="US_XX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            facility="FACILITY X",
            start_date_inclusive=date(2015, 3, 1),
            end_date_exclusive=date(2017, 3, 1),
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            included_in_state_population=True,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )
        supervision_span = SupervisionPopulationSpan(
            state_code="US_XX",
            included_in_state_population=True,
            level_1_supervision_location_external_id="site",
            start_date_inclusive=date(2017, 3, 2),
            end_date_exclusive=None,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDIUM",
            case_type=StateSupervisionCaseType.GENERAL,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            supervising_officer_staff_id=10000,
        )

        metrics = self.metric_producer.produce_metrics(
            person=self.person,
            identifier_results=[incarceration_span, supervision_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS,
            pipeline_job_id=PIPELINE_JOB_ID,
        )

        self.assertEqual(
            metrics,
            [
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=32,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2015, 3, 1),
                    end_date_exclusive=date(2015, 8, 31),
                    included_in_state_population=True,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                ),
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=33,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2015, 8, 31),
                    end_date_exclusive=date(2016, 8, 31),
                    included_in_state_population=True,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                ),
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=34,
                    created_on=CURRENT_DATETIME,
                    start_date_inclusive=date(2016, 8, 31),
                    end_date_exclusive=date(2017, 3, 1),
                    included_in_state_population=True,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                ),
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    created_on=CURRENT_DATETIME,
                    age=34,
                    start_date_inclusive=date(2017, 3, 2),
                    end_date_exclusive=date(2017, 8, 31),
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    supervision_level=StateSupervisionLevel.MEDIUM,
                    supervision_level_raw_text="MEDIUM",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                ),
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    created_on=CURRENT_DATETIME,
                    age=35,
                    start_date_inclusive=date(2017, 8, 31),
                    end_date_exclusive=date(2018, 8, 31),
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    supervision_level=StateSupervisionLevel.MEDIUM,
                    supervision_level_raw_text="MEDIUM",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                ),
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    created_on=CURRENT_DATETIME,
                    age=36,
                    start_date_inclusive=date(2018, 8, 31),
                    end_date_exclusive=date(2019, 8, 31),
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    supervision_level=StateSupervisionLevel.MEDIUM,
                    supervision_level_raw_text="MEDIUM",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                ),
                SupervisionPopulationSpanMetric(
                    person_id=12345,
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    created_on=CURRENT_DATETIME,
                    age=37,
                    start_date_inclusive=date(2019, 8, 31),
                    end_date_exclusive=None,
                    included_in_state_population=True,
                    supervising_officer_staff_id=10000,
                    level_1_supervision_location_external_id="site",
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    supervision_level=StateSupervisionLevel.MEDIUM,
                    supervision_level_raw_text="MEDIUM",
                    case_type=StateSupervisionCaseType.GENERAL,
                    custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                ),
            ],
        )
