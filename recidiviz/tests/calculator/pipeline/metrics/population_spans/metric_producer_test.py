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
from datetime import date
from typing import Dict

import attr
from freezegun import freeze_time

from recidiviz.calculator.pipeline.metrics.population_spans import metric_producer
from recidiviz.calculator.pipeline.metrics.population_spans.metrics import (
    IncarcerationPopulationSpanMetric,
    PopulationSpanMetricType,
)
from recidiviz.calculator.pipeline.metrics.population_spans.spans import (
    IncarcerationPopulationSpan,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import PersonMetadata
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_metrics_producer_delegate import (
    StateSpecificIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_metrics_producer_delegate import (
    UsXxIncarcerationMetricsProducerDelegate,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
)

ALL_METRICS_INCLUSIONS_DICT = {
    PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN: True,
}

PIPELINE_JOB_ID = "TEST_JOB_ID"
CURRENT_DATE = date(2020, 1, 1)


class TestProducePopulationSpanMetrics(unittest.TestCase):
    """Tests the produce_incarceration_population_spans_metrics function."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.PopulationSpanMetricProducer()
        self.person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1982, 8, 31),
            gender=StateGender.FEMALE,
            races=[
                StatePersonRace.new_with_defaults(
                    state_code="US_XX", race=StateRace.WHITE
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
            ethnicities=[
                StatePersonEthnicity.new_with_defaults(
                    state_code="US_XX", ethnicity=StateEthnicity.NOT_HISPANIC
                )
            ],
        )
        self.person_metadata = PersonMetadata(prioritized_race_or_ethnicity="WHITE")
        self.metrics_producer_delegates: Dict[
            str, StateSpecificMetricsProducerDelegate
        ] = {
            StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegate()
        }

    @freeze_time(CURRENT_DATE)
    def test_produce_incarceration_span_metrics(self) -> None:
        incarceration_span = IncarcerationPopulationSpan(
            state_code="US_XX",
            facility="FACILITY X",
            start_date_inclusive=date(2000, 3, 12),
            end_date_exclusive=date(2000, 7, 2),
            judicial_district_code="XXX",
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            included_in_state_population=True,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        metrics = self.metric_producer.produce_metrics(
            person=self.person,
            identifier_results=[incarceration_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            person_metadata=self.person_metadata,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates=self.metrics_producer_delegates,
        )

        self.assertEqual(
            metrics,
            [
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    person_external_id="DOC1341",
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=17,
                    prioritized_race_or_ethnicity="WHITE",
                    gender=StateGender.FEMALE,
                    created_on=CURRENT_DATE,
                    start_date_inclusive=date(2000, 3, 12),
                    end_date_exclusive=date(2000, 7, 2),
                    included_in_state_population=True,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                    judicial_district_code="XXX",
                    secondary_person_external_id="SID9889",
                )
            ],
        )

    @freeze_time(CURRENT_DATE)
    def test_produce_incarceration_span_metrics_split_into_age_spans(self) -> None:
        incarceration_span = IncarcerationPopulationSpan(
            state_code="US_XX",
            facility="FACILITY X",
            start_date_inclusive=date(2000, 3, 12),
            end_date_exclusive=date(2002, 1, 2),
            judicial_district_code="XXX",
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            included_in_state_population=True,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        metrics = self.metric_producer.produce_metrics(
            person=self.person,
            identifier_results=[incarceration_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            person_metadata=self.person_metadata,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates=self.metrics_producer_delegates,
        )

        self.assertEqual(
            metrics,
            [
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    person_external_id="DOC1341",
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=17,
                    prioritized_race_or_ethnicity="WHITE",
                    gender=StateGender.FEMALE,
                    created_on=CURRENT_DATE,
                    start_date_inclusive=date(2000, 3, 12),
                    end_date_exclusive=date(2000, 8, 31),
                    included_in_state_population=True,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                    judicial_district_code="XXX",
                    secondary_person_external_id="SID9889",
                ),
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    person_external_id="DOC1341",
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=18,
                    prioritized_race_or_ethnicity="WHITE",
                    gender=StateGender.FEMALE,
                    created_on=CURRENT_DATE,
                    start_date_inclusive=date(2000, 8, 31),
                    end_date_exclusive=date(2001, 8, 31),
                    included_in_state_population=True,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                    judicial_district_code="XXX",
                    secondary_person_external_id="SID9889",
                ),
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    person_external_id="DOC1341",
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    age=19,
                    prioritized_race_or_ethnicity="WHITE",
                    gender=StateGender.FEMALE,
                    created_on=CURRENT_DATE,
                    start_date_inclusive=date(2001, 8, 31),
                    end_date_exclusive=date(2002, 1, 2),
                    included_in_state_population=True,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                    judicial_district_code="XXX",
                    secondary_person_external_id="SID9889",
                ),
            ],
        )

    @freeze_time(CURRENT_DATE)
    def test_produce_incarceration_span_metrics_no_birthdate(self) -> None:
        person_with_no_birthday = attr.evolve(self.person, birthdate=None)

        incarceration_span = IncarcerationPopulationSpan(
            state_code="US_XX",
            facility="FACILITY X",
            start_date_inclusive=date(2000, 3, 12),
            end_date_exclusive=date(2002, 7, 2),
            judicial_district_code="XXX",
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            included_in_state_population=True,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        metrics = self.metric_producer.produce_metrics(
            person=person_with_no_birthday,
            identifier_results=[incarceration_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            person_metadata=self.person_metadata,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates=self.metrics_producer_delegates,
        )

        self.assertEqual(
            metrics,
            [
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    person_external_id="DOC1341",
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    prioritized_race_or_ethnicity="WHITE",
                    gender=StateGender.FEMALE,
                    created_on=CURRENT_DATE,
                    start_date_inclusive=date(2000, 3, 12),
                    end_date_exclusive=date(2002, 7, 2),
                    included_in_state_population=True,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                    judicial_district_code="XXX",
                    secondary_person_external_id="SID9889",
                )
            ],
        )

    @freeze_time(CURRENT_DATE)
    def test_produce_incarceration_span_metrics_open_span(self) -> None:
        incarceration_span = IncarcerationPopulationSpan(
            state_code="US_XX",
            facility="FACILITY X",
            start_date_inclusive=date(2019, 3, 12),
            end_date_exclusive=None,
            judicial_district_code="XXX",
            purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            included_in_state_population=True,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        metrics = self.metric_producer.produce_metrics(
            person=self.person,
            identifier_results=[incarceration_span],
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
            person_metadata=self.person_metadata,
            pipeline_job_id=PIPELINE_JOB_ID,
            metrics_producer_delegates=self.metrics_producer_delegates,
        )

        self.assertEqual(
            metrics,
            [
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    person_external_id="DOC1341",
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    prioritized_race_or_ethnicity="WHITE",
                    age=36,
                    gender=StateGender.FEMALE,
                    created_on=CURRENT_DATE,
                    start_date_inclusive=date(2019, 3, 12),
                    end_date_exclusive=date(2019, 8, 31),
                    included_in_state_population=True,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                    judicial_district_code="XXX",
                    secondary_person_external_id="SID9889",
                ),
                IncarcerationPopulationSpanMetric(
                    person_id=12345,
                    person_external_id="DOC1341",
                    job_id=PIPELINE_JOB_ID,
                    state_code="US_XX",
                    prioritized_race_or_ethnicity="WHITE",
                    age=37,
                    gender=StateGender.FEMALE,
                    created_on=CURRENT_DATE,
                    start_date_inclusive=date(2019, 8, 31),
                    end_date_exclusive=None,
                    included_in_state_population=True,
                    facility="FACILITY X",
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    custodial_authority=StateCustodialAuthority.STATE_PRISON,
                    judicial_district_code="XXX",
                    secondary_person_external_id="SID9889",
                ),
            ],
        )
