# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests the MergeRootEntitiesAcrossDates PTransform."""
from datetime import datetime

import apache_beam as beam
import attr
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that
from apache_beam.testing.util import matches_all

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state import entities
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.pipelines.ingest.state.generate_primary_keys import generate_primary_key
from recidiviz.tests.pipelines.ingest.state.test_case import StateIngestPipelineTestCase


class TestMergeRootEntitiesAcrossDates(StateIngestPipelineTestCase):
    """Tests the MergeRootEntitiesAcrossDates PTransform."""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)

    def test_merge_root_entities_across_dates(self) -> None:
        date_1 = datetime(2020, 1, 1)
        date_2 = datetime(2020, 1, 2)

        person_external_id_1 = entities.StatePersonExternalId(
            external_id="ID1", id_type="TYPE", state_code=StateCode.US_DD.value
        )
        person_external_id_2 = entities.StatePersonExternalId(
            external_id="ID2", id_type="TYPE", state_code=StateCode.US_DD.value
        )
        staff_external_id_1 = entities.StateStaffExternalId(
            external_id="ID1", id_type="STAFF", state_code=StateCode.US_DD.value
        )
        staff_external_id_2 = entities.StateStaffExternalId(
            external_id="ID2", id_type="STAFF", state_code=StateCode.US_DD.value
        )

        primary_key_person_1 = generate_primary_key(
            {(person_external_id_1.external_id, person_external_id_1.id_type)},
            StateCode.US_DD,
        )
        primary_key_person_2 = generate_primary_key(
            {(person_external_id_2.external_id, person_external_id_2.id_type)},
            StateCode.US_DD,
        )
        primary_key_staff_1 = generate_primary_key(
            {(staff_external_id_1.external_id, staff_external_id_1.id_type)},
            StateCode.US_DD,
        )
        primary_key_staff_2 = generate_primary_key(
            {(staff_external_id_1.external_id, staff_external_id_1.id_type)},
            StateCode.US_DD,
        )

        person_1_date_1 = entities.StatePerson(
            state_code=StateCode.US_DD.value,
            external_ids=[person_external_id_1],
            incarceration_periods=[
                entities.StateIncarcerationPeriod(
                    state_code=StateCode.US_DD.value,
                    external_id="I11",
                    admission_date=datetime(2019, 1, 1),
                    release_date=datetime(2019, 6, 1),
                )
            ],
        )
        person_1_date_2 = entities.StatePerson(
            state_code=StateCode.US_DD.value,
            external_ids=[person_external_id_1],
            supervision_periods=[
                entities.StateSupervisionPeriod(
                    state_code=StateCode.US_DD.value,
                    external_id="S11",
                    start_date=datetime(2019, 6, 1),
                )
            ],
        )
        person_2_date_1 = entities.StatePerson(
            state_code=StateCode.US_DD.value,
            external_ids=[person_external_id_2],
            incarceration_periods=[
                entities.StateIncarcerationPeriod(
                    state_code=StateCode.US_DD.value,
                    external_id="I21",
                    admission_date=datetime(2019, 2, 1),
                    release_date=datetime(2019, 7, 1),
                )
            ],
        )
        person_2_date_2 = entities.StatePerson(
            state_code=StateCode.US_DD.value,
            external_ids=[person_external_id_2],
            supervision_periods=[
                entities.StateSupervisionPeriod(
                    state_code=StateCode.US_DD.value,
                    external_id="S21",
                    start_date=datetime(2019, 7, 1),
                )
            ],
        )

        staff_1_date_1 = entities.StateStaff(
            state_code=StateCode.US_DD.value, external_ids=[staff_external_id_1]
        )
        staff_1_date_2 = entities.StateStaff(
            state_code=StateCode.US_DD.value, external_ids=[staff_external_id_1]
        )
        staff_2_date_1 = entities.StateStaff(
            state_code=StateCode.US_DD.value, external_ids=[staff_external_id_2]
        )
        staff_2_date_2 = entities.StateStaff(
            state_code=StateCode.US_DD.value, external_ids=[staff_external_id_2]
        )

        output = self.test_pipeline | beam.Create(
            [
                (
                    primary_key_person_1,
                    {date_1: [person_1_date_1], date_2: [person_1_date_2]},
                ),
                (
                    primary_key_person_2,
                    {date_1: [person_2_date_1], date_2: [person_2_date_2]},
                ),
                (
                    primary_key_staff_1,
                    {date_1: [staff_1_date_1], date_2: [staff_1_date_2]},
                ),
                (
                    primary_key_staff_2,
                    {date_1: [staff_2_date_1], date_2: [staff_2_date_2]},
                ),
            ]
            | pipeline.MergeRootEntitiesAcrossDates(StateCode.US_DD)
        )

        person_1 = entities.StatePerson(
            state_code=StateCode.US_DD.value, person_id=primary_key_person_1
        )
        person_1.external_ids = [
            attr.evolve(
                person_external_id_1,
                person=person_1,
                person_external_id_id=generate_primary_key(
                    {(person_external_id_1.external_id, "person_external_id_id")},
                    StateCode.US_DD,
                ),
            )
        ]
        person_1.incarceration_periods = [
            attr.evolve(
                incarceration_period,
                person=person_1,
                incarceration_period_id=generate_primary_key(
                    {(incarceration_period.external_id, "incarceration_period_id")},
                    StateCode.US_DD,
                ),
            )
            for incarceration_period in person_1_date_1.incarceration_periods
        ]
        person_1.supervision_periods = [
            attr.evolve(
                supervision_period,
                person=person_1,
                supervision_period_id=generate_primary_key(
                    {(supervision_period.external_id, "supervision_period_id")},
                    StateCode.US_DD,
                ),
            )
            for supervision_period in person_1_date_2.supervision_periods
        ]

        person_2 = entities.StatePerson(
            state_code=StateCode.US_DD.value, person_id=primary_key_person_2
        )
        person_2.external_ids = [
            attr.evolve(
                person_external_id_2,
                person=person_2,
                person_external_id_id=generate_primary_key(
                    {(person_external_id_2.external_id, "person_external_id_id")},
                    StateCode.US_DD,
                ),
            )
        ]
        person_2.incarceration_periods = [
            attr.evolve(
                incarceration_period,
                person=person_2,
                incarceration_period_id=generate_primary_key(
                    {(incarceration_period.external_id, "incarceration_period_id")},
                    StateCode.US_DD,
                ),
            )
            for incarceration_period in person_2_date_1.incarceration_periods
        ]
        person_2.supervision_periods = [
            attr.evolve(
                supervision_period,
                person=person_2,
                supervision_period_id=generate_primary_key(
                    {(supervision_period.external_id, "supervision_period_id")},
                    StateCode.US_DD,
                ),
            )
            for supervision_period in person_2_date_2.supervision_periods
        ]

        staff_1 = entities.StateStaff(
            state_code=StateCode.US_DD.value, staff_id=primary_key_staff_1
        )
        staff_1.external_ids = [
            attr.evolve(
                staff_external_id_1,
                staff=staff_1,
                staff_external_id_id=generate_primary_key(
                    {(staff_external_id_1.external_id, "staff_external_id_id")},
                    StateCode.US_DD,
                ),
            )
        ]

        staff_2 = entities.StateStaff(
            state_code=StateCode.US_DD.value, staff_id=primary_key_staff_2
        )
        staff_2.external_ids = [
            attr.evolve(
                staff_external_id_2,
                staff=staff_2,
                staff_external_id_id=generate_primary_key(
                    {(staff_external_id_2.external_id, "staff_external_id_id")},
                    StateCode.US_DD,
                ),
            )
        ]

        expected_entities = [person_1, person_2, staff_1, staff_2]

        assert_that(output, matches_all(expected_entities))
        self.test_pipeline.run()
