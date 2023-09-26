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
"""Tests the MergeIngestViewRootEntityTrees PTransform."""
from datetime import date, datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that, equal_to

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StatePerson,
    StatePersonExternalId,
)
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.pipelines.ingest.state.test_case import StateIngestPipelineTestCase


class TestMergeIngestViewRootEntityTrees(StateIngestPipelineTestCase):
    """Tests the MergeIngestViewRootEntityTrees PTransform."""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)

    def test_merge_root_entity_trees(self) -> None:
        expected_output = [
            (
                ("ID1", "US_DD_ID_TYPE"),
                (
                    (
                        datetime(2022, 7, 1, 0, 0).timestamp(),
                        StatePerson(
                            state_code=self.region_code.value,
                            external_ids=[
                                StatePersonExternalId(
                                    state_code=self.region_code.value,
                                    external_id="ID1",
                                    id_type="US_DD_ID_TYPE",
                                ),
                            ],
                            incarceration_periods=[
                                StateIncarcerationPeriod(
                                    state_code=self.region_code.value,
                                    external_id="IC1",
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    admission_date=date(2018, 1, 1),
                                    release_date=date(2019, 1, 1),
                                ),
                            ],
                        ),
                    )
                ),
            ),
            (
                ("ID1", "US_DD_ID_TYPE"),
                (
                    (
                        datetime(2022, 7, 3, 0, 0).timestamp(),
                        StatePerson(
                            state_code=self.region_code.value,
                            external_ids=[
                                StatePersonExternalId(
                                    state_code=self.region_code.value,
                                    external_id="ID1",
                                    id_type="US_DD_ID_TYPE",
                                ),
                            ],
                            incarceration_periods=[
                                StateIncarcerationPeriod(
                                    state_code=self.region_code.value,
                                    external_id="IC2",
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    admission_date=date(2020, 1, 1),
                                    release_date=date(2021, 1, 1),
                                ),
                            ],
                        ),
                    )
                ),
            ),
            (
                ("ID1", "US_DD_ID_TYPE"),
                (
                    (
                        datetime(2022, 7, 4, 0, 0).timestamp(),
                        StatePerson(
                            state_code=self.region_code.value,
                            external_ids=[
                                StatePersonExternalId(
                                    state_code=self.region_code.value,
                                    external_id="ID1",
                                    id_type="US_DD_ID_TYPE",
                                ),
                            ],
                            # This is the result of a merged tree.
                            incarceration_periods=[
                                StateIncarcerationPeriod(
                                    state_code=self.region_code.value,
                                    external_id="IC4",
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    admission_date=date(2021, 6, 1),
                                    release_date=date(2021, 12, 1),
                                ),
                                StateIncarcerationPeriod(
                                    state_code=self.region_code.value,
                                    external_id="IC3",
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    admission_date=date(2022, 1, 1),
                                    release_date=date(2023, 1, 1),
                                ),
                            ],
                        ),
                    )
                ),
            ),
        ]
        output = (
            self.test_pipeline
            | beam.Create(
                self.get_expected_root_entities_with_upperbound_dates(
                    ingest_view_name="ingestMultipleChildren",
                    test_name="ingestMultipleChildren",
                )
            )
            | pipeline.MergeIngestViewRootEntityTrees("ingestMultipleChildren")
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()
