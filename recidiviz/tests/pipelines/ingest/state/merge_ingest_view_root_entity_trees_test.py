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
from typing import Iterable, List, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that, equal_to
from dateutil import parser
from mock import patch
from more_itertools import one

from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonExternalId,
)
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    ADDITIONAL_SCHEMA_COLUMNS,
)
from recidiviz.tests.pipelines.ingest.state.test_case import StateIngestPipelineTestCase


class TestMergeIngestViewRootEntityTrees(StateIngestPipelineTestCase):
    """Tests the MergeIngestViewRootEntityTrees PTransform."""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)
        self.field_index = CoreEntityFieldIndex()

    def _get_input_root_entities_with_upperbound_dates(
        self, *, ingest_view_name: str, test_name: str
    ) -> Iterable[Tuple[float, Entity]]:
        """Reads fake ingest view input rows from the fixture file associated with the
        provided |ingest_view_name| and |test_name|, then parses the rows into root
        entity trees, returning those along with their associated upper bound date.
        """
        rows = list(
            self.get_ingest_view_results_from_fixture(
                ingest_view_name=ingest_view_name, test_name=test_name
            )
        )
        results: List[Tuple[float, Entity]] = []
        for row in rows:
            upper_bound_date = parser.isoparse(row[UPPER_BOUND_DATETIME_COL_NAME])
            for column in ADDITIONAL_SCHEMA_COLUMNS:
                row.pop(column.name)
            results.append(
                (
                    upper_bound_date.timestamp(),
                    one(
                        self.ingest_view_manifest_collector.ingest_view_to_manifest[
                            ingest_view_name
                        ].parse_contents(
                            contents_iterator=iter([row]),
                            context=IngestViewContentsContextImpl(
                                ingest_instance=self.ingest_instance,
                                results_update_datetime=upper_bound_date,
                            ),
                        )
                    ),
                )
            )
        return iter(results)

    def test_merge_root_entity_trees(self) -> None:
        expected_output = [
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
                            incarceration_periods=[
                                StateIncarcerationPeriod(
                                    state_code=self.region_code.value,
                                    external_id="IC1",
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    admission_date=date(2018, 1, 1),
                                    release_date=date(2019, 1, 1),
                                ),
                                StateIncarcerationPeriod(
                                    state_code=self.region_code.value,
                                    external_id="IC2",
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    admission_date=date(2020, 1, 1),
                                    release_date=date(2021, 1, 1),
                                ),
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
                self._get_input_root_entities_with_upperbound_dates(
                    ingest_view_name="ingestMultipleChildren",
                    test_name="ingestMultipleChildren",
                )
            )
            | pipeline.MergeIngestViewRootEntityTrees(
                "ingestMultipleChildren", self.region_code, self.field_index
            )
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    @patch(
        "recidiviz.pipelines.ingest.state.merge_ingest_view_root_entity_trees.INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS",
        {StateCode.US_DD: {"test_ingest_view"}},
    )
    def test_merge_entity_trees_passes_with_exemptions(self) -> None:
        expected_input = [
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
                    current_email_address="test1@some-email.com",
                ),
            ),
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
                    current_email_address="test2@some-email.com",
                ),
            ),
        ]
        expected_output = [
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
                            current_email_address="test2@some-email.com",
                        ),
                    )
                ),
            )
        ]
        output = (
            self.test_pipeline
            | beam.Create(expected_input)
            | pipeline.MergeIngestViewRootEntityTrees(
                "test_ingest_view", self.region_code, self.field_index
            )
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    @patch(
        "recidiviz.pipelines.ingest.state.merge_ingest_view_root_entity_trees.INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS",
        {StateCode.US_DD: {"test_ingest_view"}},
    )
    def test_merge_entity_trees_passes_with_exemptions_deeper_in_tree(self) -> None:
        expected_input = [
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
                    incarceration_sentences=[
                        StateIncarcerationSentence(
                            external_id="IS1",
                            state_code=self.region_code.value,
                            incarceration_type=StateIncarcerationType.STATE_PRISON,
                            status=StateSentenceStatus.INTERNAL_UNKNOWN,
                            charges=[
                                StateCharge(
                                    external_id="IC1",
                                    state_code=self.region_code.value,
                                    description="something",
                                    status=StateChargeStatus.INTERNAL_UNKNOWN,
                                ),
                            ],
                        )
                    ],
                ),
            ),
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
                    incarceration_sentences=[
                        StateIncarcerationSentence(
                            external_id="IS1",
                            state_code=self.region_code.value,
                            incarceration_type=StateIncarcerationType.STATE_PRISON,
                            status=StateSentenceStatus.INTERNAL_UNKNOWN,
                            charges=[
                                StateCharge(
                                    external_id="IC1",
                                    state_code=self.region_code.value,
                                    description="something different",
                                    status=StateChargeStatus.INTERNAL_UNKNOWN,
                                ),
                            ],
                        )
                    ],
                ),
            ),
        ]
        expected_output = [
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
                            incarceration_sentences=[
                                StateIncarcerationSentence(
                                    external_id="IS1",
                                    state_code=self.region_code.value,
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    status=StateSentenceStatus.INTERNAL_UNKNOWN,
                                    charges=[
                                        StateCharge(
                                            external_id="IC1",
                                            state_code=self.region_code.value,
                                            description="something different",
                                            status=StateChargeStatus.INTERNAL_UNKNOWN,
                                        ),
                                    ],
                                )
                            ],
                        ),
                    )
                ),
            )
        ]
        output = (
            self.test_pipeline
            | beam.Create(expected_input)
            | pipeline.MergeIngestViewRootEntityTrees(
                "test_ingest_view", self.region_code, self.field_index
            )
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    @patch(
        "recidiviz.pipelines.ingest.state.merge_ingest_view_root_entity_trees.INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS",
        {},
    )
    def test_merge_entity_trees_fails_without_exemptions(self) -> None:
        expected_input = [
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
                    current_email_address="test1@some-email.com",
                ),
            ),
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
                    current_email_address="test2@some-email.com",
                ),
            ),
        ]
        _ = (
            self.test_pipeline
            | beam.Create(expected_input)
            | pipeline.MergeIngestViewRootEntityTrees(
                "test_ingest_view", self.region_code, self.field_index
            )
        )
        with self.assertRaisesRegex(RuntimeError, r".*EntityMatchingError.*"):
            self.test_pipeline.run()
