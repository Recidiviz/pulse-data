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
from types import ModuleType
from typing import Iterable, Optional, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that, equal_to
from mock import patch

from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonExternalId,
)
from recidiviz.pipelines.ingest.state.merge_ingest_view_root_entity_trees import (
    MergeIngestViewRootEntityTrees,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fixture_util import read_ingest_view_results_fixture
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    DEFAULT_UPDATE_DATETIME,
)
from recidiviz.tests.pipelines.ingest.state.ingest_region_test_mixin import (
    IngestRegionTestMixin,
)


class TestMergeIngestViewRootEntityTrees(
    BigQueryEmulatorTestCase, IngestRegionTestMixin
):
    """Tests the MergeIngestViewRootEntityTrees PTransform."""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_DD

    @classmethod
    def region_module_override(cls) -> Optional[ModuleType]:
        return fake_regions

    def _get_input_root_entities_with_upperbound_dates(
        self, *, ingest_view_name: str, file_name_w_suffix: str
    ) -> Iterable[Tuple[float, Entity]]:
        """Reads fake ingest view input rows from the fixture file associated with the
        provided |ingest_view_name| and |test_name|, then parses the rows into root
        entity trees, returning those along with their associated upper bound date.
        """
        parser_context = IngestViewContentsContext.build_for_tests()
        df = read_ingest_view_results_fixture(
            self.state_code(), ingest_view_name, file_name_w_suffix, False
        )
        return [
            (DEFAULT_UPDATE_DATETIME.timestamp(), row)
            for row in self.ingest_view_manifest_collector()
            .ingest_view_to_manifest[ingest_view_name]
            .parse_contents(
                contents_iterator=df.to_dict("records"),
                context=parser_context,
            )
        ]

    def test_merge_root_entity_trees(self) -> None:
        expected_output = [
            (
                ("ID1", "US_DD_ID_TYPE"),
                (
                    (
                        DEFAULT_UPDATE_DATETIME.timestamp(),
                        "ingestMultipleChildren",
                        StatePerson(
                            state_code=self.state_code().value,
                            external_ids=[
                                StatePersonExternalId(
                                    state_code=self.state_code().value,
                                    external_id="ID1",
                                    id_type="US_DD_ID_TYPE",
                                ),
                            ],
                            incarceration_periods=[
                                StateIncarcerationPeriod(
                                    state_code=self.state_code().value,
                                    external_id="IC1",
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    admission_date=date(2018, 1, 1),
                                    release_date=date(2019, 1, 1),
                                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                                ),
                                StateIncarcerationPeriod(
                                    state_code=self.state_code().value,
                                    external_id="IC2",
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    admission_date=date(2020, 1, 1),
                                    release_date=date(2021, 1, 1),
                                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                                ),
                                StateIncarcerationPeriod(
                                    state_code=self.state_code().value,
                                    external_id="IC4",
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    admission_date=date(2021, 6, 1),
                                    release_date=date(2021, 12, 1),
                                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                                ),
                                StateIncarcerationPeriod(
                                    state_code=self.state_code().value,
                                    external_id="IC3",
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    admission_date=date(2022, 1, 1),
                                    release_date=date(2023, 1, 1),
                                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
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
                    file_name_w_suffix="for_merge_ingest_view_root_entity_trees_test.csv",
                )
            )
            | MergeIngestViewRootEntityTrees(
                "ingestMultipleChildren",
                self.state_code(),
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
                    state_code=self.state_code().value,
                    external_ids=[
                        StatePersonExternalId(
                            state_code=self.state_code().value,
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
                    state_code=self.state_code().value,
                    external_ids=[
                        StatePersonExternalId(
                            state_code=self.state_code().value,
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
                        "test_ingest_view",
                        StatePerson(
                            state_code=self.state_code().value,
                            external_ids=[
                                StatePersonExternalId(
                                    state_code=self.state_code().value,
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
            | MergeIngestViewRootEntityTrees(
                "test_ingest_view",
                self.state_code(),
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
                    state_code=self.state_code().value,
                    external_ids=[
                        StatePersonExternalId(
                            state_code=self.state_code().value,
                            external_id="ID1",
                            id_type="US_DD_ID_TYPE",
                        ),
                    ],
                    incarceration_sentences=[
                        StateIncarcerationSentence(
                            external_id="IS1",
                            state_code=self.state_code().value,
                            incarceration_type=StateIncarcerationType.STATE_PRISON,
                            status=StateSentenceStatus.INTERNAL_UNKNOWN,
                            charges=[
                                StateCharge(
                                    external_id="IC1",
                                    state_code=self.state_code().value,
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
                    state_code=self.state_code().value,
                    external_ids=[
                        StatePersonExternalId(
                            state_code=self.state_code().value,
                            external_id="ID1",
                            id_type="US_DD_ID_TYPE",
                        ),
                    ],
                    incarceration_sentences=[
                        StateIncarcerationSentence(
                            external_id="IS1",
                            state_code=self.state_code().value,
                            incarceration_type=StateIncarcerationType.STATE_PRISON,
                            status=StateSentenceStatus.INTERNAL_UNKNOWN,
                            charges=[
                                StateCharge(
                                    external_id="IC1",
                                    state_code=self.state_code().value,
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
                        "test_ingest_view",
                        StatePerson(
                            state_code=self.state_code().value,
                            external_ids=[
                                StatePersonExternalId(
                                    state_code=self.state_code().value,
                                    external_id="ID1",
                                    id_type="US_DD_ID_TYPE",
                                ),
                            ],
                            incarceration_sentences=[
                                StateIncarcerationSentence(
                                    external_id="IS1",
                                    state_code=self.state_code().value,
                                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                                    status=StateSentenceStatus.INTERNAL_UNKNOWN,
                                    charges=[
                                        StateCharge(
                                            external_id="IC1",
                                            state_code=self.state_code().value,
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
            | MergeIngestViewRootEntityTrees(
                "test_ingest_view",
                self.state_code(),
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
                    state_code=self.state_code().value,
                    external_ids=[
                        StatePersonExternalId(
                            state_code=self.state_code().value,
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
                    state_code=self.state_code().value,
                    external_ids=[
                        StatePersonExternalId(
                            state_code=self.state_code().value,
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
            | MergeIngestViewRootEntityTrees("test_ingest_view", self.state_code())
        )
        with self.assertRaisesRegex(RuntimeError, r".*EntityMergingError.*"):
            self.test_pipeline.run()
