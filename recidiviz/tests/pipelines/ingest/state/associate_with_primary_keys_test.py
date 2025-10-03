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
"""Testing the AssociateRootEntitiesWithPrimaryKeys PTransform."""
from datetime import datetime

import apache_beam as beam
from apache_beam.pipeline_test import assert_that, equal_to

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonExternalId,
)
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.pipelines.ingest.state.generate_primary_keys import (
    generate_primary_key,
    string_representation,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline


class TestAssociateRootEntitiesWithPrimaryKeys(BigQueryEmulatorTestCase):
    """Tests the AssociateRootEntitiesWithPrimaryKeys PTransform."""

    def setUp(self) -> None:
        super().setUp()
        self.test_pipeline = create_test_pipeline()

        self.ingest_view_1 = "ingest_view_1"
        self.ingest_view_2 = "ingest_view_2"

        self.external_id_1 = ("ID1", "TYPE1")
        self.external_id_2 = ("ID2", "TYPE2")
        self.external_id_3 = ("ID3", "TYPE1")
        self.external_id_4 = ("ID4", "TYPE1")

        self.person12 = StatePerson(
            state_code=self.state_code().value,
            external_ids=[
                StatePersonExternalId(
                    state_code=self.state_code().value,
                    external_id=self.external_id_1[0],
                    id_type=self.external_id_1[1],
                ),
                StatePersonExternalId(
                    state_code=self.state_code().value,
                    external_id=self.external_id_2[0],
                    id_type=self.external_id_2[1],
                ),
            ],
        )
        self.person3 = StatePerson(
            state_code=self.state_code().value,
            external_ids=[
                StatePersonExternalId(
                    state_code=self.state_code().value,
                    external_id=self.external_id_3[0],
                    id_type=self.external_id_3[1],
                )
            ],
        )

        self.date1 = datetime(2020, 1, 1).timestamp()
        self.date2 = datetime(2020, 1, 2).timestamp()
        self.date3 = datetime(2020, 1, 3).timestamp()

        self.primary_key_12 = generate_primary_key(
            string_representation({self.external_id_1, self.external_id_2}),
            StateCode(self.state_code().value),
        )
        self.primary_key_3 = generate_primary_key(
            string_representation({self.external_id_3}),
            StateCode(self.state_code().value),
        )

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_DD

    def test_associate_root_entities_with_primary_keys(self) -> None:
        expected_output = [
            (self.primary_key_3, {(self.date1, self.ingest_view_1): [self.person3]})
        ]
        primary_keys = self.test_pipeline | "Create primary keys" >> beam.Create(
            [(self.external_id_3, self.primary_key_3)]
        )
        merged_root_entities_with_dates = (
            self.test_pipeline
            | "Create date root entity tuples"
            >> beam.Create(
                [(self.external_id_3, (self.date1, self.ingest_view_1, self.person3))]
            )
        )
        output = {
            pipeline.PRIMARY_KEYS: primary_keys,
            pipeline.MERGED_ROOT_ENTITIES_WITH_DATES: merged_root_entities_with_dates,
        } | pipeline.AssociateRootEntitiesWithPrimaryKeys()
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_associate_root_entities_with_primary_keys_multiple_external_ids(
        self,
    ) -> None:
        # We expect duplication of the persons here because 1) the local runner is likely
        # operating differently than the parallel worker runners in the Dataflow environment
        # and 2) this will be handled properly by entity matching later on.
        expected_output = [
            (
                self.primary_key_12,
                {
                    (self.date1, self.ingest_view_1): [self.person12, self.person12],
                },
            )
        ]
        primary_keys = self.test_pipeline | "Create primary keys" >> beam.Create(
            [
                (self.external_id_1, self.primary_key_12),
                (self.external_id_2, self.primary_key_12),
            ]
        )
        merged_root_entities_with_dates = (
            self.test_pipeline
            | "Create date root entity tuples"
            >> beam.Create(
                [
                    (
                        self.external_id_1,
                        (self.date1, self.ingest_view_1, self.person12),
                    ),
                    (
                        self.external_id_2,
                        (self.date1, self.ingest_view_1, self.person12),
                    ),
                ]
            )
        )
        output = {
            pipeline.PRIMARY_KEYS: primary_keys,
            pipeline.MERGED_ROOT_ENTITIES_WITH_DATES: merged_root_entities_with_dates,
        } | pipeline.AssociateRootEntitiesWithPrimaryKeys()
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_associate_root_entities_with_primary_keys_multiple_dates(self) -> None:
        expected_output = [
            (
                self.primary_key_3,
                {
                    (self.date1, self.ingest_view_1): [self.person3],
                    (self.date2, self.ingest_view_1): [self.person3],
                },
            )
        ]
        primary_keys = self.test_pipeline | "Create primary keys" >> beam.Create(
            [(self.external_id_3, self.primary_key_3)]
        )
        merged_root_entities_with_dates = (
            self.test_pipeline
            | "Create date root entity tuples"
            >> beam.Create(
                [
                    (
                        self.external_id_3,
                        (self.date1, self.ingest_view_1, self.person3),
                    ),
                    (
                        self.external_id_3,
                        (self.date2, self.ingest_view_1, self.person3),
                    ),
                ]
            )
        )
        output = {
            pipeline.PRIMARY_KEYS: primary_keys,
            pipeline.MERGED_ROOT_ENTITIES_WITH_DATES: merged_root_entities_with_dates,
        } | pipeline.AssociateRootEntitiesWithPrimaryKeys()
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_associate_root_entities_with_primary_keys_multiple_views(self) -> None:
        expected_output = [
            (
                self.primary_key_3,
                {
                    (self.date1, self.ingest_view_1): [self.person3],
                    (self.date1, self.ingest_view_2): [self.person3],
                },
            )
        ]
        primary_keys = self.test_pipeline | "Create primary keys" >> beam.Create(
            [(self.external_id_3, self.primary_key_3)]
        )
        merged_root_entities_with_dates = (
            self.test_pipeline
            | "Create date root entity tuples"
            >> beam.Create(
                [
                    (
                        self.external_id_3,
                        (self.date1, self.ingest_view_1, self.person3),
                    ),
                    (
                        self.external_id_3,
                        (self.date1, self.ingest_view_2, self.person3),
                    ),
                ]
            )
        )
        output = {
            pipeline.PRIMARY_KEYS: primary_keys,
            pipeline.MERGED_ROOT_ENTITIES_WITH_DATES: merged_root_entities_with_dates,
        } | pipeline.AssociateRootEntitiesWithPrimaryKeys()
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_associate_root_entities_with_primary_keys_multiple_dates_multiple_views(
        self,
    ) -> None:
        expected_output = [
            (
                self.primary_key_3,
                {
                    (self.date1, self.ingest_view_1): [self.person3],
                    (self.date2, self.ingest_view_1): [self.person3],
                    (self.date1, self.ingest_view_2): [self.person3],
                    (self.date2, self.ingest_view_2): [self.person3],
                },
            )
        ]
        primary_keys = self.test_pipeline | "Create primary keys" >> beam.Create(
            [(self.external_id_3, self.primary_key_3)]
        )
        merged_root_entities_with_dates = (
            self.test_pipeline
            | "Create date root entity tuples"
            >> beam.Create(
                [
                    (
                        self.external_id_3,
                        (self.date1, self.ingest_view_1, self.person3),
                    ),
                    (
                        self.external_id_3,
                        (self.date2, self.ingest_view_1, self.person3),
                    ),
                    (
                        self.external_id_3,
                        (self.date1, self.ingest_view_2, self.person3),
                    ),
                    (
                        self.external_id_3,
                        (self.date2, self.ingest_view_2, self.person3),
                    ),
                ]
            )
        )
        output = {
            pipeline.PRIMARY_KEYS: primary_keys,
            pipeline.MERGED_ROOT_ENTITIES_WITH_DATES: merged_root_entities_with_dates,
        } | pipeline.AssociateRootEntitiesWithPrimaryKeys()
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_associate_root_entities_with_primary_keys_multiple_dates_multiple_external_ids(
        self,
    ) -> None:
        # We expect duplication of the persons here because 1) the local runner is likely
        # operating differently than the parallel worker runners in the Dataflow environment
        # and 2) this will be handled properly by entity matching later on.
        expected_output = [
            (
                self.primary_key_12,
                {
                    (self.date1, self.ingest_view_1): [self.person12, self.person12],
                    (self.date2, self.ingest_view_1): [self.person12, self.person12],
                },
            )
        ]
        primary_keys = self.test_pipeline | "Create primary keys" >> beam.Create(
            [
                (self.external_id_1, self.primary_key_12),
                (self.external_id_2, self.primary_key_12),
            ]
        )
        merged_root_entities_with_dates = (
            self.test_pipeline
            | "Create date root entity tuples"
            >> beam.Create(
                [
                    (
                        self.external_id_1,
                        (self.date1, self.ingest_view_1, self.person12),
                    ),
                    (
                        self.external_id_2,
                        (self.date1, self.ingest_view_1, self.person12),
                    ),
                    (
                        self.external_id_1,
                        (self.date2, self.ingest_view_1, self.person12),
                    ),
                    (
                        self.external_id_2,
                        (self.date2, self.ingest_view_1, self.person12),
                    ),
                ]
            )
        )
        output = {
            pipeline.PRIMARY_KEYS: primary_keys,
            pipeline.MERGED_ROOT_ENTITIES_WITH_DATES: merged_root_entities_with_dates,
        } | pipeline.AssociateRootEntitiesWithPrimaryKeys()
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_associate_root_entities_with_primary_keys_all_combinations(self) -> None:
        # We expect duplication of the persons here because 1) the local runner is likely
        # operating differently than the parallel worker runners in the Dataflow environment
        # and 2) this will be handled properly by entity matching later on.
        expected_output = [
            (
                self.primary_key_12,
                {
                    (self.date1, self.ingest_view_1): [self.person12, self.person12],
                    (self.date1, self.ingest_view_2): [self.person12],
                    (self.date2, self.ingest_view_1): [self.person12, self.person12],
                    (self.date3, self.ingest_view_2): [self.person12, self.person12],
                },
            ),
            (
                self.primary_key_3,
                {
                    (self.date1, self.ingest_view_1): [self.person3],
                    (self.date2, self.ingest_view_1): [self.person3],
                    (self.date3, self.ingest_view_1): [self.person3],
                },
            ),
        ]
        primary_keys = self.test_pipeline | "Create primary keys" >> beam.Create(
            [
                (self.external_id_1, self.primary_key_12),
                (self.external_id_2, self.primary_key_12),
                (self.external_id_3, self.primary_key_3),
            ]
        )
        merged_root_entities_with_dates = (
            self.test_pipeline
            | "Create date root entity tuples"
            >> beam.Create(
                [
                    (
                        self.external_id_1,
                        (self.date1, self.ingest_view_1, self.person12),
                    ),
                    (
                        self.external_id_1,
                        (self.date1, self.ingest_view_2, self.person12),
                    ),
                    (
                        self.external_id_2,
                        (self.date1, self.ingest_view_1, self.person12),
                    ),
                    (
                        self.external_id_3,
                        (self.date1, self.ingest_view_1, self.person3),
                    ),
                    (
                        self.external_id_1,
                        (self.date2, self.ingest_view_1, self.person12),
                    ),
                    (
                        self.external_id_2,
                        (self.date2, self.ingest_view_1, self.person12),
                    ),
                    (
                        self.external_id_3,
                        (self.date2, self.ingest_view_1, self.person3),
                    ),
                    (
                        self.external_id_1,
                        (self.date3, self.ingest_view_2, self.person12),
                    ),
                    (
                        self.external_id_2,
                        (self.date3, self.ingest_view_2, self.person12),
                    ),
                    (
                        self.external_id_3,
                        (self.date3, self.ingest_view_1, self.person3),
                    ),
                ]
            )
        )
        output = {
            pipeline.PRIMARY_KEYS: primary_keys,
            pipeline.MERGED_ROOT_ENTITIES_WITH_DATES: merged_root_entities_with_dates,
        } | pipeline.AssociateRootEntitiesWithPrimaryKeys()
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()
