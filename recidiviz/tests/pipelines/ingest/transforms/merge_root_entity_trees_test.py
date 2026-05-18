# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for the shared MergeIngestViewRootEntityTrees PTransform."""
import unittest
from datetime import date, datetime
from typing import Any

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to

from recidiviz.common.constants.state.state_person import StateRace
from recidiviz.persistence.entity.activity import entities as state_entities
from recidiviz.persistence.entity.activity.entities import (
    StatePerson,
    StatePersonExternalId,
    StatePersonRace,
)
from recidiviz.pipelines.ingest.transforms.merge_root_entity_trees import (
    MergeIngestViewRootEntityTrees,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline

_STATE_CODE = "US_OZ"
_VIEW_NAME = "test_view"


def _make_person(
    external_id: str,
    id_type: str = "US_OZ_EG",
    **kwargs: Any,
) -> StatePerson:
    return StatePerson(
        state_code=_STATE_CODE,
        external_ids=[
            StatePersonExternalId(
                state_code=_STATE_CODE,
                external_id=external_id,
                id_type=id_type,
            ),
        ],
        **kwargs,
    )


class TestMergeIngestViewRootEntityTrees(unittest.TestCase):
    """Pure tests for MergeIngestViewRootEntityTrees, not specific to any pipeline."""

    def test_single_entity_passthrough(self) -> None:
        """A single entity passes through unchanged."""
        ts = datetime(2024, 1, 1).timestamp()
        person = _make_person("P1")

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create([(ts, person)])
                | MergeIngestViewRootEntityTrees(
                    _VIEW_NAME, entities_module=state_entities
                )
            )
            assert_that(
                result,
                equal_to([(("P1", "US_OZ_EG"), (ts, _VIEW_NAME, person))]),
            )

    def test_entity_with_multiple_external_ids_emitted_per_id(self) -> None:
        """An entity with two external IDs produces one output per ID."""
        ts = datetime(2024, 1, 1).timestamp()
        person = StatePerson(
            state_code=_STATE_CODE,
            external_ids=[
                StatePersonExternalId(
                    state_code=_STATE_CODE, external_id="P1", id_type="US_OZ_EG"
                ),
                StatePersonExternalId(
                    state_code=_STATE_CODE, external_id="A1", id_type="US_OZ_ALT"
                ),
            ],
        )

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create([(ts, person)])
                | MergeIngestViewRootEntityTrees(
                    _VIEW_NAME, entities_module=state_entities
                )
            )
            assert_that(
                result,
                equal_to(
                    [
                        (("P1", "US_OZ_EG"), (ts, _VIEW_NAME, person)),
                        (("A1", "US_OZ_ALT"), (ts, _VIEW_NAME, person)),
                    ]
                ),
            )

    def test_two_entities_same_key_merged(self) -> None:
        """Two entities sharing an external ID and date are merged."""
        ts = datetime(2024, 1, 1).timestamp()
        person1 = StatePerson(
            state_code=_STATE_CODE,
            external_ids=[
                StatePersonExternalId(
                    state_code=_STATE_CODE, external_id="P1", id_type="US_OZ_EG"
                ),
            ],
            races=[
                StatePersonRace(
                    state_code=_STATE_CODE,
                    race=StateRace.WHITE,
                    race_raw_text="WHITE",
                ),
            ],
        )
        person2 = StatePerson(
            state_code=_STATE_CODE,
            external_ids=[
                StatePersonExternalId(
                    state_code=_STATE_CODE, external_id="P1", id_type="US_OZ_EG"
                ),
            ],
            races=[
                StatePersonRace(
                    state_code=_STATE_CODE,
                    race=StateRace.BLACK,
                    race_raw_text="BLACK",
                ),
            ],
        )

        expected_merged = StatePerson(
            state_code=_STATE_CODE,
            external_ids=[
                StatePersonExternalId(
                    state_code=_STATE_CODE, external_id="P1", id_type="US_OZ_EG"
                ),
            ],
            races=[
                StatePersonRace(
                    state_code=_STATE_CODE,
                    race=StateRace.WHITE,
                    race_raw_text="WHITE",
                ),
                StatePersonRace(
                    state_code=_STATE_CODE,
                    race=StateRace.BLACK,
                    race_raw_text="BLACK",
                ),
            ],
        )

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create([(ts, person1), (ts, person2)])
                | MergeIngestViewRootEntityTrees(
                    _VIEW_NAME, entities_module=state_entities
                )
            )
            assert_that(
                result,
                equal_to([(("P1", "US_OZ_EG"), (ts, _VIEW_NAME, expected_merged))]),
            )

    def test_different_dates_not_merged(self) -> None:
        """Entities with the same external ID but different dates stay separate."""
        ts1 = datetime(2024, 1, 1).timestamp()
        ts2 = datetime(2024, 6, 1).timestamp()

        person1 = _make_person("P1", birthdate=date(1990, 1, 1))
        person2 = _make_person("P1", birthdate=date(1985, 6, 1))

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create([(ts1, person1), (ts2, person2)])
                | MergeIngestViewRootEntityTrees(
                    _VIEW_NAME, entities_module=state_entities
                )
            )
            assert_that(
                result,
                equal_to(
                    [
                        (("P1", "US_OZ_EG"), (ts1, _VIEW_NAME, person1)),
                        (("P1", "US_OZ_EG"), (ts2, _VIEW_NAME, person2)),
                    ]
                ),
            )

    def test_conflicting_flat_fields_raises_by_default(self) -> None:
        """Conflicting flat fields raise when should_throw_on_conflicts is True."""
        ts = datetime(2024, 1, 1).timestamp()
        person1 = _make_person("P1", current_email_address="a@example.com")
        person2 = _make_person("P1", current_email_address="b@example.com")

        p = create_test_pipeline()
        _ = (
            p
            | beam.Create([(ts, person1), (ts, person2)])
            | MergeIngestViewRootEntityTrees(_VIEW_NAME, entities_module=state_entities)
        )
        with self.assertRaisesRegex(RuntimeError, r".*EntityMergingError.*"):
            p.run()

    def test_conflicting_flat_fields_tolerated_when_disabled(self) -> None:
        """Conflicting flat fields are tolerated when should_throw_on_conflicts is False."""
        ts = datetime(2024, 1, 1).timestamp()
        person1 = _make_person("P1", current_email_address="a@example.com")
        person2 = _make_person("P1", current_email_address="b@example.com")

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create([(ts, person1), (ts, person2)])
                | MergeIngestViewRootEntityTrees(
                    _VIEW_NAME,
                    entities_module=state_entities,
                    should_throw_on_conflicts=False,
                )
            )
            # Should produce a result without raising
            assert_that(
                result,
                equal_to([(("P1", "US_OZ_EG"), (ts, _VIEW_NAME, person2))]),
            )
