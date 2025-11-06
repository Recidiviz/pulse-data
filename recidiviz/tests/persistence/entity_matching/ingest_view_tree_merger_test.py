# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for the IngestViewTreeMerger class."""
import datetime
import unittest
from itertools import permutations
from typing import List, Union

import attr
from more_itertools import one

from recidiviz.common.constants.state.state_person import StateRace
from recidiviz.common.constants.state.state_staff_role_period import StateStaffRoleType
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.persistence.entity.state.entities import StatePerson, StateStaff
from recidiviz.persistence.entity_matching.ingest_view_tree_merger import (
    EntityMergingError,
    IngestViewTreeMerger,
)
from recidiviz.tests.persistence.entity_matching.us_xx_entity_builders import (
    make_incarceration_incident,
    make_person,
    make_person_external_id,
    make_person_race,
    make_staff,
    make_staff_external_id,
    make_staff_role_period,
    make_task_deadline,
)


class TestIngestViewTreeMerger(unittest.TestCase):
    """Tests for the IngestViewTreeMerger class."""

    def setUp(self) -> None:
        self.maxDiff = None

    def test_merge_people_exact_match(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
        ]
        expected_person = attr.evolve(ingested_persons[0])

        tree_merger = IngestViewTreeMerger()

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual([expected_person], merge_result)

    def test_merge_staff_exact_match(self) -> None:
        ingested_staff = [
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
        ]
        expected_person = attr.evolve(ingested_staff[0])

        tree_merger = IngestViewTreeMerger()

        merge_result = tree_merger.merge(ingested_staff)

        self.assertCountEqual([expected_person], merge_result)

    def test_merge_people_exact_match_with_child(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_1")
                ],
            ),
        ]
        expected_person = attr.evolve(ingested_persons[0])

        tree_merger = IngestViewTreeMerger()

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual([expected_person], merge_result)

    def test_merge_staff_exact_match_with_child(self) -> None:
        ingested_staff = [
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                role_periods=[
                    make_staff_role_period(
                        external_id="ID_1",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=datetime.date(2023, 1, 1),
                    )
                ],
            ),
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                role_periods=[
                    make_staff_role_period(
                        external_id="ID_1",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=datetime.date(2023, 1, 1),
                    )
                ],
            ),
        ]
        expected_staff = attr.evolve(ingested_staff[0])

        tree_merger = IngestViewTreeMerger()

        merge_result = tree_merger.merge(ingested_staff)

        self.assertCountEqual([expected_staff], merge_result)

    def test_merge_people_with_different_children(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_2")
                ],
            ),
        ]
        expected_people = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_1"),
                    make_incarceration_incident(external_id="ID_2"),
                ],
            ),
        ]

        tree_merger = IngestViewTreeMerger()

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual(expected_people, merge_result)

    def test_merge_staff_with_different_children(self) -> None:
        ingested_persons = [
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                role_periods=[
                    make_staff_role_period(
                        external_id="ID_1",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=datetime.date(2023, 1, 1),
                    )
                ],
            ),
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                role_periods=[
                    make_staff_role_period(
                        external_id="ID_2",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=datetime.date(2023, 1, 1),
                    )
                ],
            ),
        ]
        expected_staff = [
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                role_periods=[
                    make_staff_role_period(
                        external_id="ID_1",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=datetime.date(2023, 1, 1),
                    ),
                    make_staff_role_period(
                        external_id="ID_2",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=datetime.date(2023, 1, 1),
                    ),
                ],
            ),
        ]

        tree_merger = IngestViewTreeMerger()

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual(expected_staff, merge_result)

    def test_merge_people_with_demographic_info(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                races=[
                    make_person_race(race=StateRace.BLACK),
                ],
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                races=[
                    make_person_race(race=StateRace.BLACK),
                ],
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_2")
                ],
            ),
        ]
        expected_people = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                races=[
                    make_person_race(race=StateRace.BLACK),
                ],
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_1"),
                    make_incarceration_incident(external_id="ID_2"),
                ],
            ),
        ]

        tree_merger = IngestViewTreeMerger()

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual(expected_people, merge_result)

    def test_merge_people_with_task_deadline_children(self) -> None:
        update_datetime = datetime.datetime(2000, 1, 2, 3, 4, 5, 6)
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                task_deadlines=[
                    make_task_deadline(
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        update_datetime=update_datetime,
                        task_metadata="metadata1",
                    )
                ],
            ),
            # This tree has exactly the same task deadline
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                task_deadlines=[
                    make_task_deadline(
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        update_datetime=update_datetime,
                        task_metadata="metadata1",
                    )
                ],
            ),
            # This tree's task deadline has different metadata
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                task_deadlines=[
                    make_task_deadline(
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        update_datetime=update_datetime,
                        task_metadata="metadata2",
                    )
                ],
            ),
        ]
        expected_people = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                task_deadlines=[
                    # No duplicate
                    make_task_deadline(
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        update_datetime=update_datetime,
                        task_metadata="metadata1",
                    ),
                    make_task_deadline(
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        update_datetime=update_datetime,
                        task_metadata="metadata2",
                    ),
                ],
            ),
        ]

        tree_merger = IngestViewTreeMerger()

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual(expected_people, merge_result)

    def test_entity_merging_error_shows_conflicting_fields(self) -> None:
        """Test that EntityMergingError shows which fields have conflicts."""
        # Create two staff entities with the same external_id but different full_name
        ingested_staff = [
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                full_name="John Doe",
            ),
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                full_name="Jane Doe",
            ),
        ]

        tree_merger = IngestViewTreeMerger()

        with self.assertRaises(EntityMergingError) as context:
            tree_merger.merge(ingested_staff)

        error_message = str(context.exception)

        expected_error_message = """Found multiple different ingested entities of type [StateStaff]
with conflicting information in fields: full_name

Entities with conflicts:
  Entity 1: StateStaff(staff_id=None, external_ids=[StateStaffExternalId(external_id='ID_1', id_type='ID_TYPE_1', staff_external_id_id=None)])
  Entity 2: StateStaff(staff_id=None, external_ids=[StateStaffExternalId(external_id='ID_1', id_type='ID_TYPE_1', staff_external_id_id=None)])"""

        self.assertEqual(expected_error_message, error_message)


class TestBucketIngestedRootEntities(unittest.TestCase):
    """Tests for bucket_ingested_root_entities() in IngestViewTreeMerger."""

    def test_bucket_single_ingested_person(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            )
        ]
        buckets = IngestViewTreeMerger.bucket_ingested_root_entities(ingested_persons)
        self.assertCountEqual(buckets, [ingested_persons])

    def test_bucket_single_ingested_staff(self) -> None:
        ingested_staff = [
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            )
        ]
        buckets = IngestViewTreeMerger.bucket_ingested_root_entities(ingested_staff)
        self.assertCountEqual(buckets, [ingested_staff])

    def test_bucket_two_people_different_id_types(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_2")
                ],
            ),
        ]
        buckets = IngestViewTreeMerger.bucket_ingested_root_entities(ingested_persons)
        self.assertCountEqual(buckets, [[ingested_persons[0]], [ingested_persons[1]]])

    def test_bucket_two_staff_different_id_types(self) -> None:
        ingested_staff = [
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_2")
                ],
            ),
        ]
        buckets = IngestViewTreeMerger.bucket_ingested_root_entities(ingested_staff)
        self.assertCountEqual(buckets, [[ingested_staff[0]], [ingested_staff[1]]])

    def test_bucket_two_people_different_ids(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_2", id_type="ID_TYPE_1")
                ],
            ),
        ]
        buckets = IngestViewTreeMerger.bucket_ingested_root_entities(ingested_persons)
        self.assertCountEqual(buckets, [[ingested_persons[0]], [ingested_persons[1]]])

    def test_bucket_two_people_single_matching_id(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
        ]
        buckets = IngestViewTreeMerger.bucket_ingested_root_entities(ingested_persons)
        self.assertCountEqual(one(buckets), ingested_persons)

    def test_bucket_three_people_joined_by_one(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_2", id_type="ID_TYPE_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1"),
                    make_person_external_id(external_id="ID_2", id_type="ID_TYPE_1"),
                ],
            ),
        ]

        for ingested_persons_permutation in permutations(ingested_persons):
            buckets = IngestViewTreeMerger.bucket_ingested_root_entities(
                list(ingested_persons_permutation)
            )
            self.assertCountEqual(one(buckets), ingested_persons)

    def test_bucket_multiple_levels_of_indirection(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_2", id_type="ID_TYPE_1"),
                    make_person_external_id(external_id="ID_3", id_type="ID_TYPE_1"),
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1"),
                    make_person_external_id(external_id="ID_2", id_type="ID_TYPE_1"),
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_3", id_type="ID_TYPE_1")
                ],
            ),
        ]

        for ingested_persons_permutation in permutations(ingested_persons):
            buckets = IngestViewTreeMerger.bucket_ingested_root_entities(
                list(ingested_persons_permutation)
            )

            self.assertCountEqual(one(buckets), ingested_persons)

    def test_bucket_two_people_one_missing_external_id(self) -> None:
        ingested_persons = [
            make_person(
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
        ]
        with self.assertRaisesRegex(
            ValueError,
            "Ingested root entity objects must have one or more assigned external ids.",
        ):
            _ = IngestViewTreeMerger.bucket_ingested_root_entities(ingested_persons)

    def test_bucket_two_people_missing_external_ids(self) -> None:
        ingested_persons = [
            make_person(
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_1")
                ],
            ),
            make_person(
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_2")
                ],
            ),
        ]
        with self.assertRaisesRegex(
            ValueError,
            "Ingested root entity objects must have one or more assigned external ids.",
        ):
            _ = IngestViewTreeMerger.bucket_ingested_root_entities(ingested_persons)

    def test_two_buckets(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1"),
                    make_person_external_id(external_id="ID_2", id_type="ID_TYPE_1"),
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_3", id_type="ID_TYPE_1"),
                    make_person_external_id(external_id="ID_4", id_type="ID_TYPE_1"),
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_3", id_type="ID_TYPE_1")
                ],
            ),
        ]

        for ingested_persons_permutation in permutations(ingested_persons):
            buckets = IngestViewTreeMerger.bucket_ingested_root_entities(
                list(ingested_persons_permutation)
            )

            expected_buckets = [ingested_persons[0:2], ingested_persons[2:]]

            self.assertCountEqual(
                [
                    frozenset(
                        frozenset(e.external_id for e in p.external_ids) for p in b
                    )
                    for b in buckets
                ],
                [
                    frozenset(
                        frozenset(e.external_id for e in p.external_ids) for p in b
                    )
                    for b in expected_buckets
                ],
            )

    def test_bucket_two_people_different_entity_types(self) -> None:
        ingested_root_entities: List[Union[StateStaff, StatePerson]] = [
            make_staff(
                external_ids=[
                    make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            ),
        ]
        buckets = IngestViewTreeMerger.bucket_ingested_root_entities(  # type: ignore
            ingested_root_entities
        )
        self.assertCountEqual(
            [[ingested_root_entities[0]], [ingested_root_entities[1]]],
            buckets,
        )
