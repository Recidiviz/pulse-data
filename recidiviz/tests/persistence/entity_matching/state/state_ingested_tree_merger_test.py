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
"""Tests for the StateIngestedTreeMerger class."""
import unittest
from copy import deepcopy
from itertools import permutations
from typing import Any

import attr
from more_itertools import one

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import Race
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateCharge,
    StateCourtCase,
    StateIncarcerationIncident,
    StateIncarcerationIncidentOutcome,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonExternalId,
    StatePersonRace,
)
from recidiviz.persistence.entity_matching.state.state_ingested_tree_merger import (
    StateIngestedTreeMerger,
)

_STATE_CODE = StateCode.US_XX.value


def make_person(**kwargs: Any) -> StatePerson:
    return StatePerson.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_person_external_id(**kwargs: Any) -> StatePersonExternalId:
    return StatePersonExternalId.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_person_race(**kwargs: Any) -> StatePersonRace:
    return StatePersonRace.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_incarceration_incident(**kwargs: Any) -> StateIncarcerationIncident:
    return StateIncarcerationIncident.new_with_defaults(
        state_code=_STATE_CODE, **kwargs
    )


def make_incarceration_sentence(**kwargs: Any) -> StateIncarcerationSentence:
    return StateIncarcerationSentence.new_with_defaults(
        state_code=_STATE_CODE,
        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        **kwargs,
    )


def make_state_charge(**kwargs: Any) -> StateCharge:
    return StateCharge.new_with_defaults(
        state_code=_STATE_CODE, status=ChargeStatus.PRESENT_WITHOUT_INFO, **kwargs
    )


def make_court_case(**kwargs: Any) -> StateCourtCase:
    return StateCourtCase.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_agent(**kwargs: Any) -> StateAgent:
    return StateAgent.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_incarceration_incident_outcome(
    **kwargs: Any,
) -> StateIncarcerationIncidentOutcome:
    return StateIncarcerationIncidentOutcome.new_with_defaults(
        state_code=_STATE_CODE, **kwargs
    )


class TestStateIngestedTreeMerger(unittest.TestCase):
    """Tests for the StateIngestedTreeMerger class."""

    def setUp(self) -> None:
        self.field_index = CoreEntityFieldIndex()
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

        tree_merger = StateIngestedTreeMerger(field_index=self.field_index)

        merge_result = tree_merger.merge(ingested_persons)

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

        tree_merger = StateIngestedTreeMerger(field_index=self.field_index)

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual([expected_person], merge_result)

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

        tree_merger = StateIngestedTreeMerger(field_index=self.field_index)

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual(expected_people, merge_result)

    def test_merge_placeholder_with_different_children(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    # Placeholder incident
                    make_incarceration_incident(
                        incarceration_incident_outcomes=[
                            make_incarceration_incident_outcome(external_id="ID_1")
                        ]
                    )
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    # Placeholder incident
                    make_incarceration_incident(
                        incarceration_incident_outcomes=[
                            make_incarceration_incident_outcome(external_id="ID_2")
                        ]
                    )
                ],
            ),
        ]
        expected_people = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    # Placeholder incidents merged
                    make_incarceration_incident(
                        incarceration_incident_outcomes=[
                            make_incarceration_incident_outcome(external_id="ID_1"),
                            make_incarceration_incident_outcome(external_id="ID_2"),
                        ]
                    ),
                ],
            ),
        ]

        tree_merger = StateIngestedTreeMerger(field_index=self.field_index)

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual(expected_people, merge_result)

    def test_merge_placeholder_with_matching_children(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    # Placeholder incident
                    make_incarceration_incident(
                        incarceration_incident_outcomes=[
                            make_incarceration_incident_outcome(external_id="ID_1")
                        ]
                    )
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    # Placeholder incident
                    make_incarceration_incident(
                        incarceration_incident_outcomes=[
                            make_incarceration_incident_outcome(external_id="ID_1")
                        ]
                    )
                ],
            ),
        ]
        expected_people = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_incidents=[
                    # Placeholder incident
                    make_incarceration_incident(
                        incarceration_incident_outcomes=[
                            make_incarceration_incident_outcome(external_id="ID_1")
                        ]
                    )
                ],
            )
        ]

        tree_merger = StateIngestedTreeMerger(field_index=self.field_index)

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual(expected_people, merge_result)

    def test_merge_people_with_demographic_info(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                races=[
                    make_person_race(race=Race.BLACK),
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
                    make_person_race(race=Race.BLACK),
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
                    make_person_race(race=Race.BLACK),
                ],
                incarceration_incidents=[
                    make_incarceration_incident(external_id="ID_1"),
                    make_incarceration_incident(external_id="ID_2"),
                ],
            ),
        ]

        tree_merger = StateIngestedTreeMerger(field_index=self.field_index)

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual(expected_people, merge_result)

    def test_merge_placeholder_chain_with_one_to_one_relationships(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_sentences=[
                    make_incarceration_sentence(
                        external_id="ID_1",
                        charges=[
                            # Placeholder charge
                            make_state_charge(
                                # Placeholder court case
                                court_case=make_court_case(
                                    judge=make_agent(
                                        external_id="ID_1",
                                        agent_type=StateAgentType.JUDGE,
                                    )
                                ),
                            )
                        ],
                    )
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_sentences=[
                    make_incarceration_sentence(
                        external_id="ID_1",
                        charges=[
                            # Placeholder charge
                            make_state_charge(
                                # Placeholder court case
                                court_case=make_court_case(
                                    judge=make_agent(
                                        external_id="ID_1",
                                        agent_type=StateAgentType.JUDGE,
                                    )
                                ),
                            )
                        ],
                    )
                ],
            ),
        ]
        expected_people = [deepcopy(ingested_persons[0])]

        tree_merger = StateIngestedTreeMerger(field_index=self.field_index)

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual(expected_people, merge_result)

    def test_merge_non_placeholder_children_without_external_ids(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_sentences=[
                    make_incarceration_sentence(
                        external_id="ID_1",
                        charges=[
                            # Placeholder charge
                            make_state_charge(
                                court_case=make_court_case(county_code="COUNTY"),
                            )
                        ],
                    )
                ],
            ),
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_sentences=[
                    make_incarceration_sentence(
                        external_id="ID_1",
                        charges=[
                            # Placeholder charge
                            make_state_charge(
                                court_case=make_court_case(county_code="COUNTY"),
                            )
                        ],
                    )
                ],
            ),
        ]
        expected_people = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
                incarceration_sentences=[
                    make_incarceration_sentence(
                        external_id="ID_1",
                        charges=[
                            # Placeholder charge
                            make_state_charge(
                                court_case=make_court_case(county_code="COUNTY"),
                            )
                        ],
                    )
                ],
            ),
        ]

        tree_merger = StateIngestedTreeMerger(field_index=self.field_index)

        merge_result = tree_merger.merge(ingested_persons)

        self.assertCountEqual(expected_people, merge_result)


class TestBucketIngestedPersons(unittest.TestCase):
    """Tests for bucket_ingested_persons() in StateIngestedTreeMerger."""

    def test_bucket_single_ingested_person(self) -> None:
        ingested_persons = [
            make_person(
                external_ids=[
                    make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
                ],
            )
        ]
        buckets = StateIngestedTreeMerger.bucket_ingested_persons(ingested_persons)
        self.assertCountEqual(buckets, [ingested_persons])

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
        buckets = StateIngestedTreeMerger.bucket_ingested_persons(ingested_persons)
        self.assertCountEqual(buckets, [[ingested_persons[0]], [ingested_persons[1]]])

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
        buckets = StateIngestedTreeMerger.bucket_ingested_persons(ingested_persons)
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
        buckets = StateIngestedTreeMerger.bucket_ingested_persons(ingested_persons)
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
            buckets = StateIngestedTreeMerger.bucket_ingested_persons(
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
            buckets = StateIngestedTreeMerger.bucket_ingested_persons(
                list(ingested_persons_permutation)
            )

            self.assertCountEqual(one(buckets), ingested_persons)

    def test_bucket_two_people_one_placeholder(self) -> None:
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
            "Ingested StatePerson objects must have one or more assigned external ids.",
        ):
            _ = StateIngestedTreeMerger.bucket_ingested_persons(ingested_persons)

    def test_bucket_two_placeholder_people(self) -> None:
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
            "Ingested StatePerson objects must have one or more assigned external ids.",
        ):
            _ = StateIngestedTreeMerger.bucket_ingested_persons(ingested_persons)

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
            buckets = StateIngestedTreeMerger.bucket_ingested_persons(
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
