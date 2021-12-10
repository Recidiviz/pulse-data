# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Provides tools for traversing the hierarchy of ingest objects."""

from typing import Dict, Sequence, Set, Union

import attr
from more_itertools import one

AncestorClassName = str
AncestorChoiceKey = str


@attr.s
class AncestorTypeChoices:
    key: "AncestorChoiceKey" = attr.ib()
    ancestor_choices: Set["AncestorClassName"] = attr.ib(factory=list)


VALID_ANCESTOR_CHOICE_KEYS: Set["AncestorChoiceKey"] = {"state_sentence"}


_HIERARCHY_MAP: Dict[
    "AncestorClassName", Sequence[Union["AncestorClassName", "AncestorTypeChoices"]]
] = {
    "person": (),
    "booking": ("person",),
    "arrest": ("person", "booking"),
    "charge": ("person", "booking"),
    "hold": ("person", "booking"),
    "bond": ("person", "booking", "charge"),
    "sentence": ("person", "booking", "charge"),
    # TODO(#8905): Delete all state schema objects from this map once ingest mappings
    #  overhaul is complete for all states.
    "state_person": (),
    "state_person_race": ("state_person",),
    "state_person_ethnicity": ("state_person",),
    "state_alias": ("state_person",),
    "state_person_external_id": ("state_person",),
    "state_assessment": ("state_person",),
    "state_incarceration_period": ("state_person",),
    "state_supervision_period": ("state_person",),
    "state_supervision_case_type_entry": (
        "state_person",
        "state_supervision_period",
    ),
    "state_incarceration_incident": ("state_person",),
    "state_incarceration_incident_outcome": (
        "state_person",
        "state_incarceration_incident",
    ),
    "state_supervision_violation": ("state_person",),
    "state_supervision_violation_type_entry": (
        "state_person",
        "state_supervision_violation",
    ),
    "state_supervision_violation_response": (
        "state_person",
        "state_supervision_violation",
    ),
    "state_supervision_violation_response_decision_entry": (
        "state_person",
        "state_supervision_violation",
        "state_supervision_violation_response",
    ),
    "state_supervision_contact": ("state_person",),
    "state_sentence_group": ("state_person",),
    "state_program_assignment": ("state_person",),
    "state_supervision_sentence": (
        "state_person",
        "state_sentence_group",
    ),
    "state_incarceration_sentence": (
        "state_person",
        "state_sentence_group",
    ),
    "state_early_discharge": (
        "state_person",
        "state_sentence_group",
        AncestorTypeChoices(
            key="state_sentence",
            ancestor_choices={
                "state_incarceration_sentence",
                "state_supervision_sentence",
            },
        ),
    ),
    "state_charge": (
        "state_person",
        "state_sentence_group",
        AncestorTypeChoices(
            key="state_sentence",
            ancestor_choices={
                "state_incarceration_sentence",
                "state_supervision_sentence",
                "fine",
            },
        ),
    ),
    "state_court_case": (
        "state_person",
        "state_sentence_group",
        AncestorTypeChoices(
            key="state_sentence",
            ancestor_choices={
                "state_incarceration_sentence",
                "state_supervision_sentence",
                "fine",
            },
        ),
        "state_charge",
    ),
    # NOTE: The entry here for |state_agent| is a hack. StateAgent
    #  can have multiple ancestor paths depending on what type of agent
    #  it is. We need to update the extractor code to just generate
    #  buckets of objects of a given type, with some encoding about
    #  parent relationships. However, this is fine for the ND MVP, since
    #  we only create one type of StateAgent right now.
    "state_agent": (
        "state_person",
        "state_sentence_group",
        AncestorTypeChoices(
            key="state_sentence",
            ancestor_choices={
                "state_incarceration_sentence",
                "state_supervision_sentence",
                "fine",
            },
        ),
        "state_charge",
        "state_court_case",
    ),
}


def get_ancestor_class_sequence(
    class_name: "AncestorClassName",
    ancestor_chain: Dict["AncestorClassName", str] = None,
    enforced_ancestor_choices: Dict["AncestorChoiceKey", "AncestorClassName"] = None,
) -> Sequence["AncestorClassName"]:
    """Returns the sequence of ancestor classes leading from the root of the
    object graph, a Person type, all the way to the given |class_name|.

    This handles graphs with multiple inbound edges to a particular type. For
    example, StateCharge can be a child of IncarcerationSentence,
    SupervisionSentence, and Fine. To determine exactly which sequence to
    return, context is derived from the other parameters.

    |ancestor_chain| is the set of ids of known ancestor instances for which we
    are trying to find the ancestor sequence. If |class_name| is state_charge
    and the |ancestor_chain| is a dict containing a key of
    'state_incarceration_sentence', then the returned sequence will be
    ('state_person', 'state_sentence_group', 'state_incarceration_sentence').

    |enforced_ancestor_choices| is mapping of ancestor classes to specifically
    choose where there are multiple options. If |class_name| is state_charge and
    |enforced_ancestor_choices| is
    {'state_sentence': 'state_supervision_sentence'}, then the returned sequence
    will be
    ('state_person', 'state_sentence_group', 'state_supervision_sentence').

    If there is a valid key in the |ancestor_chain| and there are also
    |enforced_ancestor_choices|, then the key in |ancestor_chain| wins.

    Args:
        class_name: The name of the class to find the sequence for
        ancestor_chain: A dictionary with keys of ancestor class types and
            values of specific ancestor instance ids
        enforced_ancestor_choices: A dictionary with keys of kinds of ancestor
            choices, e.g. `state_sentence` for any of the state sentence types,
            and values of the specific choice for that kind of choice, e.g.
            `state_incarceration_sentence`
    """
    if not ancestor_chain:
        ancestor_chain = {}

    if not enforced_ancestor_choices:
        enforced_ancestor_choices = {}

    hierarchy_sequence = []
    for step in _HIERARCHY_MAP[class_name]:
        if isinstance(step, str):
            hierarchy_sequence.append(step)
        elif isinstance(step, AncestorTypeChoices):
            if step.key not in VALID_ANCESTOR_CHOICE_KEYS:
                raise ValueError(
                    f"Invalid ancestor choice key of [{step.key}], must be one "
                    f"of [{VALID_ANCESTOR_CHOICE_KEYS}]"
                )

            # First, pick if we've selected one of the choices via the
            # ancestor chain
            choices_ancestor_chain_overlap = step.ancestor_choices.intersection(
                ancestor_chain.keys()
            )
            if choices_ancestor_chain_overlap:
                if len(choices_ancestor_chain_overlap) > 1:
                    raise ValueError(
                        "There are multiple valid ancestor choices in the "
                        "given ancestor chain. Valid choices are: "
                        f"[{step.ancestor_choices}]. Ancestor chain includes: "
                        f"[{ancestor_chain.keys()}]"
                    )
                hierarchy_sequence.append(one(choices_ancestor_chain_overlap))
                continue

            # Next, check the enforced_ancestor_choices
            if not enforced_ancestor_choices:
                raise ValueError(
                    "For possible ancestor choices of "
                    f"[{step.ancestor_choices}], there is neither overlap with "
                    f"the ancestor chain [{ancestor_chain.keys()}] nor a "
                    f"declared choice. We don't have enough information to "
                    f"construct the ancestor hierarchy for this object."
                )

            if step.key not in enforced_ancestor_choices:
                raise ValueError(
                    f"The enforced choices [{enforced_ancestor_choices}] don't "
                    f"contain a mapping for [{step.key}]. We don't have enough "
                    "information to construct the ancestor hierarchy for this "
                    "object."
                )

            choice = enforced_ancestor_choices[step.key]
            if choice not in _HIERARCHY_MAP:
                raise ValueError(
                    f"Invalid ancestor choice value of [{choice}], must be a "
                    "valid type listed in the hierarchy map."
                )
            hierarchy_sequence.append(choice)
        else:
            raise ValueError(f"Unknown type [{type(step)}] in hierarchy map.")
    return tuple(hierarchy_sequence)
