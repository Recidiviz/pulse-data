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
"""Utils for hydrating connections between entities."""
from collections import defaultdict
from typing import Dict, Iterable, List, Tuple, Union, cast

import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types
from more_itertools import one

from recidiviz.calculator.pipeline.utils.beam_utils.extractor_utils import (
    EntityClassName,
    TableName,
    TableRow,
    UnifyingId,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    UsMoIncarcerationSentence,
    UsMoSupervisionSentence,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_NAME,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities


@with_input_types(
    beam.typehints.Tuple[
        UnifyingId,
        Dict[Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]],
    ],
)
@with_output_types(
    beam.typehints.Tuple[
        UnifyingId,
        Dict[Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]],
    ],
)
class ConvertEntitiesToStateSpecificTypes(beam.DoFn):
    """Converts entities into state-specific subclasses of those entities,
    for use in state-specific calculate flows."""

    def process(
        self,
        element: Tuple[
            UnifyingId,
            Dict[
                Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
            ],
        ],
        *_args,
        **_kwargs,
    ) -> Iterable[
        Tuple[
            UnifyingId,
            Dict[
                Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
            ],
        ]
    ]:
        """For the entities of the given person, convert to a state-specific subclass,
        if necessary.

        Args:
            element: A tuple containing person_id and a dictionary with all of the
                person's entities and sentence statuses (if applicable)

        Yields:
            A replica of the provided element, with some entities replaced by updated
            entities, where applicable.
        """
        person_id, entities_and_reference_tables = element

        person = one(list(entities_and_reference_tables[entities.StatePerson.__name__]))
        person = cast(entities.StatePerson, person)

        if person.state_code == StateCode.US_MO.value:
            self.update_US_MO_sentences(entities_and_reference_tables)
        yield person_id, entities_and_reference_tables

    def update_US_MO_sentences(
        self,
        entities_and_reference_tables: Dict[
            Union[EntityClassName, TableName], Union[List[Entity], List[TableRow]]
        ],
    ) -> None:
        """Updates US_MO StateIncarcerationSentence and StateSupervisionSentence
        entities to state-specific versions of the classes that store additional
        information required to perform calculations in this state."""
        incarceration_sentences: List[Entity] = []
        supervision_sentences: List[Entity] = []

        if (
            entities.StateIncarcerationSentence.__name__
            in entities_and_reference_tables
        ):
            incarceration_sentences = cast(
                List[Entity],
                entities_and_reference_tables[
                    entities.StateIncarcerationSentence.__name__
                ],
            )

        if entities.StateSupervisionSentence.__name__ in entities_and_reference_tables:
            supervision_sentences = cast(
                List[Entity],
                entities_and_reference_tables[
                    entities.StateSupervisionSentence.__name__
                ],
            )

        if not supervision_sentences and not incarceration_sentences:
            return

        if US_MO_SENTENCE_STATUSES_VIEW_NAME not in entities_and_reference_tables:
            raise ValueError(
                f"Must hydrate [{US_MO_SENTENCE_STATUSES_VIEW_NAME}] to "
                f"use sentences in a US_MO pipeline."
            )
        all_sentence_statuses: Union[
            List[Entity], List[TableRow]
        ] = entities_and_reference_tables.pop(US_MO_SENTENCE_STATUSES_VIEW_NAME)

        us_mo_sentence_statuses_by_sentence: Dict[
            str, List[Dict[str, str]]
        ] = defaultdict(list)

        if all_sentence_statuses:
            # Build a dictionary that maps each sentence_external_id to a list of
            # dictionaries containing status updates for this sentence
            for status_dict in all_sentence_statuses:
                if not isinstance(status_dict, Dict):
                    raise ValueError(
                        "Expected sentence status element to be of type "
                        f"Dict. Found {type(status_dict)}."
                    )

                sentence_external_id = status_dict.get("sentence_external_id")

                if sentence_external_id:
                    us_mo_sentence_statuses_by_sentence[sentence_external_id].append(
                        status_dict
                    )

        updated_incarceration_sentences: List[UsMoIncarcerationSentence] = []

        for incarceration_sentence in incarceration_sentences:
            if not isinstance(
                incarceration_sentence, entities.StateIncarcerationSentence
            ):
                raise ValueError(
                    "Expected entity to be of type "
                    f"StateIncarcerationSentence. Found {type(incarceration_sentence)}."
                )

            if incarceration_sentence.state_code != StateCode.US_MO.value:
                raise ValueError(
                    f"Found sentence that isn't of US_MO: {incarceration_sentence}"
                )

            sentence_statuses = []
            if (
                incarceration_sentence.external_id
                in us_mo_sentence_statuses_by_sentence
            ):
                sentence_statuses = us_mo_sentence_statuses_by_sentence[
                    incarceration_sentence.external_id
                ]

            state_specific_incarceration_sentence = (
                UsMoIncarcerationSentence.from_incarceration_sentence(
                    incarceration_sentence, sentence_statuses
                )
            )

            updated_incarceration_sentences.append(
                state_specific_incarceration_sentence
            )

        entities_and_reference_tables[
            entities.StateIncarcerationSentence.__name__
        ] = cast(List[Entity], updated_incarceration_sentences)

        updated_supervision_sentences: List[UsMoSupervisionSentence] = []

        for supervision_sentence in supervision_sentences:
            if not isinstance(supervision_sentence, entities.StateSupervisionSentence):
                raise ValueError(
                    "Expected entity to be of type "
                    f"StateSupervisionSentence. Found {type(supervision_sentence)}."
                )
            if supervision_sentence.state_code != StateCode.US_MO.value:
                raise ValueError(
                    f"Found sentence that isn't of US_MO: {supervision_sentence}"
                )
            sentence_statuses = []
            if supervision_sentence.external_id in us_mo_sentence_statuses_by_sentence:
                sentence_statuses = us_mo_sentence_statuses_by_sentence[
                    supervision_sentence.external_id
                ]

            state_specific_supervision_sentence = (
                UsMoSupervisionSentence.from_supervision_sentence(
                    supervision_sentence, sentence_statuses
                )
            )

            updated_supervision_sentences.append(state_specific_supervision_sentence)

        entities_and_reference_tables[
            entities.StateSupervisionSentence.__name__
        ] = cast(List[Entity], updated_supervision_sentences)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.
