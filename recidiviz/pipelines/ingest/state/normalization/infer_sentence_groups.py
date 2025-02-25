# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
Creates the NormalizedSentenceInferredGroup entity
from NormalizedStateSentence entities, and updates the
sentence_inferred_group_id on related NormalizedStateSentence
and NormalizedStateSentenceGroup entities.
"""

import datetime

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import PotentiallyOpenDateTimeRange
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSentence,
    NormalizedStateSentenceInferredGroup,
)


@attr.define
class InferredGroupBuilder:
    """
    This helper class encapsulates the required data to group sentences into
    inferred sentence groups.

    An inferred group is created when two sentences:
        - Have the same NormalizedStateSentenceGroup
        - Have the same imposed_date
        - Have an overlapping span of time between the first SERVING
          status and terminating status.
    A sentence can only belong to a single inferred group. We infer
    groups from sentences becuase not all states have sentence groups.
    """

    state_code: StateCode
    sentences: list[NormalizedStateSentence] = attr.ib(factory=list)

    # Sentences sharing a NormalizedSentenceGroup external_id are in the same inferred group.
    sg_external_ids: set[str] = attr.ib(factory=set)

    # The imposed_date values of all the |sentences| stored above.
    # Sentences sharing an imposed_date are in the same inferred group.
    imposed_dates: set[datetime.date] = attr.ib(factory=set)

    # Sentences that have an overlapping span of time from the first SERVING status
    # to the terminating status are in the same inferred group.
    dt_spans: set[PotentiallyOpenDateTimeRange] = attr.ib(factory=set)

    def should_add_sentence(self, sentence: NormalizedStateSentence) -> bool:
        """Returns True if the given sentence should be added to this group."""
        if sentence.sentence_group_external_id in self.sg_external_ids:
            return True
        if sentence.imposed_date in self.imposed_dates:
            return True
        # status_span is None if a state hasn't hydrated status snapshots
        # This sentence should be in the group if its first status is
        # within the span of other sentences in the group
        if status_span := sentence.first_serving_status_to_terminating_status_dt_range:
            for span in self.dt_spans:
                if status_span.lower_bound_inclusive in span:
                    return True
                # Statuses that get entered together often end up starting/ending at the
                # same time. Our span class is exclusive, so we check if the start/end lines up.
                if (
                    span.upper_bound_exclusive is not None
                    and status_span.lower_bound_inclusive == span.upper_bound_exclusive
                ):
                    return True

        return False

    def add_sentence_to_group(self, sentence: NormalizedStateSentence) -> None:
        """Adds the given sentence to this group."""
        if sentence.sentence_group_external_id:
            self.sg_external_ids.add(sentence.sentence_group_external_id)
        if sentence.imposed_date:
            self.imposed_dates.add(sentence.imposed_date)
        # status_span is None if a state hasn't hydrated status snapshots
        # or there is no SERVING status
        if status_span := sentence.first_serving_status_to_terminating_status_dt_range:
            self.dt_spans.add(status_span)
        self.sentences.append(sentence)

    @classmethod
    def new_group_builder_from_sentence(
        cls, sentence: NormalizedStateSentence
    ) -> "InferredGroupBuilder":
        builder = InferredGroupBuilder(state_code=StateCode(sentence.state_code))
        builder.add_sentence_to_group(sentence)
        return builder

    def build(self) -> NormalizedStateSentenceInferredGroup:
        """
        Builds a NormalizedStateSentenceInferredGroup from this
        instance's sentences. We build inferred groups from sentences because:
          - not all states necessarily have a StateSentenceGroup
          - all hydrated NormalizedStateSentenceGroup entities must have an
            associated NormalizedStateSentence entity
        """
        delimiter = NormalizedStateSentenceInferredGroup.external_id_delimiter()
        return NormalizedStateSentenceInferredGroup(
            state_code=self.state_code.value,
            external_id=delimiter.join(sorted(s.external_id for s in self.sentences)),
        )

    @classmethod
    def build_inferred_group_from_sentences(
        cls, sentences: list[NormalizedStateSentence]
    ) -> NormalizedStateSentenceInferredGroup:
        """Builds a NormalizedStateSentenceInferredGroup from the given sentences (helpful for tests)"""
        builder = InferredGroupBuilder(StateCode(sentences[0].state_code))
        for sentence in sentences:
            builder.add_sentence_to_group(sentence)
        return builder.build()


def get_normalized_inferred_sentence_groups(
    normalized_sentences: list[NormalizedStateSentence],
) -> list[NormalizedStateSentenceInferredGroup]:
    """
    Creates NormalizedStateSentenceGroupInferred entities.
    Any NormalizedStateSentenceGroup and NormalizedStateSentence
    associated with a NormalizedStateSentenceGroupInferred will
    receive a sentence_group_inferred_id.
    A NormalizedStateSentenceGroupInferred is created when two sentences:
        - Have the same NormalizedStateSentenceGroup
        - Have the same imposed_date
        - Have an overlapping span of time between the first SERVING
          status and terminating status.
    A sentence can only belong to a single inferred group. We infer
    groups from sentences becuase not all states have sentence groups.
    """
    groupings: list[InferredGroupBuilder] = []
    for sentence in normalized_sentences:
        found_group = False
        for group in groupings:
            if found_group := group.should_add_sentence(sentence):
                group.add_sentence_to_group(sentence)
                break
        if not found_group:
            groupings.append(
                InferredGroupBuilder.new_group_builder_from_sentence(sentence)
            )
    return [group.build() for group in groupings]
