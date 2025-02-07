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

from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import (
    group_has_external_id_entities_by_function,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateChargeV2,
    NormalizedStateSentence,
    NormalizedStateSentenceImposedGroup,
    NormalizedStateSentenceInferredGroup,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.utils.types import assert_type


def build_imposed_group_from_sentences(
    state_code: StateCode,
    delegate: StateSpecificSentenceNormalizationDelegate,
    sentences: list[NormalizedStateSentence],
) -> NormalizedStateSentenceImposedGroup:
    """
    Builds an imposed group from provided sentences.

    Because state-specific logic in normalization may produce a group with
    more than one imposed_date or serving_start_date (e.g. sharing a common charge),
    we chose the minimum of each.
    """
    ids = NormalizedStateSentenceImposedGroup.external_id_delimiter().join(
        sorted(s.external_id for s in sentences)
    )

    sentencing_authority = one({s.sentencing_authority for s in sentences})

    # Out of state sentences may not have an imposed_date
    try:
        imposed_date = min(
            assert_type(s.imposed_date, datetime.date) for s in sentences
        )
    except ValueError:
        imposed_date = None

    serving_starts = []
    for sentence in sentences:
        serving = sentence.first_serving_status_to_terminating_status_dt_range
        if serving:
            serving_starts.append(serving.lower_bound_inclusive.date())
    serving_start_date = min(serving_starts) if serving_starts else None

    most_severe_charge: NormalizedStateChargeV2 = delegate.get_most_severe_charge(
        sentences
    )

    return NormalizedStateSentenceImposedGroup(
        state_code=state_code.value,
        external_id=ids,
        imposed_date=imposed_date,
        sentencing_authority=sentencing_authority,
        serving_start_date=serving_start_date,
        most_severe_charge_v2_id=most_severe_charge.charge_v2_id,
    )


def get_normalized_imposed_sentence_groups(
    state_code: StateCode,
    delegate: StateSpecificSentenceNormalizationDelegate,
    normalized_sentences: list[NormalizedStateSentence],
) -> list[NormalizedStateSentenceImposedGroup]:
    """
    Creates NormalizedStateSentenceImposedGroup entities.
    Any NormalizedStateSentence associated with a NormalizedStateSentenceImposedGroup
    will receive a sentence_group_imposed_id.
    By default, a NormalizedStateSentenceImposedGroup is created when two sentences
    have the same imposed_date. Check the normalization delegate for any state specific
    grouping conditions.
    """
    sentence_map = {s.external_id: s for s in normalized_sentences}
    grouped_ids = group_has_external_id_entities_by_function(
        normalized_sentences, delegate.sentences_are_in_same_imposed_group
    )
    imposed_groups = []
    for ids in grouped_ids:
        # We pop so this breaks if a sentence appears twice.
        sentences = [sentence_map.pop(external_id) for external_id in ids]
        if sentences:
            imposed_groups.append(
                build_imposed_group_from_sentences(state_code, delegate, sentences)
            )
    return imposed_groups


def get_normalized_inferred_sentence_groups(
    state_code: StateCode,
    delegate: StateSpecificSentenceNormalizationDelegate,
    normalized_sentences: list[NormalizedStateSentence],
) -> list[NormalizedStateSentenceInferredGroup]:
    """
    Creates NormalizedStateSentenceInferredGroup entities.
    Any NormalizedStateSentenceGroup and NormalizedStateSentence
    associated with a NormalizedStateSentenceGroupInferred will
    receive a sentence_group_inferred_id.
    A NormalizedStateSentenceGroupInferred is created when two sentences:
        - Have the same NormalizedStateSentenceGroup
        - Have the same imposed_date
        - Have a common charge
        - Have charges with a common offense_date
        - Have an overlapping span of time between the first SERVING
          status and terminating status.
    A sentence can only belong to a single inferred group. We infer
    groups from sentences becuase not all states have sentence groups.
    """
    grouped_ids = group_has_external_id_entities_by_function(
        normalized_sentences, delegate.sentences_are_in_same_inferred_group
    )
    return [
        NormalizedStateSentenceInferredGroup.from_sentence_external_ids(state_code, ids)
        for ids in grouped_ids
    ]
