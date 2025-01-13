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

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import (
    group_has_external_id_entities_by_function,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSentence,
    NormalizedStateSentenceInferredGroup,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)


# TODO(#36078) Make imposed sentence groups
def get_normalized_inferred_sentence_groups(
    state_code: StateCode,
    delegate: StateSpecificSentenceNormalizationDelegate,
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
