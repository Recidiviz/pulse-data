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
Houses logic for normalizing sentencing V2 entities:
  - StateSentence
  - StateSentenceLength
  - StateSentenceGroup
  - StateSentenceGroupLength
  - StateSentenceStatusSnapshot
  - StateChargeV2

It also produces ingerred sentence groups that only exist in normalized_state:
  - StateSentenceGroupInferred
  - StateSentenceGroupInferredLength
"""
import datetime
from typing import Dict, List

from more_itertools import first

from recidiviz.common.constants.state.state_sentence import (
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ncic import get_description
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import set_backedges
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.entities import (
    StateChargeV2,
    StatePerson,
    StateSentence,
    StateSentenceGroup,
    StateSentenceGroupLength,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateChargeV2,
    NormalizedStateSentence,
    NormalizedStateSentenceGroup,
    NormalizedStateSentenceGroupLength,
    NormalizedStateSentenceImposedGroup,
    NormalizedStateSentenceInferredGroup,
    NormalizedStateSentenceLength,
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.persistence.entity.state.state_entity_utils import (
    ConsecutiveSentenceGraph,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.pipelines.ingest.state.normalization.sentencing.infer_sentence_groups import (
    get_normalized_imposed_sentence_groups,
    get_normalized_inferred_sentence_groups,
)
from recidiviz.pipelines.ingest.state.normalization.sentencing.normalize_sentence_statuses import (
    normalize_sentence_status_snapshots,
)
from recidiviz.pipelines.ingest.state.normalization.utils import get_min_max_fields
from recidiviz.utils.types import assert_type


def normalize_charge_v2(charge: StateChargeV2) -> NormalizedStateChargeV2:
    """
    Default normalization for StateChargeV2.
    No fields are changed, but we do add "external" fields for
    use in downstream views.

    Note this does not produce references to the sentences or
    person associated with this charge.
    """
    return NormalizedStateChargeV2(
        external_id=charge.external_id,
        state_code=charge.state_code,
        charge_v2_id=assert_type(charge.charge_v2_id, int),
        status=charge.status,
        status_raw_text=charge.status_raw_text,
        offense_date=charge.offense_date,
        date_charged=charge.date_charged,
        county_code=charge.county_code,
        ncic_code=charge.ncic_code,
        statute=charge.statute,
        description=charge.description,
        attempted=charge.attempted,
        classification_type=charge.classification_type,
        classification_type_raw_text=charge.classification_type_raw_text,
        classification_subtype=charge.classification_subtype,
        offense_type=charge.offense_type,
        is_violent=charge.is_violent,
        is_sex_offense=charge.is_sex_offense,
        is_drug=charge.is_drug,
        counts=charge.counts,
        charge_notes=charge.charge_notes,
        is_controlling=charge.is_controlling,
        charging_entity=charge.charging_entity,
        judge_full_name=charge.judge_full_name,
        judge_external_id=charge.judge_external_id,
        judicial_district_code=charge.judicial_district_code,
        # We duplicate these fields to external
        ncic_code_external=charge.ncic_code,
        ncic_category_external=(
            get_description(charge.ncic_code) if charge.ncic_code else None
        ),
        description_external=charge.description,
        is_violent_external=charge.is_violent,
        is_drug_external=charge.is_drug,
        is_sex_offense_external=charge.is_sex_offense,
    )


def normalize_sentence_lengths(
    sentence: StateSentence,
    delegate: StateSpecificSentenceNormalizationDelegate,
) -> List[NormalizedStateSentenceLength]:
    """
    Normalizes StateSentenceLength, with two updates:
        - Updates sequence_num to be contiguous from 1 based on the partition_key,
          correcting it if external data is increasing, but not contiguous from 1
        - Updates sentence_length_days min/max and projected_completion_date min/max
          to actually be the min/max of the hydrated values, correcting it if external
          data is not consistent. Other date enforcement occurs at ingest in the
          __attrs_post_init__ of StateSentenceLength
    """
    normalized_lengths = []
    serving_status_datetimes = [
        snapshot.status_update_datetime
        for snapshot in sentence.sentence_status_snapshots
        if snapshot.status.is_considered_serving_status
    ]
    serving_start = (
        sorted(serving_status_datetimes)[0].date()
        if any(serving_status_datetimes)
        else None
    )
    for idx, length in enumerate(
        sorted(sentence.sentence_lengths, key=lambda s: s.partition_key)
    ):
        # If the sentence length projected min/max fields are not
        # consistent in "state" such that min is always < max, we ensure that
        # they are consistent in "normalized_state"
        # See the __attrs_post_init__ of StateSentenceLength to see what
        # projected field consitency we enforce at ingest.
        days_min, days_max = get_min_max_fields(
            length.sentence_length_days_min, length.sentence_length_days_max
        )
        # Projected completion dates are either provided through state provided data,
        # or calculated from sentence length days + serving start.
        # We do not allow a mixture. Note that if a sentence hasn't begun serving,
        # we cannot calculate a projected completion date.
        comp_date_min, comp_date_max = None, None
        if delegate.override_projected_completion_dates_using_sentence_length_days:
            if serving_start is not None:
                comp_date_min = (
                    serving_start + datetime.timedelta(days=days_min)
                    if days_min
                    else None
                )
                comp_date_max = (
                    serving_start + datetime.timedelta(days=days_max)
                    if days_max
                    else None
                )
        else:
            comp_date_min, comp_date_max = get_min_max_fields(
                length.projected_completion_date_min_external,
                length.projected_completion_date_max_external,
            )

        normalized_lengths.append(
            NormalizedStateSentenceLength(
                sequence_num=idx + 1,
                state_code=length.state_code,
                sentence_length_id=assert_type(length.sentence_length_id, int),
                length_update_datetime=length.length_update_datetime,
                good_time_days=length.good_time_days,
                earned_time_days=length.earned_time_days,
                projected_parole_release_date_external=length.projected_parole_release_date_external,
                sentence_length_days_min=days_min,
                sentence_length_days_max=days_max,
                projected_completion_date_min_external=comp_date_min,
                projected_completion_date_max_external=comp_date_max,
                parole_eligibility_date_external=length.parole_eligibility_date_external,
            )
        )
    return normalized_lengths


def _normalize_single_sentence(
    *,
    sentence: StateSentence,
    charge_id_to_normalized_charge_cache: Dict[int, NormalizedStateChargeV2],
    delegate: StateSpecificSentenceNormalizationDelegate,
    normalized_snapshots: list[NormalizedStateSentenceStatusSnapshot],
) -> NormalizedStateSentence:
    """Normalizes the sentencing V2 entities for a given StateSentence."""
    normalized_charges = []
    for charge in sentence.charges:
        charge_id = assert_type(charge.charge_v2_id, int)
        if charge_id not in charge_id_to_normalized_charge_cache:
            normalized_charge = normalize_charge_v2(charge)
            charge_id_to_normalized_charge_cache[charge_id] = normalized_charge
        normalized_charges.append(charge_id_to_normalized_charge_cache[charge_id])

    earliest_serving_status_date: datetime.date | None = None
    serving_snapshots = [
        snapshot
        for snapshot in normalized_snapshots
        if snapshot.status.is_considered_serving_status
    ]
    # We don't want states that haven't hydrated status snapshots
    # or sentences that completed before we ever ingested to crash
    if any(serving_snapshots):
        earliest_serving_snapshot = first(
            sorted(
                serving_snapshots,
                key=lambda snapshot: assert_type(snapshot.sequence_num, int),
            )
        )
        earliest_serving_status_date = (
            earliest_serving_snapshot.status_update_datetime.date()
        )
    if (
        sentence.current_state_provided_start_date
        and sentence.current_state_provided_start_date != earliest_serving_status_date
    ):
        raise ValueError(
            f"Sentence has a current_state_provided_start_date that does not align "
            "with the NormalizedStateSentenceStatusSnapshot values. The earliest serving status date "
            f"is {earliest_serving_status_date} and the current_state_provided_start_date is "
            f"{sentence.current_state_provided_start_date}. "
            f"Sentence: {sentence.limited_pii_repr()}. "
            f"Person: {assert_type(sentence.person, StatePerson).limited_pii_repr()}"
        )
    current_start_date = (
        sentence.current_state_provided_start_date or earliest_serving_status_date
    )

    return NormalizedStateSentence(
        sentence_id=assert_type(sentence.sentence_id, int),
        external_id=sentence.external_id,
        state_code=sentence.state_code,
        sentence_type=assert_type(sentence.sentence_type, StateSentenceType),
        sentence_type_raw_text=sentence.sentence_type_raw_text,
        sentencing_authority=assert_type(
            sentence.sentencing_authority, StateSentencingAuthority
        ),
        sentencing_authority_raw_text=sentence.sentencing_authority_raw_text,
        sentence_group_external_id=sentence.sentence_group_external_id,
        sentence_inferred_group_id=None,
        sentence_imposed_group_id=None,
        imposed_date=sentence.imposed_date,
        initial_time_served_days=sentence.initial_time_served_days,
        is_life=sentence.is_life,
        is_capital_punishment=sentence.is_capital_punishment,
        parole_possible=sentence.parole_possible,
        county_code=sentence.county_code,
        parent_sentence_external_id_array=sentence.parent_sentence_external_id_array,
        conditions=sentence.conditions,
        sentence_metadata=sentence.sentence_metadata,
        current_state_provided_start_date=sentence.current_state_provided_start_date,
        current_start_date=current_start_date,
        # Relationships
        charges=normalized_charges,
        sentence_lengths=normalize_sentence_lengths(sentence, delegate),
        sentence_status_snapshots=normalized_snapshots,
    )


def get_normalized_sentences_for_person(
    person: StatePerson,
    delegate: StateSpecificSentenceNormalizationDelegate,
) -> List[NormalizedStateSentence]:
    """
    Normalizes the list of sentences for a given person.
    It also normalizes all sentencing v2 entities, and maintains
    the many-to-many relationship for sentences and charges.
    """
    charge_id_to_normalized_charge_cache: Dict[int, NormalizedStateChargeV2] = {}
    sentences_by_external_id = {
        sentence.external_id: sentence for sentence in person.sentences
    }
    consecutive_sentence_graph = ConsecutiveSentenceGraph.from_person(person)

    # We normalize sentence statuses first, because they are often the source
    # of truth for sentences themselves yet also depend on information of
    # other sentences.
    # For instance, we can't normalize the statuses of a consecutive sentence
    # without first normalizing the sentences for its parent sentences.
    normalized_snapshots = normalize_sentence_status_snapshots(
        delegate=delegate,
        unprocessed_sentences=sentences_by_external_id,
        processing_order=consecutive_sentence_graph.topological_order,
    )
    normalized_sentences = [
        _normalize_single_sentence(
            sentence=sentence,
            charge_id_to_normalized_charge_cache=charge_id_to_normalized_charge_cache,
            delegate=delegate,
            normalized_snapshots=normalized_snapshots[sentence.external_id],
        )
        for sentence in sentences_by_external_id.values()
    ]
    entities_module_context = entities_module_context_for_module(normalized_entities)
    for sentence in normalized_sentences:
        set_backedges(sentence, entities_module_context)
    return normalized_sentences


def normalize_group_lengths(
    group_lengths: List[StateSentenceGroupLength],
) -> List[NormalizedStateSentenceGroupLength]:
    """
    Normalizes StateSentenceGroupLength, with two updates:
        - Updates sequence_num to be contiguous from 1 based on the partition_key,
          correcting it if external data is increasing, but not contiguous from 1
        - Updates sentence_length_days min/max and projected_completion_date min/max
          to actually be the min/max of the hydrated values, correcting it if external
          data is not consistent. Other date enforcement occurs at ingest in the
          __attrs_post_init__ of StateSentenceLength
    """
    normalized_lengths = []
    for idx, length in enumerate(sorted(group_lengths, key=lambda s: s.partition_key)):
        # If the group length full term min/max fields are not
        # consistent in "state" such that min is always < max, we ensure that
        # they are consistent in "normalized_state"
        # See the __attrs_post_init__ of StateSentenceGroupLength to see what
        # projected field consitency we enforce at ingest.
        full_term_min, full_term_max = get_min_max_fields(
            length.projected_full_term_release_date_min_external,
            length.projected_full_term_release_date_max_external,
        )
        normalized_lengths.append(
            NormalizedStateSentenceGroupLength(
                sequence_num=idx + 1,
                state_code=length.state_code,
                sentence_group_length_id=assert_type(
                    length.sentence_group_length_id, int
                ),
                group_update_datetime=length.group_update_datetime,
                parole_eligibility_date_external=length.parole_eligibility_date_external,
                projected_parole_release_date_external=length.projected_parole_release_date_external,
                projected_full_term_release_date_min_external=full_term_min,
                projected_full_term_release_date_max_external=full_term_max,
                sentence_group_length_metadata=length.sentence_group_length_metadata,
            )
        )
    return normalized_lengths


def get_normalized_sentence_groups(
    sentence_groups: List[StateSentenceGroup],
) -> List[NormalizedStateSentenceGroup]:
    entities_module_context = entities_module_context_for_module(normalized_entities)
    return [
        assert_type(
            set_backedges(
                NormalizedStateSentenceGroup(
                    external_id=group.external_id,
                    state_code=group.state_code,
                    sentence_group_id=assert_type(group.sentence_group_id, int),
                    sentence_group_lengths=normalize_group_lengths(
                        group.sentence_group_lengths
                    ),
                    sentence_inferred_group_id=None,
                ),
                entities_module_context,
            ),
            NormalizedStateSentenceGroup,
        )
        for group in sentence_groups
    ]


def get_normalized_sentencing_entities(
    state_code: StateCode,
    person: StatePerson,
    delegate: StateSpecificSentenceNormalizationDelegate,
    expected_output_entities: set[type[Entity]],
) -> tuple[
    list[NormalizedStateSentence],
    list[NormalizedStateSentenceGroup],
    list[NormalizedStateSentenceInferredGroup],
    list[NormalizedStateSentenceImposedGroup],
]:
    """
    This top level function takes in sentences and sentence groups for a person and
    produces their normalized versions along side inferred groups.
    The NormalizedStateSentence and NormalizedStateSentenceGroup will have the
    sentence_inferred_group_id of their respective NormalizedStateSentenceInferredGroup.
    """
    normalized_sentences_by_external_id = {
        s.external_id: s for s in get_normalized_sentences_for_person(person, delegate)
    }
    normalized_sentence_groups_by_external_id = {
        g.external_id: g for g in get_normalized_sentence_groups(person.sentence_groups)
    }

    if NormalizedStateSentenceInferredGroup in expected_output_entities:
        inferred_groups = get_normalized_inferred_sentence_groups(
            state_code, delegate, list(normalized_sentences_by_external_id.values())
        )
        for inferred_group in inferred_groups:
            inferred_id = inferred_group.sentence_inferred_group_id
            for sentence_external_id in inferred_group.sentence_external_ids:
                sentence = normalized_sentences_by_external_id[sentence_external_id]
                sentence.sentence_inferred_group_id = inferred_id
                if sg_ex_id := sentence.sentence_group_external_id:
                    normalized_sentence_groups_by_external_id[
                        sg_ex_id
                    ].sentence_inferred_group_id = inferred_id
    else:
        inferred_groups = []

    if NormalizedStateSentenceImposedGroup in expected_output_entities:
        imposed_groups = get_normalized_imposed_sentence_groups(
            state_code, delegate, list(normalized_sentences_by_external_id.values())
        )
        for imposed_group in imposed_groups:
            imposed_id = imposed_group.sentence_imposed_group_id
            for sentence_external_id in imposed_group.sentence_external_ids:
                sentence = normalized_sentences_by_external_id[sentence_external_id]
                sentence.sentence_imposed_group_id = imposed_id
    else:
        imposed_groups = []
    return (
        list(normalized_sentences_by_external_id.values()),
        list(normalized_sentence_groups_by_external_id.values()),
        inferred_groups,
        imposed_groups,
    )
