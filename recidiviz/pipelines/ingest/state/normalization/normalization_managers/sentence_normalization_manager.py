# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains the logic for a SentenceNormalizationManager that manages the normalization
of StateCharge entities in the calculation pipelines."""
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Type

from more_itertools import first

from recidiviz.common.constants.state.state_sentence import StateSentenceType
from recidiviz.common.ncic import get_description
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
)
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateEarlyDischarge,
    StateIncarcerationSentence,
    StateSupervisionSentence,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateCharge,
    NormalizedStateChargeV2,
    NormalizedStateIncarcerationSentence,
    NormalizedStateSentence,
    NormalizedStateSupervisionSentence,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.ingest.state.normalization.utils import get_min_max_fields
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.utils.types import assert_type


def _sort_sentences_by_severity(
    sentences: list[NormalizedStateSentence],
) -> list[NormalizedStateSentence]:
    # TODO(#44111): consider either deprecating or refactoring this logic so it can replace the SQL logic in `person_charge_severity_ranking`
    """
    Sorts the list of sentences by their severity (in priority order):
        - A state prison sentence is more severe than other types
        - A life sentence is more severe than non-life sentences
        - A sentence with the most controlling charges (when hydrated) is more severe than other sentences
        - The sentence with the longest initial sentence length (at imposition) is more severe than other sentences,
          with sentence length determined by the first non-null date field in this order:
              - sentence length days max
              - sentence length days min
              - projected completion date max external (calculated from sentence serving start date)
              - projected completion date min external (calculated from sentence serving start date)
        - A sentence linked to at least 1 charge classified as a violent offense OR a sex offense is more severe
        - If all prior attributes are equal (or null) than the sentence with the highest sentence_id is the most severe
    """

    def _sort_key(
        sentence: NormalizedStateSentence,
    ) -> tuple[int, int, int, int, int, int]:
        # TODO(#39589): add more granular priority levels here to cover more sentence types
        sentence_type_priority = (
            0 if sentence.sentence_type == StateSentenceType.STATE_PRISON else -1
        )
        life_sentence_priority = 0 if sentence.is_life else -1
        controlling_charges_count = sum(
            (charge.is_controlling or False) for charge in sentence.charges
        )
        violent_or_sex_offense_charges = [
            charge
            for charge in sentence.charges
            if charge.is_violent or charge.is_sex_offense
        ]
        violent_or_sex_offense_priority = (
            0 if len(violent_or_sex_offense_charges) > 0 else -1
        )
        first_sentence_length = first(
            sorted(sentence.sentence_lengths, key=lambda l: l.length_update_datetime),
            default=None,
        )
        # Return -1 for the length priority value if there is no sentence length data for this sentence
        if not first_sentence_length:
            return (
                sentence_type_priority,
                life_sentence_priority,
                controlling_charges_count,
                -1,
                violent_or_sex_offense_priority,
                sentence.sentence_id,
            )
        min_length_days, max_length_days = get_min_max_fields(
            first_sentence_length.sentence_length_days_min,
            first_sentence_length.sentence_length_days_max,
        )
        # Try to compute a sentence length days value from the projected completion dates
        # and sentence serving start date if there is no min/max sentence length days data
        if (min_length_days is None) & (max_length_days is None):
            # Get the sentence start date from the first SERVING status
            sentence_serving_start_date = first(
                sorted(
                    [
                        sentence_status
                        for sentence_status in sentence.sentence_status_snapshots
                        if sentence_status.status.is_considered_serving_status
                    ],
                    key=lambda s: s.status_update_datetime,
                ),
                default=None,
            )
            projected_completion_date = (
                first_sentence_length.projected_completion_date_max_external
                or first_sentence_length.projected_completion_date_min_external
            )
            if (sentence_serving_start_date is not None) and (
                projected_completion_date is not None
            ):
                max_length_days = (
                    projected_completion_date
                    - sentence_serving_start_date.status_update_datetime.date()
                ).days
        return (
            sentence_type_priority,
            life_sentence_priority,
            controlling_charges_count,
            assert_type(max_length_days or min_length_days or -1, int),
            violent_or_sex_offense_priority,
            sentence.sentence_id,
        )

    return list(sorted(sentences, key=_sort_key, reverse=True))


def sentences_overlap_serving(
    s1: NormalizedStateSentence, s2: NormalizedStateSentence
) -> bool:
    span1 = s1.first_serving_status_to_terminating_status_dt_range
    span2 = s2.first_serving_status_to_terminating_status_dt_range
    if not (span1 and span2):
        return False
    if span1.lower_bound_inclusive in span2 or span2.lower_bound_inclusive in span1:
        return True
    if span1.upper_bound_exclusive == span2.lower_bound_inclusive:
        return True
    if span2.upper_bound_exclusive == span1.lower_bound_inclusive:
        return True
    return False


# pylint: disable=unused-argument
class StateSpecificSentenceNormalizationDelegate(StateSpecificDelegate):
    """Interface for state-specific decisions involved in normalizing sentences
    for calculations."""

    def __init__(self) -> None:
        if (
            self.allow_non_credit_serving
            and self.correct_imposed_pending_serving_statuses
        ):
            raise ValueError(
                "Both allow_non_credit_serving and correct_imposed_pending_serving_statuses return True. "
                "We can only have one of these properties return True (if a state is providing "
                "explicit serving start dates, then we should be using them). "
                "If this is too strict, please ping #platform-team"
            )

    @property
    def override_projected_completion_dates_using_sentence_length_days(self) -> bool:
        """
        If True, we'll calculate projected_completion_date_min/max_external
        from <the serving start date> + <sentence_length_days_min/max>

        Note if your state provides completion dates and this is True,
        we will override that data!
        """
        return False

    @property
    def correct_imposed_pending_serving_statuses(self) -> bool:
        """
        If True, SERVING statuses for a sentence that are before it's
        parents' final terminating statuses will be changed to IMPOSED_PENDING_SERVING.
        We will also create an initial SERVING status if one does not exist at
        that datetime.

        The delegate will fail if this property is True and allow_non_credit_serving
        is also True (if a state is providing explicit serving start dates, then
        we should be using them). If this is too strict, please ping #platform-team
        """
        return False

    def update_incarceration_sentence(
        self, incarceration_sentence: StateIncarcerationSentence
    ) -> StateIncarcerationSentence:
        """Contains state-specific logic for updating certain fields on incarceration sentences.

        By default, returns the incarceration sentence itself."""
        return incarceration_sentence

    def update_supervision_sentence(
        self, supervision_sentence: StateSupervisionSentence
    ) -> StateSupervisionSentence:
        """Contains state-specific logic for updating certain fields on supervision sentences.

        By default, returns the supervision sentence itself."""
        return supervision_sentence

    @property
    def correct_early_completed_statuses(self) -> bool:
        """
        If True, if we see a StateSentenceStatusSnapshot that is not the last status for a sentence which
        has status COMPLETED, correct that status to SERVING. Otherwise, we'll throw if we see a COMPLETED
        status that is followed by other statuses.
        """
        return False

    @property
    def allow_non_credit_serving(self) -> bool:
        """
        If True, statuses that count towards serving that are before their sentence's
        current_state_provided_start_date will be switched to NON_CREDIT_SERVING.
        Only return True in a state where we know that serving credit can be removed.
        """
        return False

    @staticmethod
    def sentences_are_in_same_imposed_group(
        s1: NormalizedStateSentence, s2: NormalizedStateSentence
    ) -> bool:
        """Returns True if the two given sentences are imposed together."""
        return (s1.imposed_date == s2.imposed_date) and (
            s1.sentencing_authority == s2.sentencing_authority
        )

    @staticmethod
    def get_most_severe_charge(
        sentences: list[NormalizedStateSentence],
    ) -> NormalizedStateChargeV2:
        """
        Returns the most severe charge for this group of sentences.
        By default, this will return the controlling charge on the most severe sentence.
        """
        most_severe_sentence = first(_sort_sentences_by_severity(sentences))
        # Pick a controlling charge first, then a violent or sex offense charge,
        # and then if all else is the same arbitrarily pick the highest charge ID
        charges = list(
            sorted(
                most_severe_sentence.charges,
                key=lambda c: (
                    c.is_controlling or False,
                    c.is_violent or c.is_sex_offense or False,
                    c.charge_v2_id,
                ),
                reverse=True,
            )
        )
        return charges[0]

    @staticmethod
    def sentences_are_in_same_inferred_group(
        s1: NormalizedStateSentence, s2: NormalizedStateSentence
    ) -> bool:
        """
        Returns True if the two given sentences belong in the same
        NormalizedStateSentenceInferredGroup.
        An inferred group is created when two sentences:
            - Have the same NormalizedStateSentenceGroup
            - Have the same imposed_date
            - Have a common charge
            - Have charges with a common offense_date
            - Have an overlapping span of time between the first SERVING
              status and terminating status.
        """
        # Sentences have the same state provided sentence group
        if s1.sentence_group_external_id == s2.sentence_group_external_id and (
            s1.sentence_group_external_id and s2.sentence_group_external_id
        ):
            return True
        # Sentences have the same imposed date
        if (s1.imposed_date == s2.imposed_date) and (
            s1.imposed_date and s2.imposed_date
        ):
            return True
        # Sentences have a common charge
        if {c.charge_v2_id for c in s1.charges}.intersection(
            {c.charge_v2_id for c in s2.charges}
        ):
            return True
        # Sentences have a common offense date
        if {c.offense_date for c in s1.charges if c.offense_date}.intersection(
            {c.offense_date for c in s2.charges if c.offense_date}
        ):
            return True
        return sentences_overlap_serving(s1, s2)


class SentenceNormalizationManager(EntityNormalizationManager):
    """Interface for generalized and state-specific normalization of StateCharges
    for use in calculations."""

    def __init__(
        self,
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        delegate: StateSpecificSentenceNormalizationDelegate,
    ) -> None:
        self._incarceration_sentences = incarceration_sentences
        self._supervision_sentences = supervision_sentences
        self._normalized_incarceration_sentences_and_additional_attributes: Optional[
            Tuple[List[StateIncarcerationSentence], AdditionalAttributesMap]
        ] = None
        self._normalized_supervision_sentences_and_additional_attributes: Optional[
            Tuple[List[StateSupervisionSentence], AdditionalAttributesMap]
        ] = None

        self.delegate = delegate

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [
            StateCharge,
            StateSupervisionSentence,
            StateIncarcerationSentence,
            StateEarlyDischarge,
        ]

    @staticmethod
    def normalized_entity_associations() -> List[Tuple[Type[Entity], Type[Entity]]]:
        return [
            (StateCharge, StateSupervisionSentence),
            (StateCharge, StateIncarcerationSentence),
        ]

    def _merge_multi_parent_charges(
        self,
        normalized_incarceration_sentences: list[NormalizedStateIncarcerationSentence],
        normalized_supervision_sentences: list[NormalizedStateSupervisionSentence],
    ) -> None:
        """Given a list of normalized legacy incarceration and supervision sentences,
        merges charges so any duplicates that are linked to both an incarceration and
        supervision sentence are merged into one.
        """
        charge_external_id_to_parents: dict[
            str,
            list[
                NormalizedStateIncarcerationSentence
                | NormalizedStateSupervisionSentence
            ],
        ] = defaultdict(list)

        sentence: (
            NormalizedStateIncarcerationSentence | NormalizedStateSupervisionSentence
        )
        for sentence in normalized_incarceration_sentences:
            for charge in sentence.charges:
                charge_external_id_to_parents[charge.external_id].append(sentence)

        for sentence in normalized_supervision_sentences:
            for charge in sentence.charges:
                charge_external_id_to_parents[charge.external_id].append(sentence)

        for (
            charge_external_id,
            parent_sentences_list,
        ) in charge_external_id_to_parents.items():
            primary_charge: NormalizedStateCharge = next(
                c
                for c in parent_sentences_list[0].charges
                if c.external_id == charge_external_id
            )

            for sentence in parent_sentences_list[1:]:
                index_of_charge_on_sentence = next(
                    i
                    for i, c in enumerate(sentence.charges)
                    if c.external_id == charge_external_id
                )
                sentence.charges[index_of_charge_on_sentence] = primary_charge

                if isinstance(sentence, NormalizedStateIncarcerationSentence):
                    if not any(
                        s
                        for s in primary_charge.incarceration_sentences
                        if s.external_id == sentence.external_id
                    ):
                        primary_charge.incarceration_sentences.append(sentence)
                elif isinstance(sentence, NormalizedStateSupervisionSentence):
                    if not any(
                        s
                        for s in primary_charge.supervision_sentences
                        if s.external_id == sentence.external_id
                    ):
                        primary_charge.supervision_sentences.append(sentence)
                else:
                    raise ValueError(f"Unexpected sentence type [{type(sentence)}]")

    def get_normalized_sentences(
        self,
    ) -> tuple[
        list[NormalizedStateIncarcerationSentence],
        list[NormalizedStateSupervisionSentence],
    ]:
        """Generates and returns a list of NormalizedStateIncarcerationSentence and
        NormalizedStateSupervisionSentence from the un-normalized inputs to this
        SentenceNormalizationManager class.
        """
        (
            processed_incarceration_sentences,
            additional_incarceration_sentence_attributes,
        ) = self.normalized_incarceration_sentences_and_additional_attributes()

        (
            processed_supervision_sentences,
            additional_supervision_sentence_attributes,
        ) = self.normalized_supervision_sentences_and_additional_attributes()

        normalized_incarceration_sentences = (
            convert_entity_trees_to_normalized_versions(
                processed_incarceration_sentences,
                NormalizedStateIncarcerationSentence,
                additional_incarceration_sentence_attributes,
            )
        )

        normalized_supervision_sentences = convert_entity_trees_to_normalized_versions(
            processed_supervision_sentences,
            NormalizedStateSupervisionSentence,
            additional_supervision_sentence_attributes,
        )

        self._merge_multi_parent_charges(
            normalized_incarceration_sentences, normalized_supervision_sentences
        )

        return normalized_incarceration_sentences, normalized_supervision_sentences

    def normalized_incarceration_sentences_and_additional_attributes(
        self,
    ) -> Tuple[List[StateIncarcerationSentence], AdditionalAttributesMap]:
        """Performs normalization on incarceration sentences."""

        if not self._normalized_incarceration_sentences_and_additional_attributes:
            incarceration_sentences_for_normalization = [
                self.delegate.update_incarceration_sentence(incarceration_sentence)
                for incarceration_sentence in self._incarceration_sentences
            ]
            self._normalized_incarceration_sentences_and_additional_attributes = (
                incarceration_sentences_for_normalization,
                self.additional_attributes_map_for_normalized_incarceration_sentences(
                    incarceration_sentences_for_normalization
                ),
            )
        return self._normalized_incarceration_sentences_and_additional_attributes

    def additional_attributes_map_for_normalized_incarceration_sentences(
        self,
        incarceration_sentences: List[StateIncarcerationSentence],
    ) -> AdditionalAttributesMap:
        """Returns the attributes that should be set on the normalized version of each of
        the StateIncarcerationSentences for each of the attributes that are unique to the
        NormalizedStateIncarcerationSentence."""

        shared_attributes = get_shared_additional_attributes_map_for_entities(
            entities=incarceration_sentences
        )

        charges = [
            charge
            for incarceration_sentence in incarceration_sentences
            for charge in incarceration_sentence.charges
        ]

        return merge_additional_attributes_maps(
            [
                shared_attributes,
                self.additional_attributes_map_for_normalized_charges(charges),
            ]
        )

    def normalized_supervision_sentences_and_additional_attributes(
        self,
    ) -> Tuple[List[StateSupervisionSentence], AdditionalAttributesMap]:
        """Performs normalization on supervision sentences."""

        if not self._normalized_supervision_sentences_and_additional_attributes:
            supervision_sentences_for_normalization = [
                self.delegate.update_supervision_sentence(supervision_sentence)
                for supervision_sentence in self._supervision_sentences
            ]
            self._normalized_supervision_sentences_and_additional_attributes = (
                supervision_sentences_for_normalization,
                self.additional_attributes_map_for_normalized_supervision_sentences(
                    supervision_sentences_for_normalization
                ),
            )
        return self._normalized_supervision_sentences_and_additional_attributes

    def additional_attributes_map_for_normalized_supervision_sentences(
        self,
        supervision_sentences: List[StateSupervisionSentence],
    ) -> AdditionalAttributesMap:
        """Returns the attributes that should be set on the normalized version of each of
        the StateSupervisionSentences for each of the attributes that are unique to the
        NormalizedStateSupervisionSentence."""

        shared_attributes = get_shared_additional_attributes_map_for_entities(
            entities=supervision_sentences
        )

        charges = [
            charge
            for supervision_sentence in supervision_sentences
            for charge in supervision_sentence.charges
        ]

        return merge_additional_attributes_maps(
            [
                shared_attributes,
                self.additional_attributes_map_for_normalized_charges(charges),
            ]
        )

    def additional_attributes_map_for_normalized_charges(
        self, charges: List[StateCharge]
    ) -> AdditionalAttributesMap:
        shared_additional_attributes = (
            get_shared_additional_attributes_map_for_entities(entities=charges)
        )

        charges_additional_attributes_map: Dict[int, Dict[str, Any]] = {}

        for charge in charges:
            if not charge.charge_id:
                raise ValueError(f"Unexpected charge with no charge_id {charge}")
            charges_additional_attributes_map[
                charge.charge_id
            ] = self._get_extra_metadata_for_charge(charge)

        return merge_additional_attributes_maps(
            [
                shared_additional_attributes,
                {StateCharge.__name__: charges_additional_attributes_map},
            ]
        )

    def _get_extra_metadata_for_charge(self, charge: StateCharge) -> Dict[str, Any]:
        """Adds extra metadata for the StateCharge entity."""
        if not charge.charge_id:
            raise ValueError(f"Unexpected charge with no charge_id {charge}")

        # Columns that are renamed to *_external
        return {
            "ncic_code_external": charge.ncic_code,
            "ncic_category_external": (
                get_description(charge.ncic_code) if charge.ncic_code else None
            ),
            "description_external": charge.description,
            "is_violent_external": charge.is_violent,
            "is_drug_external": charge.is_drug,
            "is_sex_offense_external": charge.is_sex_offense,
        }
