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
"""Tests the entity_normalizer."""
import unittest
from copy import deepcopy
from typing import Dict, List, Optional, Sequence

import attr
import mock

from recidiviz.calculator.pipeline.normalization.base_entity_normalizer import (
    EntityNormalizerResult,
)
from recidiviz.calculator.pipeline.normalization.comprehensive import (
    entity_normalizer,
    pipeline,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateEntity,
    NormalizedStateIncarcerationPeriod,
    NormalizedStateProgramAssignment,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities_utils import (
    AdditionalAttributesMap,
    convert_entity_trees_to_normalized_versions,
    normalized_entity_class_with_base_class_name,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_delegates,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateProgramAssignment,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalized_entities_utils_test import (
    get_normalized_violation_tree,
    get_violation_tree,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.state_calculation_config_manager_test import (
    STATE_DELEGATES_FOR_TESTS,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
)

_STATE_CODE = "US_XX"


class TestNormalizeEntities(unittest.TestCase):
    """Tests the normalize_entities function on the ComprehensiveEntityNormalizer."""

    def setUp(self) -> None:
        self.pipeline_config = (
            pipeline.ComprehensiveNormalizationPipelineRunDelegate.pipeline_config()
        )
        self.entity_normalizer = entity_normalizer.ComprehensiveEntityNormalizer()

        self.full_graph_person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=False, set_ids=True
        )

        self.violation_responses: List[StateSupervisionViolationResponse] = []
        for v in self.full_graph_person.supervision_violations:
            self.violation_responses.extend(v.supervision_violation_responses)

    def _run_normalize_entities(
        self,
        incarceration_periods: Optional[List[StateIncarcerationPeriod]] = None,
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
        program_assignments: Optional[List[StateProgramAssignment]] = None,
        incarceration_sentences: Optional[List[StateIncarcerationSentence]] = None,
        supervision_sentences: Optional[List[StateSupervisionSentence]] = None,
        state_code_override: Optional[str] = None,
    ) -> EntityNormalizerResult:
        """Helper for testing the normalize_entities function on the
        ComprehensiveEntityNormalizer."""

        state_specific_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.state_utils"
            ".state_calculation_config_manager.get_all_state_specific_delegates",
            return_value=STATE_DELEGATES_FOR_TESTS,
        )
        if not state_code_override:
            state_specific_delegate_patcher.start()

        required_delegates = get_required_state_specific_delegates(
            state_code=(state_code_override or _STATE_CODE),
            required_delegates=self.pipeline_config.state_specific_required_delegates,
        )

        if not state_code_override:
            state_specific_delegate_patcher.stop()

        all_kwargs = {
            **required_delegates,
            StateIncarcerationPeriod.__name__: incarceration_periods or [],
            StateSupervisionPeriod.__name__: supervision_periods or [],
            StateSupervisionViolationResponse.__name__: violation_responses or [],
            StateProgramAssignment.__name__: program_assignments or [],
            StateIncarcerationSentence.__name__: incarceration_sentences or [],
            StateSupervisionSentence.__name__: supervision_sentences or [],
        }
        return self.entity_normalizer.normalize_entities(all_kwargs)

    def test_normalize_entities(self) -> None:
        expected_additional_attributes_map: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                ip.incarceration_period_id: {
                    "sequence_num": index,
                    "purpose_for_incarceration_subtype": None,
                }
                for index, ip in enumerate(self.full_graph_person.incarceration_periods)
                if ip.incarceration_period_id
            }
        }

        expected_additional_attributes_map.update(
            {
                StateSupervisionPeriod.__name__: {
                    sp.supervision_period_id: {
                        "sequence_num": index,
                    }
                    for index, sp in enumerate(
                        self.full_graph_person.supervision_periods
                    )
                    if sp.supervision_period_id
                }
            }
        )

        expected_additional_attributes_map.update(
            {
                StateProgramAssignment.__name__: {
                    pa.program_assignment_id: {
                        "sequence_num": index,
                    }
                    for index, pa in enumerate(
                        self.full_graph_person.program_assignments
                    )
                    if pa.program_assignment_id
                }
            }
        )

        violation = get_violation_tree()
        violation_responses = violation.supervision_violation_responses

        expected_additional_attributes_map.update(
            {
                StateSupervisionViolationResponse.__name__: {
                    vr.supervision_violation_response_id: {
                        "sequence_num": index,
                    }
                    for index, vr in enumerate(violation_responses)
                    if vr.supervision_violation_response_id
                }
            }
        )

        expected_output_entities: Dict[str, Sequence[Entity]] = {
            StateIncarcerationPeriod.__name__: [
                attr.evolve(
                    ip,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                )
                for ip in self.full_graph_person.incarceration_periods
            ],
            StateSupervisionPeriod.__name__: [
                attr.evolve(sp) for sp in self.full_graph_person.supervision_periods
            ],
            StateSupervisionViolation.__name__: [attr.evolve(violation)],
            StateProgramAssignment.__name__: [
                attr.evolve(pa) for pa in self.full_graph_person.program_assignments
            ],
        }

        normalized_entities, additional_attributes_map = self._run_normalize_entities(
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=self.full_graph_person.supervision_periods,
            violation_responses=violation_responses,
            program_assignments=self.full_graph_person.program_assignments,
        )

        self.assertEqual(expected_additional_attributes_map, additional_attributes_map)

        for entity_name, items in normalized_entities.items():
            self.assertEqual(expected_output_entities[entity_name], items)

    def test_normalize_entities_missing_entities(self) -> None:
        """Tests that calling normalize_entities will still work even when a person
        doesn't have instances of all of the entities that get normalized."""

        _ = self._run_normalize_entities(
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=None,
            violation_responses=get_violation_tree().supervision_violation_responses,
            program_assignments=None,
        )


class TestNormalizeEntitiesConvertedToNormalized(unittest.TestCase):
    """Tests that the output of the normalize_entities function on the
    ComprehensiveEntityNormalizer can be converted into fully hydrated trees of
    Normalized entities.

    This isn't a process that is ever done in the pipelines, but ensures that the
    values in the AdditionalAttributesMap in the output contain all required values
    necessary to load the Normalized version of the entities in the downstream metric
    pipelines.
    """

    def setUp(self) -> None:
        self.pipeline_config = (
            pipeline.ComprehensiveNormalizationPipelineRunDelegate.pipeline_config()
        )
        self.entity_normalizer = entity_normalizer.ComprehensiveEntityNormalizer()

        self.full_graph_person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=False, set_ids=True
        )

        self.violation_responses: List[StateSupervisionViolationResponse] = []
        for v in self.full_graph_person.supervision_violations:
            self.violation_responses.extend(v.supervision_violation_responses)

        self.field_index = CoreEntityFieldIndex()

    def _normalize_entities_and_convert(
        self,
        incarceration_periods: Optional[List[StateIncarcerationPeriod]] = None,
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
        program_assignments: Optional[List[StateProgramAssignment]] = None,
        incarceration_sentences: Optional[List[StateIncarcerationSentence]] = None,
        supervision_sentences: Optional[List[StateSupervisionSentence]] = None,
        state_code_override: Optional[str] = None,
    ) -> Dict[str, Sequence[NormalizedStateEntity]]:
        """Helper for testing the find_events function on the identifier."""

        state_specific_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.state_utils"
            ".state_calculation_config_manager.get_all_state_specific_delegates",
            return_value=STATE_DELEGATES_FOR_TESTS,
        )
        if not state_code_override:
            state_specific_delegate_patcher.start()

        required_delegates = get_required_state_specific_delegates(
            state_code=(state_code_override or _STATE_CODE),
            required_delegates=self.pipeline_config.state_specific_required_delegates,
        )

        if not state_code_override:
            state_specific_delegate_patcher.stop()

        all_kwargs = {
            **required_delegates,
            StateIncarcerationPeriod.__name__: incarceration_periods or [],
            StateSupervisionPeriod.__name__: supervision_periods or [],
            StateSupervisionViolationResponse.__name__: violation_responses or [],
            StateProgramAssignment.__name__: program_assignments or [],
            StateIncarcerationSentence.__name__: incarceration_sentences or [],
            StateSupervisionSentence.__name__: supervision_sentences or [],
        }

        (
            all_processed_entities,
            additional_attributes_map,
        ) = self.entity_normalizer.normalize_entities(all_kwargs)

        normalized_entities: Dict[str, Sequence[NormalizedStateEntity]] = {}

        for entity_name, processed_entities in all_processed_entities.items():
            normalized_entities[
                entity_name
            ] = convert_entity_trees_to_normalized_versions(
                root_entities=processed_entities,
                normalized_entity_class=normalized_entity_class_with_base_class_name(
                    entity_name
                ),
                additional_attributes_map=additional_attributes_map,
                field_index=self.field_index,
            )

        return normalized_entities

    def test_normalize_entities(self) -> None:
        normalized_ips: List[NormalizedStateIncarcerationPeriod] = []

        for index, ip in enumerate(self.full_graph_person.incarceration_periods):
            normalized_ip = NormalizedStateIncarcerationPeriod.new_with_defaults(
                **{
                    **ip.__dict__,
                    **{
                        "specialized_purpose_for_incarceration": StateSpecializedPurposeForIncarceration.GENERAL,
                        "sequence_num": index,
                    },
                }
            )

            normalized_ips.append(normalized_ip)

        normalized_sps: List[NormalizedStateSupervisionPeriod] = []

        for index, sp in enumerate(self.full_graph_person.supervision_periods):
            normalized_case_type_entries: Sequence[
                NormalizedStateSupervisionCaseTypeEntry
            ] = [
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
                    **{
                        field: value
                        for field, value in cte.__dict__.items()
                        if field != "supervision_period"
                    }
                )
                for cte in sp.case_type_entries
            ]

            sp_copy = deepcopy(sp)
            sp_copy.case_type_entries = []
            normalized_sp = NormalizedStateSupervisionPeriod.new_with_defaults(
                **{
                    **{
                        field: value
                        for field, value in sp_copy.__dict__.items()
                        if field not in ("supervising_officer", "case_type_entries")
                    },
                    **{
                        "sequence_num": index,
                        "case_type_entries": normalized_case_type_entries,
                    },
                }
            )

            normalized_sps.append(normalized_sp)

            for cte in normalized_case_type_entries:
                cte.supervision_period = normalized_sp

        normalized_pas: List[NormalizedStateProgramAssignment] = []

        for index, pa in enumerate(self.full_graph_person.program_assignments):
            normalized_pa = NormalizedStateProgramAssignment.new_with_defaults(
                **{
                    **{
                        field: value
                        for field, value in pa.__dict__.items()
                        if field != "referring_agent"
                    },
                    **{
                        "sequence_num": index,
                    },
                }
            )

            normalized_pas.append(normalized_pa)

        normalized_entities = self._normalize_entities_and_convert(
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=self.full_graph_person.supervision_periods,
            violation_responses=get_violation_tree().supervision_violation_responses,
            program_assignments=self.full_graph_person.program_assignments,
        )

        expected_normalized_entities: Dict[str, Sequence[NormalizedStateEntity]] = {
            StateIncarcerationPeriod.__name__: normalized_ips,
            StateSupervisionPeriod.__name__: normalized_sps,
            StateSupervisionViolation.__name__: [get_normalized_violation_tree()],
            StateProgramAssignment.__name__: normalized_pas,
        }

        for entity_name, items in normalized_entities.items():
            self.assertEqual(expected_normalized_entities[entity_name], items)

    def test_normalize_entities_missing_entities(self) -> None:
        """Tests that calling normalize_entities will still work even when a person
        doesn't have instances of all of the entities that get normalized."""
        normalized_ips: List[NormalizedStateIncarcerationPeriod] = []

        for index, ip in enumerate(self.full_graph_person.incarceration_periods):
            normalized_ip = NormalizedStateIncarcerationPeriod.new_with_defaults(
                **{
                    **ip.__dict__,
                    **{
                        "specialized_purpose_for_incarceration": StateSpecializedPurposeForIncarceration.GENERAL,
                        "sequence_num": index,
                    },
                }
            )

            normalized_ips.append(normalized_ip)

        normalized_entities = self._normalize_entities_and_convert(
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=None,
            violation_responses=get_violation_tree().supervision_violation_responses,
            program_assignments=None,
        )

        expected_normalized_entities: Dict[str, Sequence[NormalizedStateEntity]] = {
            StateIncarcerationPeriod.__name__: normalized_ips,
            StateSupervisionPeriod.__name__: [],
            StateSupervisionViolation.__name__: [get_normalized_violation_tree()],
            StateProgramAssignment.__name__: [],
        }

        for entity_name, entity_list in normalized_entities.items():
            self.assertEqual(expected_normalized_entities[entity_name], entity_list)
