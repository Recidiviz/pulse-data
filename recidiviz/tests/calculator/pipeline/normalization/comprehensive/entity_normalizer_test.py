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

import mock

from recidiviz.calculator.pipeline.normalization.comprehensive import (
    entity_normalizer,
    pipeline,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_delegates,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateProgramAssignment,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolationResponse,
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
        # TODO(#10724): Change this to Sequence[NormalizedStateEntity] once the
        #  conversion to Normalized entities is built
    ) -> Dict[str, Sequence[Entity]]:
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
        return self.entity_normalizer.normalize_entities(all_kwargs)

    def test_normalize_entities(self) -> None:
        normalized_ips = deepcopy(self.full_graph_person.incarceration_periods)

        for ip in normalized_ips:
            ip.specialized_purpose_for_incarceration = (
                StateSpecializedPurposeForIncarceration.GENERAL
            )

        normalized_sps = deepcopy(self.full_graph_person.supervision_periods)

        normalized_vrs = deepcopy(self.violation_responses)

        normalized_pas = deepcopy(self.full_graph_person.program_assignments)

        normalized_entities = self._run_normalize_entities(
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=self.full_graph_person.supervision_periods,
            violation_responses=self.violation_responses,
            program_assignments=self.full_graph_person.program_assignments,
        )

        # TODO(#10724): Update this expected output once the conversion to Normalized
        #  entities is built
        expected_normalized_entities: Dict[str, Sequence[Entity]] = {
            StateIncarcerationPeriod.__name__: normalized_ips,
            StateSupervisionPeriod.__name__: normalized_sps,
            StateSupervisionViolationResponse.__name__: normalized_vrs,
            StateProgramAssignment.__name__: normalized_pas,
        }

        for entity_name, items in normalized_entities.items():
            self.assertEqual(expected_normalized_entities[entity_name], items)

    def test_normalize_entities_missing_entities(self) -> None:
        """Tests that calling normalize_entities will still work even when a person
        doesn't have instances of all of the entities that get normalized."""
        normalized_ips = deepcopy(self.full_graph_person.incarceration_periods)

        for ip in normalized_ips:
            ip.specialized_purpose_for_incarceration = (
                StateSpecializedPurposeForIncarceration.GENERAL
            )

        normalized_vrs = deepcopy(self.violation_responses)

        normalized_entities = self._run_normalize_entities(
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=None,
            violation_responses=self.violation_responses,
            program_assignments=None,
        )

        # TODO(#10724): Update this expected output once the conversion to Normalized
        #  entities is built
        expected_normalized_entities: Dict[str, Sequence[Entity]] = {
            StateIncarcerationPeriod.__name__: normalized_ips,
            StateSupervisionPeriod.__name__: [],
            StateSupervisionViolationResponse.__name__: normalized_vrs,
            StateProgramAssignment.__name__: [],
        }

        for entity_name, entities in normalized_entities.items():
            self.assertEqual(expected_normalized_entities[entity_name], entities)
