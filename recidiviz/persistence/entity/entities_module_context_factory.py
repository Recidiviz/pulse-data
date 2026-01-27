# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Provides module-specific implementations of EntitiesModuleContext"""
from types import ModuleType
from typing import Type

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.operations import entities as operations_entities
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.utils import environment

ENTITIES_MODULE_CONTEXT_SUPPORTED_MODULES = [
    normalized_entities,
    state_entities,
    operations_entities,
]

_module_context_by_module: dict[ModuleType, EntitiesModuleContext] = {}


@environment.test_only
def clear_entities_module_context_cache() -> None:
    _module_context_by_module.clear()


_module_by_entity_class: dict[type[Entity], ModuleType] = {}


def _get_module_for_entity_class(entity_cls: type[Entity]) -> ModuleType:
    if entity_cls not in _module_by_entity_class:
        for module in ENTITIES_MODULE_CONTEXT_SUPPORTED_MODULES:
            classes_in_module = get_all_entity_classes_in_module(module)
            if entity_cls in classes_in_module:
                # Register all classes in this module for future calls
                for cls in classes_in_module:
                    _module_by_entity_class[cls] = module
                return module
        raise ValueError(f"Unexpected entity_cls [{entity_cls}]")
    return _module_by_entity_class[entity_cls]


class _StateEntitiesModuleContext(EntitiesModuleContext):
    """EntitiesModuleContext for the state entities module defined at state/entities.py."""

    @classmethod
    def entities_module(cls) -> ModuleType:
        return state_entities

    @classmethod
    def class_hierarchy(cls) -> list[str]:
        return [
            # StatePerson hierarchy
            state_entities.StatePerson.__name__,
            state_entities.StatePersonExternalId.__name__,
            state_entities.StatePersonAddressPeriod.__name__,
            state_entities.StatePersonHousingStatusPeriod.__name__,
            state_entities.StatePersonAlias.__name__,
            state_entities.StatePersonRace.__name__,
            state_entities.StateIncarcerationSentence.__name__,
            state_entities.StateSupervisionSentence.__name__,
            state_entities.StateCharge.__name__,
            state_entities.StateIncarcerationPeriod.__name__,
            state_entities.StateIncarcerationIncident.__name__,
            state_entities.StateIncarcerationIncidentOutcome.__name__,
            state_entities.StateSupervisionPeriod.__name__,
            state_entities.StateSupervisionContact.__name__,
            state_entities.StateScheduledSupervisionContact.__name__,
            state_entities.StateSupervisionCaseTypeEntry.__name__,
            state_entities.StateSupervisionViolation.__name__,
            state_entities.StateSupervisionViolatedConditionEntry.__name__,
            state_entities.StateSupervisionViolationTypeEntry.__name__,
            state_entities.StateSupervisionViolationResponse.__name__,
            state_entities.StateSupervisionViolationResponseDecisionEntry.__name__,
            state_entities.StateAssessment.__name__,
            state_entities.StateProgramAssignment.__name__,
            state_entities.StateEarlyDischarge.__name__,
            state_entities.StateEmploymentPeriod.__name__,
            state_entities.StateDrugScreen.__name__,
            state_entities.StateTaskDeadline.__name__,
            state_entities.StateSentence.__name__,
            # TODO(#26240): Replace StateCharge with this entity
            state_entities.StateChargeV2.__name__,
            state_entities.StateSentenceStatusSnapshot.__name__,
            state_entities.StateSentenceLength.__name__,
            state_entities.StateSentenceGroup.__name__,
            state_entities.StateSentenceGroupLength.__name__,
            # StateStaff hierarchy
            state_entities.StateStaff.__name__,
            state_entities.StateStaffExternalId.__name__,
            state_entities.StateStaffRolePeriod.__name__,
            state_entities.StateStaffSupervisorPeriod.__name__,
            state_entities.StateStaffLocationPeriod.__name__,
            state_entities.StateStaffCaseloadTypePeriod.__name__,
            state_entities.StatePersonStaffRelationshipPeriod.__name__,
        ]

    # TODO(#10389): Remove this custom handling for legacy sentence association tables
    #  once we remove these classes from the schema.
    @classmethod
    def custom_association_tables(cls) -> dict[str, tuple[type[Entity], type[Entity]]]:
        return {
            "state_charge_supervision_sentence_association": (
                state_entities.StateCharge,
                state_entities.StateSupervisionSentence,
            ),
            "state_charge_incarceration_sentence_association": (
                state_entities.StateCharge,
                state_entities.StateIncarcerationSentence,
            ),
        }


class _NormalizedStateEntitiesModuleContext(EntitiesModuleContext):
    """EntitiesModuleContext for the normalized state entities module defined at
    state/normalized_entities.py.
    """

    @classmethod
    def entities_module(cls) -> ModuleType:
        return normalized_entities

    @classmethod
    def class_hierarchy(cls) -> list[str]:
        return [
            # NormalizedStatePerson hierarchy
            normalized_entities.NormalizedStatePerson.__name__,
            normalized_entities.NormalizedStatePersonExternalId.__name__,
            normalized_entities.NormalizedStatePersonAddressPeriod.__name__,
            normalized_entities.NormalizedStatePersonHousingStatusPeriod.__name__,
            normalized_entities.NormalizedStatePersonAlias.__name__,
            normalized_entities.NormalizedStatePersonRace.__name__,
            normalized_entities.NormalizedStateIncarcerationSentence.__name__,
            normalized_entities.NormalizedStateSupervisionSentence.__name__,
            normalized_entities.NormalizedStateCharge.__name__,
            normalized_entities.NormalizedStateIncarcerationPeriod.__name__,
            normalized_entities.NormalizedStateIncarcerationIncident.__name__,
            normalized_entities.NormalizedStateIncarcerationIncidentOutcome.__name__,
            normalized_entities.NormalizedStateSupervisionPeriod.__name__,
            normalized_entities.NormalizedStateSupervisionContact.__name__,
            normalized_entities.NormalizedStateScheduledSupervisionContact.__name__,
            normalized_entities.NormalizedStateSupervisionCaseTypeEntry.__name__,
            normalized_entities.NormalizedStateSupervisionViolation.__name__,
            normalized_entities.NormalizedStateSupervisionViolatedConditionEntry.__name__,
            normalized_entities.NormalizedStateSupervisionViolationTypeEntry.__name__,
            normalized_entities.NormalizedStateSupervisionViolationResponse.__name__,
            normalized_entities.NormalizedStateSupervisionViolationResponseDecisionEntry.__name__,
            normalized_entities.NormalizedStateAssessment.__name__,
            normalized_entities.NormalizedStateProgramAssignment.__name__,
            normalized_entities.NormalizedStateEarlyDischarge.__name__,
            normalized_entities.NormalizedStateEmploymentPeriod.__name__,
            normalized_entities.NormalizedStateDrugScreen.__name__,
            normalized_entities.NormalizedStateTaskDeadline.__name__,
            normalized_entities.NormalizedStateSentence.__name__,
            # TODO(#26240): Replace NormalizedStateCharge with this entity
            normalized_entities.NormalizedStateChargeV2.__name__,
            normalized_entities.NormalizedStateSentenceStatusSnapshot.__name__,
            normalized_entities.NormalizedStateSentenceLength.__name__,
            normalized_entities.NormalizedStateSentenceInferredGroup.__name__,
            normalized_entities.NormalizedStateSentenceImposedGroup.__name__,
            normalized_entities.NormalizedStateSentenceGroup.__name__,
            normalized_entities.NormalizedStateSentenceGroupLength.__name__,
            # StateStaff hierarchy
            normalized_entities.NormalizedStateStaff.__name__,
            normalized_entities.NormalizedStateStaffExternalId.__name__,
            normalized_entities.NormalizedStateStaffRolePeriod.__name__,
            normalized_entities.NormalizedStateStaffSupervisorPeriod.__name__,
            normalized_entities.NormalizedStateStaffLocationPeriod.__name__,
            normalized_entities.NormalizedStateStaffCaseloadTypePeriod.__name__,
            normalized_entities.NormalizedStatePersonStaffRelationshipPeriod.__name__,
        ]

    # TODO(#10389): Remove this custom handling for legacy sentence association tables
    #  once we remove these classes from the schema.
    @classmethod
    def custom_association_tables(cls) -> dict[str, tuple[type[Entity], type[Entity]]]:
        return {
            "state_charge_supervision_sentence_association": (
                normalized_entities.NormalizedStateCharge,
                normalized_entities.NormalizedStateSupervisionSentence,
            ),
            "state_charge_incarceration_sentence_association": (
                normalized_entities.NormalizedStateCharge,
                normalized_entities.NormalizedStateIncarcerationSentence,
            ),
        }


class _OperationsEntitiesModuleContext(EntitiesModuleContext):
    """EntitiesModuleContext for the operations entities module defined at
    operations/entities.py.
    """

    @classmethod
    def entities_module(cls) -> ModuleType:
        return operations_entities

    @classmethod
    def class_hierarchy(cls) -> list[str]:
        return [
            # RawFileMetadata Hierarchy
            operations_entities.DirectIngestRawFileImportRun.__name__,
            operations_entities.DirectIngestRawFileImport.__name__,
            operations_entities.DirectIngestRawBigQueryFileMetadata.__name__,
            operations_entities.DirectIngestRawGCSFileMetadata.__name__,
            # DataflowMetadata Hierarchy
            operations_entities.DirectIngestDataflowJob.__name__,
            operations_entities.DirectIngestDataflowRawTableUpperBounds.__name__,
            # Classes w/o Relationships here to satifsy includes all classes
            operations_entities.DirectIngestRawDataResourceLock.__name__,
            operations_entities.DirectIngestSftpIngestReadyFileMetadata.__name__,
            operations_entities.DirectIngestSftpRemoteFileMetadata.__name__,
            operations_entities.DirectIngestRawDataFlashStatus.__name__,
        ]


def entities_module_context_for_module(
    entities_module: ModuleType,
) -> EntitiesModuleContext:
    """Returns the EntitiesModuleContext for a given module. Throws if the module is not
    supported.
    """
    context: EntitiesModuleContext
    if entities_module not in _module_context_by_module:
        if entities_module is state_entities:
            context = _StateEntitiesModuleContext()
        elif entities_module is normalized_entities:
            context = _NormalizedStateEntitiesModuleContext()
        elif entities_module is operations_entities:
            context = _OperationsEntitiesModuleContext()
        else:
            raise ValueError(f"Unsupported module: [{entities_module}]")
        _module_context_by_module[context.entities_module()] = context

    return _module_context_by_module[entities_module]


def entities_module_context_for_entity(entity: Entity) -> EntitiesModuleContext:
    """Returns an EntityModuleContext that can be used to determine information about
    structure of any Entity classes in the module associated with the given
    |entity|.
    """
    return entities_module_context_for_entity_class(type(entity))


def entities_module_context_for_entity_class(
    entity_cls: Type[Entity],
) -> EntitiesModuleContext:
    """Returns an EntityModuleContext that can be used to determine information about
    structure of any Entity classes in the module associated with the given
    |entity_cls|.
    """
    return entities_module_context_for_module(_get_module_for_entity_class(entity_cls))
