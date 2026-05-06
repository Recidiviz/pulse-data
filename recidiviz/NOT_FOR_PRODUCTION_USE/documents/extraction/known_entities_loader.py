# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Loader for per-state known entity YAML files.

Known entity YAMLs live at:
    extraction/config/known_entities/{state_code}.yaml

Each file lists proper-noun entities (treatment programs, drug courts, staffing
agencies, etc.) that the first-pass extraction LLM should classify correctly
even when case note context is sparse. The loader renders these into
{known_entities_context} prompt variables for the housing and employment
extraction prompt templates.
"""
import os
from enum import Enum
from pathlib import Path

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.yaml_dict import YAMLDict

_KNOWN_ENTITIES_CONFIG_DIR = os.path.join(
    os.path.dirname(__file__), "config", "known_entities"
)


class KnownEntityType(Enum):
    """Classification of a known proper-noun entity for use in extraction prompts."""

    NONRESIDENTIAL_PROGRAM = "nonresidential_program"
    COMMUNITY_CORRECTIONS = "community_corrections"
    RESIDENTIAL_TREATMENT = "residential_treatment"
    SOBER_LIVING = "sober_living"
    HALFWAY_HOUSE = "halfway_house"
    SHELTER = "shelter"
    HOTEL_MOTEL = "hotel_motel"
    STAFFING_AGENCY = "staffing_agency"
    EMPLOYER = "employer"

    @property
    def is_housing_location(self) -> bool:
        return (
            self.is_custody_location
            or self.is_temporary_housing
            or self.is_context_dependent_treatment_facility
        )

    @property
    def is_custody_location(self) -> bool:
        return self is KnownEntityType.COMMUNITY_CORRECTIONS

    @property
    def is_temporary_housing(self) -> bool:
        return self.temporary_housing_type is not None

    @property
    def is_context_dependent_treatment_facility(self) -> bool:
        return self is KnownEntityType.RESIDENTIAL_TREATMENT

    @property
    def temporary_housing_type(self) -> str | None:
        return {
            KnownEntityType.SOBER_LIVING: "sober_living",
            KnownEntityType.HALFWAY_HOUSE: "transitional_program",
            KnownEntityType.SHELTER: "shelter",
            KnownEntityType.HOTEL_MOTEL: "hotel_motel",
        }.get(self)

    @property
    def is_employer(self) -> bool:
        return self in (KnownEntityType.STAFFING_AGENCY, KnownEntityType.EMPLOYER)

    @property
    def not_housing_description(self) -> str:
        """Human-readable description for use in the housing prompt's 'NOT housing locations' list."""
        return {
            KnownEntityType.NONRESIDENTIAL_PROGRAM: "nonresidential program; clients attend but do not live there",
            KnownEntityType.STAFFING_AGENCY: "staffing agency",
            KnownEntityType.EMPLOYER: "employer",
        }.get(self, self.value)

    @property
    def not_employer_description(self) -> str:
        """Human-readable description for use in the employment prompt's 'NOT employers' list."""
        return {
            KnownEntityType.NONRESIDENTIAL_PROGRAM: "nonresidential program (drug court, treatment, supervision)",
            KnownEntityType.RESIDENTIAL_TREATMENT: "residential or outpatient treatment facility (not employment)",
            KnownEntityType.COMMUNITY_CORRECTIONS: "DOC-supervised facility",
            KnownEntityType.SOBER_LIVING: "sober living facility",
            KnownEntityType.HALFWAY_HOUSE: "halfway house / transitional housing",
            KnownEntityType.SHELTER: "shelter",
            KnownEntityType.HOTEL_MOTEL: "hotel/motel",
        }.get(self, self.value)


@attr.define(frozen=True)
class KnownEntity:
    name: str
    entity_type: KnownEntityType
    aliases: list[str] = attr.Factory(list)
    notes: str = ""

    def display_names(self) -> str:
        """Comma-joined primary name and any aliases for use in prompt text."""
        return ", ".join([self.name] + self.aliases)


def load_known_entities(state_code: StateCode) -> list[KnownEntity]:
    """Loads known entities for a state. Returns [] if no file exists."""
    yaml_path = os.path.join(
        _KNOWN_ENTITIES_CONFIG_DIR, f"{state_code.value.lower()}.yaml"
    )
    if not Path(yaml_path).exists():
        return []

    yaml_dict = YAMLDict.from_path(yaml_path)
    raw_entities = yaml_dict.pop_optional("known_entities", list) or []

    entities = []
    for raw in raw_entities:
        entity_type_str = raw.get("entity_type", "")
        try:
            entity_type = KnownEntityType(entity_type_str)
        except ValueError as e:
            raise ValueError(
                f"Unknown entity_type '{entity_type_str}' in {yaml_path}. "
                f"Must be one of: {[t.value for t in KnownEntityType]}"
            ) from e

        entities.append(
            KnownEntity(
                name=raw["name"],
                entity_type=entity_type,
                aliases=raw.get("aliases") or [],
                notes=raw.get("notes") or "",
            )
        )
    return entities


def render_housing_known_entities_context(entities: list[KnownEntity]) -> str:
    """Renders known entities as a prompt block for housing extraction.

    Groups entities into:
    - NOT housing locations
    - Custody locations (primary_status=in_custody)
    - Temporary housing locations (with sub-type hints)
    - Mixed treatment (context-dependent: residential or outpatient)

    Returns "" if there are no relevant entities.
    """
    not_housing = [e for e in entities if not e.entity_type.is_housing_location]
    custody_location = [e for e in entities if e.entity_type.is_custody_location]
    temp_housing = [e for e in entities if e.entity_type.is_temporary_housing]
    treatment = [
        e for e in entities if e.entity_type.is_context_dependent_treatment_facility
    ]

    if not not_housing and not custody_location and not temp_housing and not treatment:
        return ""

    lines = ["KNOWN ENTITIES — USE THESE TO GUIDE CLASSIFICATION:"]

    if not_housing:
        lines.append(
            "The following are NOT housing locations "
            "(do not set primary_status or housed_type based on these):"
        )
        for entity in not_housing:
            lines.append(
                f"- {entity.display_names()} ({entity.entity_type.not_housing_description})"
            )

    if custody_location:
        lines.append(
            "The following are facilities where a person might be in custody. "
            "If a person is in or at this place currently, use "
            "primary_status=in_custody, not housed):"
        )
        for entity in custody_location:
            lines.append(
                f"- {entity.display_names()} (community corrections / DOC-supervised facility)"
            )

    if temp_housing:
        lines.append(
            "The following ARE temporary housing locations "
            "(use housed_type=temporary_housing when the person is living there):"
        )
        for entity in temp_housing:
            lines.append(
                f"- {entity.display_names()} → temporary_housing_type: {entity.entity_type.temporary_housing_type}"
            )

    if treatment:
        lines.append(
            "The following treatment organizations may be residential or outpatient. "
            "Use context to determine whether the person is living there: "
            "if the note indicates the person is admitted or residing there, use "
            "housed_type=temporary_housing, temporary_housing_type=treatment_program; "
            "if the note indicates outpatient sessions only, do not classify as housing:"
        )
        for entity in treatment:
            lines.append(f"- {entity.display_names()}")

    return "\n".join(lines)


def render_employment_known_entities_context(entities: list[KnownEntity]) -> str:
    """Renders known entities as a prompt block for employment extraction.

    Groups entities into:
    - NOT employers (programs, treatment facilities, supervision programs)
    - Known employers, with staffing agencies flagged as employment_type: temp_agency

    Returns "" if there are no relevant entities.
    """
    not_employers = [e for e in entities if not e.entity_type.is_employer]
    known_employers = [e for e in entities if e.entity_type.is_employer]

    if not not_employers and not known_employers:
        return ""

    lines = ["KNOWN ENTITIES — USE THESE TO GUIDE CLASSIFICATION:"]

    if not_employers:
        lines.append(
            "The following are NOT employers "
            "(do not extract as employment, even if mentioned in the note):"
        )
        for entity in not_employers:
            lines.append(
                f"- {entity.display_names()} ({entity.entity_type.not_employer_description})"
            )

    if known_employers:
        lines.append(
            "The following are known employers with a specific organization type:"
        )
        for entity in known_employers:
            if entity.entity_type is KnownEntityType.STAFFING_AGENCY:
                lines.append(
                    f"- {entity.display_names()} → employment_type: temp_agency"
                )
            else:
                lines.append(f"- {entity.display_names()}")

    return "\n".join(lines)
