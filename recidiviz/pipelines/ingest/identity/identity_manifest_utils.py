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
"""Helpers for reading static properties out of identity ingest view manifests."""
from recidiviz.common.constants.identity import PersonType
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    EnumLiteralFieldManifest,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifest,
)
from recidiviz.utils.types import assert_type

_PERSON_TYPE_FIELD_NAME = "person_type"


def get_view_person_type(manifest: IngestViewManifest) -> PersonType:
    """Returns the single `PersonType` that every fragment produced by this
    identity view carries, read from the view's compiled manifest.

    Identity views must author `person_type` as a literal enum (e.g.
    `$literal_enum(PersonType.JII)`) at the top level of the mapping so that a
    view produces fragments of a single, statically known person type. Raises if
    a view maps `person_type` as anything else (e.g. a data-driven
    `$enum_mapping`) or omits it entirely."""
    person_type_manifest = manifest.output.field_manifests.get(_PERSON_TYPE_FIELD_NAME)
    if not isinstance(person_type_manifest, EnumLiteralFieldManifest):
        raise ValueError(
            f"Identity view [{manifest.ingest_view_name}] must author "
            f"[{_PERSON_TYPE_FIELD_NAME}] as a literal enum (e.g. "
            f"$literal_enum(PersonType.JII)) so that the view produces fragments "
            f"of a single person type, but found "
            f"[{type(person_type_manifest)}]."
        )
    return assert_type(person_type_manifest.enum_value, PersonType)
