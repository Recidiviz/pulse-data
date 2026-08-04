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
"""The name shared by an entity-resolution composite-document collection and the
ER extractor collection that reads it, derived from the (first-order extractor
collection, entity group) pair they resolve entities for.

Kept in its own dependency-free module so the ER configs and the result views can
both import the naming rule without importing each other.
"""

# Suffix appended to the first-order extractor collection name (plus the entity
# group name) to form the composite-document collection name.
ENTITY_RESOLUTION_COLLECTION_NAME_SUFFIX = "ENTITY_RESOLUTION"


def entity_resolution_collection_name(
    *, first_order_extractor_collection_name: str, entity_group_name: str
) -> str:
    """Returns the composite-document collection name for a (first-order extractor
    collection, entity group) pair, e.g.
    CASE_NOTE_EMPLOYMENT_INFO_EMPLOYER_ENTITY_RESOLUTION.

    The ER extractor collection shares this name with the composite-document
    collection, so a state's ER extractor binds one as its input document
    collection and the other as its extractor collection.
    """
    return (
        f"{first_order_extractor_collection_name}_{entity_group_name.upper()}"
        f"_{ENTITY_RESOLUTION_COLLECTION_NAME_SUFFIX}"
    )
