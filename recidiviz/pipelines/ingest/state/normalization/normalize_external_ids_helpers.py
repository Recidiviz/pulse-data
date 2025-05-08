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
"""Helpers for normalizing external id entities."""
from recidiviz.persistence.entity.state.entities import StatePersonExternalId


def select_alphabetically_highest_person_external_id(
    external_ids: list[StatePersonExternalId],
) -> StatePersonExternalId:
    """Given a list of StatePersonExternalId, returns the StatePersonExternalId with
    the external_id that is alphabetically highest.

    NOTE: If you are attempting to sort external_ids that can all be parsed as integers,
    this is not likely the function you want to use (i.e. because "10" will sort before
    "9")

    Throws if given an empty list.
    """
    if not external_ids:
        raise ValueError(
            "Cannot call select_alphabetically_highest_person_external_id() with an "
            "empty external_ids list"
        )
    id_types = {pei.id_type for pei in external_ids}
    if len(id_types) > 1:
        raise ValueError(
            f"Found multiple id_types in the provided external_ids list: "
            f"{sorted(id_types)}. Expected external_ids only of a single type."
        )

    found_external_ids = set()
    for pei in external_ids:
        if pei.external_id in found_external_ids:
            raise ValueError(
                f"Found multiple external ids with external_id [{pei.external_id}] "
                f"and id_type [{list(id_types)[0]}]. These objects should be merged at "
                f"this point."
            )
        found_external_ids.add(pei.external_id)

    return list(reversed(sorted(external_ids, key=lambda pei: pei.external_id)))[0]
