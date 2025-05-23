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
import datetime

from more_itertools import one

from recidiviz.persistence.entity.state.entities import StatePersonExternalId


def select_most_recently_active_person_external_id(
    external_ids: list[StatePersonExternalId],
    enforce_nonnull_id_active_from: bool = True,
) -> StatePersonExternalId:
    """Given a list of StatePersonExternalId all with the same id_type, returns the
    StatePersonExternalId that was active most recently, using the
    (id_active_from_datetime and id_active_to_datetime) dates.

    Throws if given an empty list.
    """
    if not external_ids:
        raise ValueError(
            "Cannot call select_most_recently_active_person_external_id() with an "
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

    def sort_key(
        e: StatePersonExternalId,
    ) -> tuple[datetime.datetime, datetime.datetime | None, str]:
        if not enforce_nonnull_id_active_from and e.id_active_from_datetime is None:
            id_active_from_datetime = datetime.datetime.min
        else:
            if e.id_active_from_datetime is None:
                raise ValueError(
                    f"Found null id_active_from_datetime value on external_id "
                    f"[{e.limited_pii_repr()}]."
                )
            id_active_from_datetime = e.id_active_from_datetime

        return (
            (e.id_active_to_datetime or datetime.datetime.max),
            id_active_from_datetime,
            # If all the dates are the same, sort by external id
            e.external_id,
        )

    return list(reversed(sorted(external_ids, key=sort_key)))[0]


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


def select_single_external_id_with_is_current_display_id(
    external_ids: list[StatePersonExternalId],
) -> StatePersonExternalId:
    """Given a list of StatePersonExternalId all with the same id_type, returns the
    single StatePersonExternalId with the external_id that has
    is_current_display_id_for_type=True.

    Throws if:
    * Given an empty list
    * Not all external ids have the same id_type
    * There are multiple external ids with the same external_id value.
    * Any is_current_display_id_for_type value is None
    * More than one external id has is_current_display_id_for_type=True
    """
    if not external_ids:
        raise ValueError(
            "Cannot call select_single_external_id_with_is_current_display_id() with "
            "an empty external_ids list"
        )

    id_types = {pei.id_type for pei in external_ids}
    if len(id_types) > 1:
        raise ValueError(
            f"Found multiple id_types in the provided external_ids list: "
            f"{sorted(id_types)}. Expected external_ids only of a single type."
        )
    id_type = one(id_types)

    found_external_ids = set()
    for pei in external_ids:
        if pei.external_id in found_external_ids:
            raise ValueError(
                f"Found multiple external ids with external_id [{pei.external_id}] "
                f"and id_type [{list(id_types)[0]}]. These objects should be merged at "
                f"this point."
            )
        found_external_ids.add(pei.external_id)

    all_have_is_display_id_flags_set = all(
        pei.is_current_display_id_for_type is not None for pei in external_ids
    )

    if not all_have_is_display_id_flags_set:
        raise ValueError(
            f"Found person who has at least one StatePersonExternalId with a null "
            f"is_current_display_id_for_type value. If you are going to rely on "
            f"directly hydrated is_current_display_id_for_type values, you must hydrate "
            f"it for ALL external ids of this type ({id_type}). External ids: "
            f"{external_ids}"
        )

    display_ids = [pei for pei in external_ids if pei.is_current_display_id_for_type]
    if len(display_ids) > 1:
        raise ValueError(
            f"Found more than one external_id with is_current_display_id_for_type=True. "
            f"Either update ingest logic to ensure there is only one "
            f"is_current_display_id_for_type=True value per merged person OR implement "
            f"custom logic to select the external id that is the display id. External "
            f"ids: {external_ids}"
        )

    if not display_ids:
        raise ValueError(
            "Did not find any external_id with is_current_display_id_for_type=True. "
            "Either update ingest logic to ensure there is exactly one "
            "is_current_display_id_for_type=True value per merged person OR implement "
            f"custom logic to select the external id that is the display id. External "
            f"ids: {external_ids}"
        )
    return one(display_ids)
