# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Validates that ingest_info protos conform to the schema requirements."""
from typing import Iterable, Dict, Set, Any

import iteration_utilities

from recidiviz.ingest.models import ingest_info_pb2

DUPLICATES = 'duplicate_ids'
NON_EXISTING_IDS = 'ids_referenced_that_do_not_exist'
EXTRA_IDS = 'ids_never_referenced'


class ValidationError(Exception):
    """Raised when encountering an error with ingest_info validation."""

    def __init__(self, errors: Dict[str, Dict[str, Set]]):
        # TODO(#1261): Return errors to caller instead of printing in exception
        super().__init__(errors)
        self.errors = errors


def validate(ingest_info: ingest_info_pb2.IngestInfo) -> None:
    """Validates that the ingest_info is correctly structured."""
    errors = {
        'people': _person_errors(ingest_info),
        'bookings': _booking_errors(ingest_info),
        'arrests': _arrest_errors(ingest_info),
        'charges': _charge_errors(ingest_info),
        'sentences': _sentence_errors(ingest_info),
        'bonds': _bond_errors(ingest_info),

        'state_people': _state_person_errors(ingest_info),
    }

    errors = _trim_all_empty_errors(errors)

    if errors:
        raise ValidationError(errors)


def _person_errors(ingest_info: ingest_info_pb2.IngestInfo) -> Dict[str, Set]:
    return {
        DUPLICATES: _get_duplicates(
            person.person_id for person in ingest_info.people)
    }


def _booking_errors(ingest_info: ingest_info_pb2.IngestInfo) -> Dict[str, Set]:
    booking_ids = {booking.booking_id for booking in ingest_info.bookings}
    referenced_booking_ids = set(iteration_utilities.flatten(
        person.booking_ids for person in ingest_info.people))

    return {
        DUPLICATES: _get_duplicates(
            booking.booking_id for booking in ingest_info.bookings),
        NON_EXISTING_IDS: referenced_booking_ids - booking_ids,
        EXTRA_IDS: booking_ids - referenced_booking_ids
    }


def _arrest_errors(ingest_info: ingest_info_pb2.IngestInfo) -> Dict[str, Set]:
    arrest_ids = {arrest.arrest_id for arrest in ingest_info.arrests}
    referenced_arrest_ids = {booking.arrest_id for booking in
                             ingest_info.bookings if
                             booking.HasField('arrest_id')}

    return {
        DUPLICATES: _get_duplicates(
            arrest.arrest_id for arrest in ingest_info.arrests),
        NON_EXISTING_IDS: referenced_arrest_ids - arrest_ids,
        EXTRA_IDS: arrest_ids - referenced_arrest_ids
    }


def _charge_errors(ingest_info: ingest_info_pb2.IngestInfo) -> Dict[str, Set]:
    charge_ids = {charge.charge_id for charge in ingest_info.charges}
    referenced_charge_ids = set(iteration_utilities.flatten(
        booking.charge_ids for booking in ingest_info.bookings))

    return {
        DUPLICATES: _get_duplicates(
            charge.charge_id for charge in ingest_info.charges),
        NON_EXISTING_IDS: referenced_charge_ids - charge_ids,
        EXTRA_IDS: charge_ids - referenced_charge_ids
    }


def _sentence_errors(ingest_info: ingest_info_pb2.IngestInfo) -> Dict[str, Set]:
    sentence_ids = {sentence.sentence_id for sentence in ingest_info.sentences}
    referenced_sentence_ids = {charge.sentence_id for charge in
                               ingest_info.charges if
                               charge.HasField('sentence_id')}

    return {
        DUPLICATES: _get_duplicates(
            sentence.sentence_id for sentence in ingest_info.sentences),
        NON_EXISTING_IDS: referenced_sentence_ids - sentence_ids,
        EXTRA_IDS: sentence_ids - referenced_sentence_ids
    }


def _bond_errors(ingest_info: ingest_info_pb2.IngestInfo) -> Dict[str, Set]:
    bond_ids = {bond.bond_id for bond in ingest_info.bonds}
    referenced_bond_ids = {charge.bond_id for charge in
                           ingest_info.charges if
                           charge.HasField('bond_id')}

    return {
        DUPLICATES: _get_duplicates(
            bond.bond_id for bond in ingest_info.bonds),
        NON_EXISTING_IDS: referenced_bond_ids - bond_ids,
        EXTRA_IDS: bond_ids - referenced_bond_ids
    }


def _state_person_errors(ingest_info: ingest_info_pb2.IngestInfo) -> Dict[str, Set]:
    return {
        DUPLICATES:
            _get_duplicates(state_person.state_person_id
                            for state_person in ingest_info.state_people)
    }


def _get_duplicates(collection: Iterable) -> Set:
    """Returns a set of all elements duplicated in the collection.

    Example: [1,2,3,3,3,4,4] -> {3,4}
    """
    return set(iteration_utilities.duplicates(collection))


def _trim_all_empty_errors(all_errors: Dict[str, Dict[str, Set]]) -> Dict[str, Dict[str, Set]]:
    """Trims every empty value in the provided dict of all_errors."""
    trimmed_errors = {name: _trim_empty(error) for name, error in
                      all_errors.items()}
    return _trim_empty(trimmed_errors)


def _trim_empty(dictionary: Dict[str, Any]) -> Dict[str, Any]:
    """Returns the dict with all keys that point to an empty set removed.

    Example: {a: {1,2}, b: {}} -> {a: {1, 2}}
    """
    return {k: v for k, v in dictionary.items() if len(v) > 0}
