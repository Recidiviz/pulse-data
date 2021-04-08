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
"""Contains utils for match database entities with ingested entities."""
import datetime
import logging
from typing import Optional, Sequence, Callable, Iterable, Set, Dict, Any, cast, List

import deepdiff
from more_itertools import pairwise

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity_matching.entity_matching_utils import get_all_matches
from recidiviz.persistence.persistence_utils import is_booking_active

_CHARGE_MATCH_FIELDS = {
    "offense_date",
    "statute",
    "name",
    "attempted",
    "degree_raw_text",
    "class_raw_text",
    "level",
    "fee_dollars",
    "charging_entity",
    "court_type",
    "case_number",
}

_HOLD_MATCH_FIELDS = {
    "jurisdiction_name",
}

_BOND_MATCH_FIELDS = {
    "amount_dollars",
    "bond_type_raw_text",
    "bond_agent",
}

_SENTENCE_MATCH_FIELDS = {
    "sentencing_region",
    "min_length_days",
    "max_length_days",
    "date_imposed",
    "is_life",
    "is_probation",
    "is_suspended",
    "fine_dollars",
    "parole_possible",
    "post_release_supervision_length_days",
}


def is_person_match(
    *, db_entity: entities.Person, ingested_entity: entities.Person
) -> bool:
    """
    Given a database person and an ingested person, determine if they should be
    considered the same person.
    Args:
        db_entity: (entities.Person)
        ingested_entity: (entities.Person)
    Returns: (bool)
    """
    if db_entity.external_id or ingested_entity.external_id:
        return db_entity.external_id == ingested_entity.external_id

    if not all([db_entity.full_name, ingested_entity.full_name]):
        return False

    return (
        db_entity.full_name == ingested_entity.full_name
        and is_birthdate_match(db_entity, ingested_entity)
        and _db_open_booking_matches_ingested_booking(
            db_entity=db_entity, ingested_entity=ingested_entity
        )
    )


def _db_open_booking_matches_ingested_booking(
    *, db_entity: entities.Person, ingested_entity: entities.Person
) -> bool:
    """Returns True if the external id on the open booking in the database
    matches any of the external ids of the bookings on the ingested person.
    If there is no open booking in the db, return True as well.

    Note: if the same person has been rebooked on subsequent scrapes, and the
    ingested person doesn't have historical bookings, we will not match the
    person entities. This is the same behavior as if the person is rebooked on
    non-consecutive days.
    """
    db_open_bookings = [b for b in db_entity.bookings if is_booking_active(b)]
    if not db_open_bookings:
        return True
    return any(
        db_open_booking.external_id == ingested_booking.external_id
        for ingested_booking in ingested_entity.bookings
        for db_open_booking in db_open_bookings
    )


# '*' catches positional arguments, making our arguments named and required.
def is_booking_match(
    *, db_entity: entities.Booking, ingested_entity: entities.Booking
) -> bool:
    """
    Given a database booking and an ingested booking, determine if they should
    be considered the same booking. Should only be used to compare bookings for
    the same person.
    Args:
        db_entity: (entities.Booking)
        ingested_entity: (entities.Booking)
    Returns: (bool)
    """
    if db_entity.external_id or ingested_entity.external_id:
        return db_entity.external_id == ingested_entity.external_id

    # If the db booking's admission date was scraped (not inferred), the
    # ingested booking must have the same admission date to be a match.
    if not db_entity.admission_date_inferred:
        return db_entity.admission_date == ingested_entity.admission_date
    return is_booking_active(db_entity) and is_booking_active(ingested_entity)


# '*' catches positional arguments, making our arguments named and required.
def is_hold_match(*, db_entity: entities.Hold, ingested_entity: entities.Hold) -> bool:
    """
    Given a database hold and an ingested hold, determine if they should
    be considered the same hold. Should only be used to compare holds for
    the same booking.
    Args:
        db_entity: (entities.Hold)
        ingested_entity: (entities.Hold)
    Returns: (bool)
    """
    return is_match(db_entity, ingested_entity, _HOLD_MATCH_FIELDS)


def is_charge_match_with_children(
    *, db_entity: entities.Charge, ingested_entity: entities.Charge
) -> bool:
    sentences_match = is_sentence_match(
        db_entity=db_entity.sentence, ingested_entity=ingested_entity.sentence
    )
    bonds_match = is_bond_match(
        db_entity=db_entity.bond, ingested_entity=ingested_entity.bond
    )

    return (
        sentences_match
        and bonds_match
        and is_charge_match(db_entity=db_entity, ingested_entity=ingested_entity)
    )


# '*' catches positional arguments, making our arguments named and required.
def is_charge_match(
    *, db_entity: entities.Charge, ingested_entity: entities.Charge
) -> bool:
    """
    Given a database charge and an ingested charge, determine if they should be
    considered the same charge. Should only be used to compare charges for the
    same booking.
    Args:
        db_entity: (entities.Charge)
        ingested_entity: (entities.Charge)
    Returns: (bool)
    """
    return is_match(db_entity, ingested_entity, _CHARGE_MATCH_FIELDS)


# '*' catches positional arguments, making our arguments named and required.
def is_bond_match(
    *, db_entity: Optional[entities.Bond], ingested_entity: Optional[entities.Bond]
) -> bool:
    """
    Given a database bond and an ingested bond, determine if they should be
    considered the same bond. Should only be used to compare bonds for the same
    charges.
    Args:
        db_entity: (entities.Bond)
        ingested_entity: (entities.Bond)
    Returns: (bool)
    """
    return is_match(db_entity, ingested_entity, _BOND_MATCH_FIELDS)


# '*' catches positional arguments, making our arguments named and required.
def is_sentence_match(
    *,
    db_entity: Optional[entities.Sentence],
    ingested_entity: Optional[entities.Sentence]
) -> bool:
    """
    Given a database sentence and an ingested sentence, determine if they
    should be considered the same sentence. Should only be used to compare
    sentences for the same charge.
    Args:
        db_entity: (entities.Sentence)
        ingested_entity: (entities.Sentence)
    Returns: (bool)
    """
    return is_match(db_entity, ingested_entity, _SENTENCE_MATCH_FIELDS)


def diff_count(entity_a: Entity, entity_b: Entity) -> int:
    """Counts the number of differences between two entities, including
    their descendants."""
    ddiff = deepdiff.DeepDiff(
        entity_a, entity_b, ignore_order=True, report_repetition=True
    )
    diff_types = (
        "values_changed",
        "type_changes",
        "iterable_item_added",
        "iterable_item_removed",
        "repetition_change",
    )
    if not any(diff_type in ddiff for diff_type in diff_types):
        logging.warning(
            "DeepDiff did not return any of the expected diff "
            "report fields. Maybe the API changed?\nDiff output:%s",
            ddiff,
        )

    return sum(
        len(diffs) for diff_type, diffs in ddiff.items() if diff_type in diff_types
    )


def get_best_match(
    ingest_entity: Entity,
    db_entities: Sequence[Entity],
    matcher: Callable,
    matched_db_ids: Iterable[int],
) -> Optional[Entity]:
    """
    Selects the database entity that most closely matches the ingest entity,
    if a match exists. The steps are as follows:
        - Use |matcher| to select a list of candidate matches
        - Disqualify previously matched entities in |matched_db_ids|
        - Select the candidate match that differs minimally from the ingested
          entity.
    """
    matches = cast(List[Entity], get_all_matches(ingest_entity, db_entities, matcher))
    matches = [m for m in matches if m.get_id() not in matched_db_ids]
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    logging.info(
        "Using diff_count to pick best match: Multiple matches found for a "
        "single ingested person.\nIngested entity: %s\nDatabase matches:%s",
        ingest_entity,
        matches,
    )
    return min(matches, key=lambda db_entity: diff_count(ingest_entity, db_entity))


def is_birthdate_match(a: EntityPersonType, b: EntityPersonType) -> bool:
    if a.birthdate_inferred_from_age and b.birthdate_inferred_from_age:
        return _is_inferred_birthdate_match(a.birthdate, b.birthdate)

    if not a.birthdate_inferred_from_age and not b.birthdate_inferred_from_age:
        return a.birthdate == b.birthdate

    return False


def _is_inferred_birthdate_match(
    a: Optional[datetime.date], b: Optional[datetime.date]
) -> bool:
    if not a or not b:
        return False
    return abs(a.year - b.year) <= 1


def is_match(
    db_entity: Optional[ExternalIdEntity],
    ingested_entity: Optional[ExternalIdEntity],
    match_fields: Set[str],
) -> bool:
    if not db_entity or not ingested_entity:
        return db_entity == ingested_entity

    if db_entity.external_id or ingested_entity.external_id:
        return db_entity.external_id == ingested_entity.external_id

    return all(
        getattr(db_entity, field) == getattr(ingested_entity, field)
        for field in match_fields
    )


def get_next_available_match(
    ingested_entity: Entity,
    db_entities: Sequence[Entity],
    db_entities_matched_by_id: Dict[int, Any],
    matcher: Callable,
) -> Optional[Entity]:
    """
    Finds all |db_entities| that match the provided |ingested_entity| based on
    the |matcher| function, and returns the first of these matches that has not
    already been matched (based on the provided |db_entities_matched_by_id|.
    """
    for db_entity in db_entities:
        if not db_entity.get_id() in db_entities_matched_by_id and matcher(
            db_entity=db_entity, ingested_entity=ingested_entity
        ):
            return db_entity
    return None


def close_multiple_open_bookings(bookings: List[entities.Booking]) -> None:
    """
    Assigns all open |bookings| a release date which is the next booking's
    admission date by modifying bookings in place.
    """
    for b1, b2 in pairwise(
        sorted(bookings, key=lambda b: b.admission_date or datetime.date.max)
    ):
        if not b1.release_date:
            b1.release_date = b2.admission_date
            b1.release_date_inferred = True
            b1.custody_status = CustodyStatus.REMOVED_WITHOUT_INFO


def generate_id_from_obj(obj: Entity) -> str:
    return str(id(obj)) + "_ENTITY_MATCHING_GENERATED"
