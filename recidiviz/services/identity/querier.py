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
"""Data-access layer between the Identity Service Flask routes and the
Identity Postgres database. Methods return typed domain objects.
"""
import uuid
from collections import defaultdict

from sqlalchemy import case
from sqlalchemy.orm import Session

from recidiviz.common.constants.identity import IdentifierType, IdentityStatus
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.services.identity import types
from recidiviz.services.identity.resolution_helpers import resolve_surviving_ids


def _to_external_id(row: schema.ExternalId) -> types.ExternalId:
    return types.ExternalId(
        external_id=row.external_id,
        id_type=row.id_type,
        is_active=row.is_active,
    )


def _to_sourced_name(row: schema.Name) -> types.SourcedAttributeValue:
    return types.SourcedAttributeValue(
        value=types.Name(
            surname=row.surname,
            given_name=row.given_name,
            middle_names=list(row.middle_names),
            name_suffix=row.name_suffix,
            use=row.use,
        ),
        source_type=row.source_type,
        source_product_app=row.source_product_app,
        last_updated_utc=row.last_updated_utc,
    )


def _to_sourced_date_of_birth(row: schema.DateOfBirth) -> types.SourcedAttributeValue:
    return types.SourcedAttributeValue(
        value=types.DateOfBirth(
            date=row.date,
            canonical=row.canonical,
            canonical_locked=row.canonical_locked,
        ),
        source_type=row.source_type,
        source_product_app=row.source_product_app,
        last_updated_utc=row.last_updated_utc,
    )


def _to_sourced_gender(row: schema.Gender) -> types.SourcedAttributeValue:
    return types.SourcedAttributeValue(
        value=types.Gender(
            gender=row.gender,
            canonical=row.canonical,
            canonical_locked=row.canonical_locked,
        ),
        source_type=row.source_type,
        source_product_app=row.source_product_app,
        last_updated_utc=row.last_updated_utc,
    )


def _to_sourced_race(row: schema.Race) -> types.SourcedAttributeValue:
    return types.SourcedAttributeValue(
        value=types.Race(race=row.race),
        source_type=row.source_type,
        source_product_app=row.source_product_app,
        last_updated_utc=row.last_updated_utc,
    )


def _to_sourced_sex(row: schema.Sex) -> types.SourcedAttributeValue:
    return types.SourcedAttributeValue(
        value=types.Sex(
            sex=row.sex,
            canonical=row.canonical,
            canonical_locked=row.canonical_locked,
        ),
        source_type=row.source_type,
        source_product_app=row.source_product_app,
        last_updated_utc=row.last_updated_utc,
    )


def _to_sourced_ethnicity(row: schema.Ethnicity) -> types.SourcedAttributeValue:
    return types.SourcedAttributeValue(
        value=types.Ethnicity(
            ethnicity=row.ethnicity,
            canonical=row.canonical,
            canonical_locked=row.canonical_locked,
        ),
        source_type=row.source_type,
        source_product_app=row.source_product_app,
        last_updated_utc=row.last_updated_utc,
    )


def _to_sourced_phone_number(row: schema.PhoneNumber) -> types.SourcedAttributeValue:
    return types.SourcedAttributeValue(
        value=types.PhoneNumber(
            number=row.number,
            type=row.type,
            preferred=row.preferred,
        ),
        source_type=row.source_type,
        source_product_app=row.source_product_app,
        last_updated_utc=row.last_updated_utc,
    )


def _to_sourced_email(row: schema.Email) -> types.SourcedAttributeValue:
    return types.SourcedAttributeValue(
        value=types.Email(address=row.address, address_hash=row.address_hash),
        source_type=row.source_type,
        source_product_app=row.source_product_app,
        last_updated_utc=row.last_updated_utc,
    )


def _to_identity(row: schema.Identity) -> types.Identity:
    """Builds a domain Identity from an ORM row, reading its eagerly-loaded
    child collections."""
    return types.Identity(
        recidiviz_id=row.recidiviz_id,
        tenant=row.tenant,
        person_type=row.person_type,
        status=row.status,
        merged_into=row.merged_into,
        last_cluster_hash=row.last_cluster_hash,
        skip_demographic_guard=row.skip_demographic_guard,
        created_utc=row.created_utc,
        last_updated_utc=row.last_updated_utc,
        external_ids=[_to_external_id(e) for e in row.external_ids],
        attributes=types.IdentityAttributes(
            names=[_to_sourced_name(n) for n in row.names],
            dates_of_birth=[_to_sourced_date_of_birth(d) for d in row.dates_of_birth],
            genders=[_to_sourced_gender(g) for g in row.genders],
            races=[_to_sourced_race(r) for r in row.races],
            sexes=[_to_sourced_sex(s) for s in row.sexes],
            ethnicities=[_to_sourced_ethnicity(e) for e in row.ethnicities],
            phone_numbers=[_to_sourced_phone_number(p) for p in row.phone_numbers],
            emails=[_to_sourced_email(e) for e in row.emails],
        ),
    )


def _to_attribute_conflict(row: schema.AttributeConflict) -> types.AttributeConflict:
    return types.AttributeConflict(
        attribute_type=row.attribute_type,
        retired_value=types.SourcedAttributeValue.from_dict(
            row.retired_value, attribute_type=row.attribute_type
        ),
        surviving_value=types.SourcedAttributeValue.from_dict(
            row.surviving_value, attribute_type=row.attribute_type
        ),
    )


def _to_merge_event(row: schema.MergeEvent) -> types.MergeEvent:
    return types.MergeEvent(
        surviving_id=row.surviving_id,
        retired_id=row.retired_id,
        trigger=row.trigger,
        requested_by=row.requested_by,
        timestamp_utc=row.timestamp_utc,
        conflicts=[_to_attribute_conflict(c) for c in row.conflicts],
    )


def _to_split_event(row: schema.SplitEvent) -> types.SplitEvent:
    # The moved external IDs and attributes are split across destinations by
    # new_recidiviz_id, so group them by destination before assembling.
    moved_external_ids: defaultdict[
        uuid.UUID, list[schema.SplitEventMovedExternalId]
    ] = defaultdict(list)
    for moved_external_id in row.moved_external_ids:
        moved_external_ids[moved_external_id.new_recidiviz_id].append(moved_external_id)

    moved_attributes: defaultdict[
        uuid.UUID, list[schema.SplitEventMovedAttribute]
    ] = defaultdict(list)
    for moved_attribute in row.moved_attributes:
        moved_attributes[moved_attribute.new_recidiviz_id].append(moved_attribute)

    return types.SplitEvent(
        original_id=row.original_id,
        trigger=row.trigger,
        requested_by=row.requested_by,
        timestamp_utc=row.timestamp_utc,
        destinations=[
            types.SplitDestination(
                new_recidiviz_id=new_identity.new_recidiviz_id,
                external_ids=[
                    types.ExternalId(
                        external_id=moved.external_id,
                        id_type=moved.id_type,
                        # The moved-external-id audit table records no is_active
                        # flag; a moved ID is active on its new identity.
                        is_active=True,
                    )
                    for moved in moved_external_ids[new_identity.new_recidiviz_id]
                ],
                attributes=[
                    types.SourcedAttributeValue.from_dict(
                        moved.attribute_value, attribute_type=moved.attribute_type
                    )
                    for moved in moved_attributes[new_identity.new_recidiviz_id]
                ],
            )
            for new_identity in row.new_identities
        ],
    )


def _resolve_surviving_identities(
    session: Session, recidiviz_ids: list[uuid.UUID]
) -> dict[uuid.UUID, types.Identity | None]:
    """Returns a mapping from each id in `recidiviz_ids` to its surviving ACTIVE
    Identity, or None if the id does not exist.
    Raises IdentityHistoryIntegrityException if a merged_into hop references a
    nonexistent record, or if a chain contains a cycle."""
    surviving_id_by_input = resolve_surviving_ids(session, recidiviz_ids)
    surviving_ids = set(filter(None, surviving_id_by_input.values()))
    surviving_rows = (
        session.query(schema.Identity)
        .filter(schema.Identity.recidiviz_id.in_(surviving_ids))
        .all()
        if surviving_ids
        else []
    )
    identity_by_id = {row.recidiviz_id: _to_identity(row) for row in surviving_rows}
    return {
        input_id: identity_by_id[surviving_id] if surviving_id is not None else None
        for input_id, surviving_id in surviving_id_by_input.items()
    }


class IdentityServiceQuerier:
    """Implements Querier abstractions for the Identity Service data source."""

    @property
    def database_key(self) -> SQLAlchemyDatabaseKey:
        return SQLAlchemyDatabaseKey.for_schema(SchemaType.IDENTITY)

    def get_identity(
        self, recidiviz_id: uuid.UUID, *, resolve_retired: bool
    ) -> types.Identity | None:
        """Returns the identity for the given recidiviz_id, or None if not found.

        When `resolve_retired` is True and the given recidiviz_id has been retired
        (merged into another record), this follows the `merged_into` chain
        (possibly multiple hops) and returns the surviving Identity instead. When
        False, the record is returned as stored, even if it is RETIRED.

        Child attributes and external IDs are eagerly loaded via the `selectin`
        relationships on `schema.Identity`.
        """
        with SessionFactory.using_database(self.database_key) as session:
            if resolve_retired:
                return _resolve_surviving_identities(session, [recidiviz_id])[
                    recidiviz_id
                ]
            identity_row = (
                session.query(schema.Identity)
                .filter(schema.Identity.recidiviz_id == recidiviz_id)
                .one_or_none()
            )
            if identity_row is None:
                return None
            return _to_identity(identity_row)

    def get_identity_history(self, identity: types.Identity) -> types.IdentityHistory:
        """Returns the given identity paired with its merge and split audit history.

        The caller is responsible for resolving retired records first (e.g. via
        `get_identity(..., resolve_retired=True)`); this reads the audit events
        recorded against `identity.recidiviz_id`.
        """
        with SessionFactory.using_database(self.database_key) as session:
            merge_event_rows = (
                session.query(schema.MergeEvent)
                .filter(schema.MergeEvent.surviving_id == identity.recidiviz_id)
                .all()
            )
            split_event_rows = (
                session.query(schema.SplitEvent)
                .filter(schema.SplitEvent.original_id == identity.recidiviz_id)
                .all()
            )
            return types.IdentityHistory(
                identity=identity,
                merge_events=[_to_merge_event(row) for row in merge_event_rows],
                split_events=[_to_split_event(row) for row in split_event_rows],
            )

    def get_by_external_id(
        self, external_id: str, id_type: IdentifierType
    ) -> types.Identity | None:
        """Returns the active identity for the given external ID and ID type, or
        None if no active (external_id, id_type) pair exists.

        If the identity attached to the external ID is RETIRED, follows the
        merged_into chain and returns the surviving ACTIVE identity.
        """
        with SessionFactory.using_database(self.database_key) as session:
            row = (
                session.query(schema.ExternalId.recidiviz_id)
                .filter(
                    schema.ExternalId.external_id == external_id,
                    schema.ExternalId.id_type == id_type,
                    schema.ExternalId.is_active.is_(True),
                )
                .one_or_none()
            )
        if row is None:
            return None
        return self.get_identity(row.recidiviz_id, resolve_retired=True)

    def get_by_email_hash(
        self, email_hash: str, tenant: Tenant
    ) -> types.Identity | None:
        """Returns the active identity for the given email_hash and tenant, or None.

        Queries Email by address_hash joined to Identity by tenant. Orders ACTIVE
        identities first so that the common path (email on an active record) avoids
        the merged_into chain walk. If the owning identity is RETIRED, follows
        merged_into to the surviving ACTIVE record.
        """
        with SessionFactory.using_database(self.database_key) as session:
            row = (
                session.query(schema.Email.recidiviz_id, schema.Identity.status)
                .join(
                    schema.Identity,
                    schema.Email.recidiviz_id == schema.Identity.recidiviz_id,
                )
                .filter(
                    schema.Email.address_hash == email_hash,
                    schema.Identity.tenant == tenant,
                )
                .order_by(
                    case(
                        (schema.Identity.status == IdentityStatus.ACTIVE, 0),
                        else_=1,
                    ),
                    schema.Identity.recidiviz_id,
                )
                # address_hash is not DB-unique; ordering makes the result deterministic when hashes collide.
                .first()
            )
        if row is None:
            return None
        return self.get_identity(
            row.recidiviz_id,
            resolve_retired=row.status is not IdentityStatus.ACTIVE,
        )
