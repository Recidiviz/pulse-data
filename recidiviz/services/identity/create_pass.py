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
"""Create pass of the Identity Service import: builds the rows for a new
identity from a cluster.

Builds an Identity row with a fresh Recidiviz ID plus one attribute row per
cluster value. All attribute rows carry source_type EXTERNAL_DATA_SYSTEM because
the cluster's values come from the identity ingest pipeline; canonical selection
is trivial while that is the only source type writing to the service, so each
single-valued attribute row is marked canonical. The caller owns the session and
decides how many identities to commit per transaction.
"""
import datetime
import logging
import uuid

from recidiviz.common.constants.identity import (
    IdentifierType,
    IdentityStatus,
    NameUse,
    SourceType,
)
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
)
from recidiviz.services.identity.bq_snapshot_reader import ClusterSnapshot
from recidiviz.utils.user_hash import normalized_email_hash


def build_identity_rows(
    *,
    snapshot: ClusterSnapshot,
    now: datetime.datetime,
    committed_email_hashes: set[str],
    staged_email_hashes: set[str],
) -> list[schema.IdentityBase]:
    """Returns all ORM rows for a new identity built from the cluster snapshot.

    The rows are the Identity plus one row per external id and attribute value;
    the caller adds them to a session and controls the transaction boundary. The
    identity stores the pipeline's cluster_hash as last_cluster_hash so a future
    idempotency check (OBT-37725) can skip the cluster on the next import.

    TODO(OBT-50191): POST /identities creates identities through
    IdentityServiceQuerier.create_identity and the domain types instead of
    building ORM rows directly. Unify the two creation paths without giving up
    this pass's chunked commits and in-memory email dedupe.

    An email whose address hash is already committed or staged is excluded, since
    an address is expected to reach at most one person.

    Args:
        snapshot: The cluster to build rows for, carrying the pipeline's hash.
        now: Timestamp stamped on the identity and its attribute rows.
        committed_email_hashes: Hashes of emails written in prior transactions;
            read only here.
        staged_email_hashes: Hashes used earlier in this transaction; this
            function adds each email it keeps, and the caller folds these into
            committed_email_hashes once the transaction commits.
    """
    cluster = snapshot.cluster
    recidiviz_id = uuid.uuid4()
    rows: list[schema.IdentityBase] = [
        schema.Identity(
            recidiviz_id=recidiviz_id,
            created_utc=now,
            last_updated_utc=now,
            tenant=cluster.tenant,
            person_type=cluster.person_type,
            status=IdentityStatus.ACTIVE,
            merged_into=None,
            last_cluster_hash=snapshot.stored_cluster_hash,
        )
    ]
    rows.extend(
        schema.ExternalId(
            recidiviz_id=recidiviz_id,
            external_id=external_id.external_id,
            id_type=IdentifierType(external_id.id_type),
            is_active=True,
        )
        for external_id in cluster.external_ids
    )
    rows.extend(_name_rows(cluster, recidiviz_id=recidiviz_id, now=now))
    if cluster.birthdate is not None:
        rows.append(
            schema.DateOfBirth(
                recidiviz_id=recidiviz_id,
                date=cluster.birthdate,
                canonical=True,
                canonical_locked=False,
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=now,
            )
        )
    if cluster.gender is not None:
        rows.append(
            schema.Gender(
                recidiviz_id=recidiviz_id,
                gender=cluster.gender.gender,
                canonical=True,
                canonical_locked=False,
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=now,
            )
        )
    if cluster.sex is not None:
        rows.append(
            schema.Sex(
                recidiviz_id=recidiviz_id,
                sex=cluster.sex.sex,
                canonical=True,
                canonical_locked=False,
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=now,
            )
        )
    if cluster.ethnicity is not None:
        rows.append(
            schema.Ethnicity(
                recidiviz_id=recidiviz_id,
                ethnicity=cluster.ethnicity.ethnicity,
                canonical=True,
                canonical_locked=False,
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=now,
            )
        )
    rows.extend(
        schema.Race(
            recidiviz_id=recidiviz_id,
            race=race.race,
            source_type=SourceType.EXTERNAL_DATA_SYSTEM,
            source_product_app=None,
            last_updated_utc=now,
        )
        for race in cluster.races
    )
    rows.extend(
        schema.PhoneNumber(
            recidiviz_id=recidiviz_id,
            number=phone_number.number,
            type=None,
            preferred=None,
            source_type=SourceType.EXTERNAL_DATA_SYSTEM,
            source_product_app=None,
            last_updated_utc=now,
        )
        for phone_number in cluster.phone_numbers
    )
    rows.extend(
        _email_rows(
            snapshot,
            recidiviz_id=recidiviz_id,
            now=now,
            committed_email_hashes=committed_email_hashes,
            staged_email_hashes=staged_email_hashes,
        )
    )
    return rows


def _name_rows(
    cluster: IdentityCluster, *, recidiviz_id: uuid.UUID, now: datetime.datetime
) -> list[schema.IdentityBase]:
    """Returns the Name rows for the cluster's primary name and aliases.

    For name (given_name=Bob, surname=Smith, preferred_name=Bobby) plus one alias
    (given_name=Robert, surname=Smith, name_use=ALIAS), returns:
        OFFICIAL   given_name=Bob     surname=Smith
        PREFERRED  given_name=Bobby   surname=Smith
        ALIAS      given_name=Robert  surname=Smith
    The PREFERRED row is the official name with the given name swapped for
    preferred_name; middle names and suffix are dropped.
    """
    rows: list[schema.IdentityBase] = []
    if cluster.name is not None:
        rows.append(
            schema.Name(
                recidiviz_id=recidiviz_id,
                given_name=cluster.name.given_name,
                surname=cluster.name.surname,
                middle_names=(
                    [cluster.name.middle_name] if cluster.name.middle_name else []
                ),
                name_suffix=cluster.name.name_suffix,
                use=NameUse.OFFICIAL,
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=now,
            )
        )
        if cluster.name.preferred_name is not None:
            rows.append(
                schema.Name(
                    recidiviz_id=recidiviz_id,
                    given_name=cluster.name.preferred_name,
                    surname=cluster.name.surname,
                    middle_names=[],
                    name_suffix=None,
                    use=NameUse.PREFERRED,
                    source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                    source_product_app=None,
                    last_updated_utc=now,
                )
            )
    for alias in cluster.aliases:
        rows.append(
            schema.Name(
                recidiviz_id=recidiviz_id,
                given_name=alias.given_name,
                surname=alias.surname,
                middle_names=[alias.middle_name] if alias.middle_name else [],
                name_suffix=alias.name_suffix,
                use=alias.name_use,
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=now,
            )
        )
    return rows


def _email_rows(
    snapshot: ClusterSnapshot,
    *,
    recidiviz_id: uuid.UUID,
    now: datetime.datetime,
    committed_email_hashes: set[str],
    staged_email_hashes: set[str],
) -> list[schema.IdentityBase]:
    """Returns the Email rows for the cluster, excluding already-seen addresses.

    See build_identity_rows for how the two hash sets are used.
    """
    rows: list[schema.IdentityBase] = []
    for email in snapshot.cluster.emails:
        address_hash = normalized_email_hash(email.address)
        if (
            address_hash in committed_email_hashes
            or address_hash in staged_email_hashes
        ):
            logging.warning(
                "Email on cluster [%s] already belongs to another identity in the "
                "tenant; excluding it from the new identity.",
                snapshot.cluster.identity_cluster_id,
            )
            continue
        staged_email_hashes.add(address_hash)
        rows.append(
            schema.Email(
                recidiviz_id=recidiviz_id,
                address=email.address,
                address_hash=address_hash,
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=now,
            )
        )
    return rows
