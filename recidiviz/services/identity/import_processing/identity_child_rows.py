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
"""Builders for an identity's child rows (external ids and attribute values)."""
import datetime
import logging
import uuid

from recidiviz.common.constants.identity import IdentifierType, NameUse, SourceType
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
)
from recidiviz.utils.user_hash import normalized_email_hash

# Every row type build_attribute_rows can produce, one per attribute child
# table. A consumer that sweeps an identity's attribute tables iterates this
# instead of hand-listing them, so the sweep cannot drift from what the builder
# writes; identity_child_rows_test enforces that the list matches the builder's
# output.
ATTRIBUTE_ROW_TYPES: tuple[type[schema.IdentityBase], ...] = (
    schema.Name,
    schema.DateOfBirth,
    schema.Gender,
    schema.Race,
    schema.Sex,
    schema.Ethnicity,
    schema.PhoneNumber,
    schema.Email,
)


def build_external_id_rows(
    cluster: IdentityCluster, *, recidiviz_id: uuid.UUID
) -> list[schema.ExternalId]:
    """Returns an active ExternalId row for each of the cluster's external ids."""
    return [
        schema.ExternalId(
            recidiviz_id=recidiviz_id,
            external_id=external_id.external_id,
            id_type=IdentifierType(external_id.id_type),
            is_active=True,
        )
        for external_id in cluster.external_ids
    ]


def build_attribute_rows(
    cluster: IdentityCluster,
    *,
    recidiviz_id: uuid.UUID,
    now: datetime.datetime,
    committed_email_hashes: set[str],
    staged_email_hashes: set[str],
) -> list[schema.IdentityBase]:
    """Returns one attribute row per cluster value for the given identity.
    Each returned row is an instance of one of ATTRIBUTE_ROW_TYPES.

    An email whose address hash is already committed or staged is excluded,
    since an address is expected to reach at most one person.

    All attribute rows carry source_type EXTERNAL_DATA_SYSTEM because the
    cluster's values come from the identity ingest pipeline. Canonical selection
    is trivial while that is the only source type writing to the service, so
    each single-valued attribute row is marked canonical.

    Args:
        cluster: The cluster whose values become attribute rows.
        recidiviz_id: The identity the rows belong to.
        now: Timestamp stamped on the attribute rows.
        committed_email_hashes: Hashes of emails written in prior transactions;
            read only here.
        staged_email_hashes: Hashes used earlier in this transaction; this
            function adds each email it keeps, and the caller folds these into
            committed_email_hashes once the transaction commits.
    """
    rows: list[schema.IdentityBase] = []
    rows.extend(_build_name_rows(cluster, recidiviz_id=recidiviz_id, now=now))
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
        _build_email_rows(
            cluster,
            recidiviz_id=recidiviz_id,
            now=now,
            committed_email_hashes=committed_email_hashes,
            staged_email_hashes=staged_email_hashes,
        )
    )
    return rows


def _build_name_rows(
    cluster: IdentityCluster, *, recidiviz_id: uuid.UUID, now: datetime.datetime
) -> list[schema.Name]:
    """Returns the Name rows for the cluster's primary name and aliases.

    For name (given_name=Bob, surname=Smith, preferred_name=Bobby) plus one alias
    (given_name=Robert, surname=Smith, name_use=ALIAS), returns:
        OFFICIAL   given_name=Bob     surname=Smith
        PREFERRED  given_name=Bobby   surname=Smith
        ALIAS      given_name=Robert  surname=Smith
    The PREFERRED row is the official name with the given name swapped for
    preferred_name; middle names and suffix are dropped.
    """
    rows: list[schema.Name] = []
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


def _build_email_rows(
    cluster: IdentityCluster,
    *,
    recidiviz_id: uuid.UUID,
    now: datetime.datetime,
    committed_email_hashes: set[str],
    staged_email_hashes: set[str],
) -> list[schema.Email]:
    """Returns the Email rows for the cluster, excluding already-seen addresses.

    See build_attribute_rows for how the two hash sets are used.
    """
    rows: list[schema.Email] = []
    for email in cluster.emails:
        address_hash = normalized_email_hash(email.address)
        if (
            address_hash in committed_email_hashes
            or address_hash in staged_email_hashes
        ):
            # TODO(OBT-49081): Record this exclusion as a reviewable email
            # conflict instead of only logging it.
            logging.warning(
                "Email on cluster [%s] already belongs to another identity in the "
                "tenant; excluding it from this identity.",
                cluster.identity_cluster_id,
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
