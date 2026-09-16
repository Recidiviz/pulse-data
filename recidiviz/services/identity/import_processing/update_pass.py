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
"""Update pass of the Identity Service import: brings an existing identity up
to date with its cluster."""
import datetime
import logging
import uuid

from sqlalchemy.orm import Session

from recidiviz.common.constants.identity import SourceType
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
)
from recidiviz.services.identity.import_processing.bq_snapshot_reader import (
    ClusterSnapshot,
)
from recidiviz.services.identity.import_processing.identity_child_rows import (
    ATTRIBUTE_ROW_TYPES,
    build_attribute_rows,
    build_external_id_rows,
)

# The attribute tables whose rows carry canonical/canonical_locked flags, derived
# from the schema so a table that gains the flags is never missed here; missing
# one would let an update delete rows a human pinned.
_CANONICAL_LOCKABLE_TABLES = tuple(
    table for table in ATTRIBUTE_ROW_TYPES if hasattr(table, "canonical_locked")
)


# TODO(OBT-50382): This issues several queries per identity. If production-scale
# runs show it is a bottleneck, refactor to operate on a batch of identities and
# snapshots so the queries batch at the DB level, falling back to batches of size
# 1 when a batch fails.
def apply_identity_update(
    *,
    session: Session,
    identity: schema.Identity,
    snapshot: ClusterSnapshot,
    now: datetime.datetime,
    committed_email_hashes: set[str],
    staged_email_hashes: set[str],
) -> None:
    """Brings an identity up to date with its matching cluster snapshot.

    Adds any external ids the identity lacks, never removing any, replaces its
    EXTERNAL_DATA_SYSTEM attribute rows with the cluster's values (a row a
    human has pinned via canonical_locked survives untouched), and makes the
    identity's person_type match the cluster's, logging a warning when that
    changes it.

    Stages every change on the given session; the caller controls the
    transaction boundary. Writes the snapshot's stored cluster hash to
    last_cluster_hash so the next run skips the cluster if it is unchanged.

    Args:
        session: Session the identity is attached to and changes are staged on.
        identity: The identity to bring up to date.
        snapshot: The cluster whose values the identity should carry.
        now: Timestamp stamped on the identity and its new attribute rows.
        committed_email_hashes: Hashes of emails the tenant's identities own as
            of prior transactions; the identity's own hashes are ignored so it
            keeps addresses it already owns. Read only here.
        staged_email_hashes: Hashes used earlier in this transaction; this
            function adds each email it keeps, and the caller folds these into
            committed_email_hashes once the transaction commits.
    """
    cluster = snapshot.cluster
    _add_missing_external_ids(
        session=session, cluster=cluster, recidiviz_id=identity.recidiviz_id
    )
    _replace_external_data_system_attributes(
        session=session,
        cluster=cluster,
        recidiviz_id=identity.recidiviz_id,
        now=now,
        committed_email_hashes=committed_email_hashes,
        staged_email_hashes=staged_email_hashes,
    )
    if identity.person_type is not cluster.person_type:
        # A flip means the tenant's ingest views reclassified the person; apply
        # it, but loudly, since products treat JII and staff differently.
        logging.warning(
            "Import is changing the person type of identity [%s] from [%s] to [%s].",
            identity.recidiviz_id,
            identity.person_type.value,
            cluster.person_type.value,
        )
        identity.person_type = cluster.person_type
    identity.last_cluster_hash = snapshot.stored_cluster_hash
    identity.last_updated_utc = now


def _add_missing_external_ids(
    *, session: Session, cluster: IdentityCluster, recidiviz_id: uuid.UUID
) -> None:
    """Adds an active ExternalId row for each cluster external id the identity
    lacks.

    External ids are additive: an id already on the identity, active or not, is
    left as is, and ids on the identity but absent from the cluster are never
    removed.
    """
    existing_keys = {
        (row.id_type, row.external_id)
        for row in session.query(schema.ExternalId).filter(
            schema.ExternalId.recidiviz_id == recidiviz_id
        )
    }
    session.add_all(
        [
            row
            for row in build_external_id_rows(cluster, recidiviz_id=recidiviz_id)
            if (row.id_type, row.external_id) not in existing_keys
        ]
    )


def _replace_external_data_system_attributes(
    *,
    session: Session,
    cluster: IdentityCluster,
    recidiviz_id: uuid.UUID,
    now: datetime.datetime,
    committed_email_hashes: set[str],
    staged_email_hashes: set[str],
) -> None:
    """Replaces the identity's EXTERNAL_DATA_SYSTEM attribute rows with the
    cluster's values.

    A canonical_locked row is preserved and no replacement row of its type is
    written; the lock protects the human's selection in either direction, so a
    row pinned canonical=False also blocks the incoming value, leaving the
    identity with no canonical value of that type until someone unpins it.
    Only EXTERNAL_DATA_SYSTEM rows exist while import is the only writer; once
    other source types write locked rows, how a locked row of another source
    interacts with the incoming value is decided as part of cross-source
    canonical selection (OBT-37723).
    """
    own_email_hashes = {
        row.address_hash
        for row in session.query(schema.Email).filter(
            schema.Email.recidiviz_id == recidiviz_id,
            schema.Email.source_type == SourceType.EXTERNAL_DATA_SYSTEM,
        )
    }
    locked_tables = {
        table
        for table in _CANONICAL_LOCKABLE_TABLES
        if session.query(table)
        .filter(
            table.recidiviz_id == recidiviz_id,
            table.canonical_locked.is_(True),
        )
        .count()
    }
    for table in ATTRIBUTE_ROW_TYPES:
        delete_query = session.query(table).filter(
            table.recidiviz_id == recidiviz_id,
            table.source_type == SourceType.EXTERNAL_DATA_SYSTEM,
        )
        if table in _CANONICAL_LOCKABLE_TABLES:
            delete_query = delete_query.filter(table.canonical_locked.is_(False))
        delete_query.delete(synchronize_session=False)
    session.add_all(
        [
            row
            for row in build_attribute_rows(
                cluster,
                recidiviz_id=recidiviz_id,
                now=now,
                committed_email_hashes=committed_email_hashes - own_email_hashes,
                staged_email_hashes=staged_email_hashes,
            )
            if type(row) not in locked_tables
        ]
    )
