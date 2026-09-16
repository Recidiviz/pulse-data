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
identity from a cluster."""
import datetime
import uuid

from recidiviz.common.constants.identity import IdentityStatus
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.services.identity.bq_snapshot_reader import ClusterSnapshot
from recidiviz.services.identity.identity_child_rows import (
    build_attribute_rows,
    build_external_id_rows,
)


def build_identity_rows(
    *,
    snapshot: ClusterSnapshot,
    now: datetime.datetime,
    committed_email_hashes: set[str],
    staged_email_hashes: set[str],
) -> list[schema.IdentityBase]:
    """Builds the rows for a new identity from a cluster.

    An identity consists of one row in the identities table, plus one row for
    each value the cluster carries in the appropriate child table: an
    external_ids row per external id, a names row per name, an emails row per
    address, and so on. See identity_child_rows for the full mapping.

    The identity stores the pipeline's cluster_hash as last_cluster_hash so
    the update pass skips the cluster on the next import if it is unchanged.

    TODO(OBT-50191): POST /identities creates identities through
    IdentityServiceQuerier.create_identity and the domain types instead of
    building ORM rows directly. Unify the two creation paths without giving up
    this pass's chunked commits and in-memory email dedupe.

    An email whose address hash is already committed or staged is excluded, since
    an address is expected to reach at most one person.

    The caller adds the returned rows to a session and controls the transaction
    boundary.

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
    rows.extend(build_external_id_rows(cluster, recidiviz_id=recidiviz_id))
    rows.extend(
        build_attribute_rows(
            cluster,
            recidiviz_id=recidiviz_id,
            now=now,
            committed_email_hashes=committed_email_hashes,
            staged_email_hashes=staged_email_hashes,
        )
    )
    return rows
