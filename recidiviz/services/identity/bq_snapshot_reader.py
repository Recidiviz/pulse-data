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
"""Reads a tenant's identity clustering snapshot from BigQuery.

The identity ingest pipeline writes each tenant's clustering results to the
{tenant}_identity_cluster dataset, one table per cluster entity type. This module
reads those tables and hydrates each cluster into an IdentityCluster entity for
the import, carrying the pipeline-computed cluster_hash verbatim so the import can
persist what the pipeline wrote without recomputing it.

It reads the tables by listing their rows directly (client.list_rows) rather than
running a SQL query job, because the import wants every row of each table with no
filtering or transformation, so a plain row listing is simpler and avoids a query
job's SQL planning, slot usage, and temp results table. It also uses Google's raw
google.cloud.bigquery client rather than the repo's recidiviz.big_query wrapper,
which would pull a large dependency tree into the service.
"""
import datetime
from collections.abc import Callable
from typing import TypeVar

import attr
from google.cloud import bigquery
from more_itertools import one

from recidiviz.common import attr_validators
from recidiviz.common.constants.identity import NameUse, PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterAlias,
    IdentityClusterEmail,
    IdentityClusterEthnicity,
    IdentityClusterExternalId,
    IdentityClusterGender,
    IdentityClusterName,
    IdentityClusterPhoneNumber,
    IdentityClusterRace,
    IdentityClusterSex,
)
from recidiviz.pipelines.ingest.identity.dataset_config import (
    identity_cluster_dataset_for_tenant,
)
from recidiviz.utils import metadata
from recidiviz.utils.list_helpers import group_by
from recidiviz.utils.types import assert_type

# The identity_cluster root row's primary key, which every child row carries as a
# foreign key back to its cluster. Both roles use the root entity's id field name,
# so one constant names the column everywhere it appears.
_CLUSTER_ID_COLUMN = IdentityCluster.get_class_id_name()

# Cap on the per-cluster details included in the malformed-snapshot error, so a
# systemically broken snapshot (every cluster failing the same way) still raises
# a readable message rather than one line per cluster.
_MAX_REPORTED_MALFORMED_CLUSTERS = 20

_Row = dict[str, object]

_ChildEntityT = TypeVar("_ChildEntityT")


@attr.define(frozen=True, kw_only=True)
class ClusterSnapshot:
    """A hydrated identity cluster paired with the cluster_hash the pipeline committed.

    stored_cluster_hash is the hash the identity ingest pipeline wrote to BigQuery for
    this cluster, which is the authoritative value the import persists as
    last_cluster_hash. The cluster entity recomputes its own cluster_hash when it is
    constructed from the snapshot rows; the import deliberately does not rely on that
    recomputed value, because trusting it would couple correctness to the reader
    reconstructing the pipeline's hash exactly — identical field ordering, hashing
    version, and every hashed field carried through. Persisting the stored hash keeps
    the eventual idempotency comparison pipeline-to-pipeline."""

    cluster: IdentityCluster = attr.ib(
        validator=attr.validators.instance_of(IdentityCluster)
    )
    """The cluster reconstructed from the snapshot tables."""

    stored_cluster_hash: str = attr.ib(validator=attr_validators.is_str)
    """The cluster_hash the pipeline wrote for this cluster."""


def read_cluster_snapshot(tenant: Tenant) -> list[ClusterSnapshot]:
    """Returns one ClusterSnapshot per cluster in the tenant's clustering snapshot.

    Reads the identity_cluster root table and each child table in full, groups the
    child rows under their cluster, and builds an IdentityCluster for each root row.
    Every entity's tenant comes from its own row's tenant column rather than the
    requested tenant, so IdentityCluster's tenant-agreement validation checks the
    snapshot's data integrity.

    Raises ValueError if any cluster's rows are malformed, after trying every
    cluster, so one read reports every malformed cluster rather than just the
    first.
    """
    client = bigquery.Client(project=metadata.project_id())
    dataset = identity_cluster_dataset_for_tenant(tenant.value)

    root_rows = _read_table(client, dataset, IdentityCluster.get_table_id())
    external_ids = _group_by_cluster(
        _read_table(client, dataset, IdentityClusterExternalId.get_table_id())
    )
    names = _group_by_cluster(
        _read_table(client, dataset, IdentityClusterName.get_table_id())
    )
    genders = _group_by_cluster(
        _read_table(client, dataset, IdentityClusterGender.get_table_id())
    )
    sexes = _group_by_cluster(
        _read_table(client, dataset, IdentityClusterSex.get_table_id())
    )
    races = _group_by_cluster(
        _read_table(client, dataset, IdentityClusterRace.get_table_id())
    )
    ethnicities = _group_by_cluster(
        _read_table(client, dataset, IdentityClusterEthnicity.get_table_id())
    )
    phone_numbers = _group_by_cluster(
        _read_table(client, dataset, IdentityClusterPhoneNumber.get_table_id())
    )
    emails = _group_by_cluster(
        _read_table(client, dataset, IdentityClusterEmail.get_table_id())
    )
    aliases = _group_by_cluster(
        _read_table(client, dataset, IdentityClusterAlias.get_table_id())
    )

    snapshots = []
    malformed_cluster_errors: list[str] = []
    for root in root_rows:
        cluster_id = assert_type(root[_CLUSTER_ID_COLUMN], str)
        try:
            cluster = IdentityCluster(
                tenant=_row_tenant(root),
                person_type=PersonType(root["person_type"]),
                birthdate=_as_opt_date(root["birthdate"]),
                external_ids=tuple(
                    _hydrate_external_id(row)
                    for row in external_ids.get(cluster_id, [])
                ),
                name=_single(names.get(cluster_id, []), _hydrate_name, cluster_id),
                gender=_single(
                    genders.get(cluster_id, []), _hydrate_gender, cluster_id
                ),
                sex=_single(sexes.get(cluster_id, []), _hydrate_sex, cluster_id),
                ethnicity=_single(
                    ethnicities.get(cluster_id, []), _hydrate_ethnicity, cluster_id
                ),
                races=tuple(_hydrate_race(row) for row in races.get(cluster_id, [])),
                phone_numbers=tuple(
                    _hydrate_phone_number(row)
                    for row in phone_numbers.get(cluster_id, [])
                ),
                emails=tuple(_hydrate_email(row) for row in emails.get(cluster_id, [])),
                aliases=tuple(
                    _hydrate_alias(row) for row in aliases.get(cluster_id, [])
                ),
            )
            snapshots.append(
                ClusterSnapshot(
                    cluster=cluster,
                    stored_cluster_hash=assert_type(root["cluster_hash"], str),
                )
            )
        except (ValueError, TypeError) as e:
            # ValueError comes from assert_type, enum parsing, and entity
            # validation; TypeError from attrs instance_of validators. Anything
            # else is not a malformed-row failure and propagates.
            malformed_cluster_errors.append(f"cluster [{cluster_id}]: {e}")
    if malformed_cluster_errors:
        reported = malformed_cluster_errors[:_MAX_REPORTED_MALFORMED_CLUSTERS]
        details = "\n".join(reported)
        raise ValueError(
            f"Snapshot for tenant [{tenant.value}] has "
            f"[{len(malformed_cluster_errors)}] malformed clusters; reporting the "
            f"first [{len(reported)}]:\n{details}"
        )
    return snapshots


def _read_table(client: bigquery.Client, dataset: str, table_id: str) -> list[_Row]:
    """Returns every row of the given table as a list of column-keyed dicts."""
    table_ref = bigquery.TableReference.from_string(
        f"{client.project}.{dataset}.{table_id}"
    )
    return [dict(row) for row in client.list_rows(table_ref)]


def _group_by_cluster(rows: list[_Row]) -> dict[str, list[_Row]]:
    """Returns the child-table rows grouped by the cluster they belong to."""
    return group_by(rows, key_fn=lambda row: assert_type(row[_CLUSTER_ID_COLUMN], str))


def _row_tenant(row: _Row) -> Tenant:
    """Returns the tenant recorded on the row itself."""
    return Tenant(assert_type(row["tenant"], str))


def _single(
    rows: list[_Row],
    hydrate: Callable[[_Row], _ChildEntityT],
    cluster_id: str,
) -> _ChildEntityT | None:
    """Returns the hydrated single row of a one-per-cluster child table, or None if
    absent.

    Raises if a cluster carries more than one row for a single-valued attribute,
    which would mean the snapshot violates that table's one-per-cluster shape."""
    if not rows:
        return None
    return hydrate(
        one(
            rows,
            too_long=ValueError(
                f"Expected at most one row per cluster for single-valued attribute, "
                f"but cluster [{cluster_id}] has [{len(rows)}]."
            ),
        )
    )


def _hydrate_external_id(row: _Row) -> IdentityClusterExternalId:
    return IdentityClusterExternalId(
        tenant=_row_tenant(row),
        external_id=assert_type(row["external_id"], str),
        id_type=assert_type(row["id_type"], str),
    )


def _hydrate_name(row: _Row) -> IdentityClusterName:
    return IdentityClusterName(
        tenant=_row_tenant(row),
        given_name=_as_opt_str(row["given_name"]),
        preferred_name=_as_opt_str(row["preferred_name"]),
        surname=_as_opt_str(row["surname"]),
        middle_name=_as_opt_str(row["middle_name"]),
        name_suffix=_as_opt_str(row["name_suffix"]),
    )


def _hydrate_gender(row: _Row) -> IdentityClusterGender:
    return IdentityClusterGender(
        tenant=_row_tenant(row),
        gender=Gender(row["gender"]),
        gender_raw_text=_as_opt_str(row["gender_raw_text"]),
    )


def _hydrate_sex(row: _Row) -> IdentityClusterSex:
    return IdentityClusterSex(
        tenant=_row_tenant(row),
        sex=Sex(row["sex"]),
        sex_raw_text=_as_opt_str(row["sex_raw_text"]),
    )


def _hydrate_race(row: _Row) -> IdentityClusterRace:
    return IdentityClusterRace(
        tenant=_row_tenant(row),
        race=Race(row["race"]),
        race_raw_text=_as_opt_str(row["race_raw_text"]),
    )


def _hydrate_ethnicity(row: _Row) -> IdentityClusterEthnicity:
    return IdentityClusterEthnicity(
        tenant=_row_tenant(row),
        ethnicity=Ethnicity(row["ethnicity"]),
        ethnicity_raw_text=_as_opt_str(row["ethnicity_raw_text"]),
    )


def _hydrate_phone_number(row: _Row) -> IdentityClusterPhoneNumber:
    return IdentityClusterPhoneNumber(
        tenant=_row_tenant(row), number=assert_type(row["number"], str)
    )


def _hydrate_email(row: _Row) -> IdentityClusterEmail:
    return IdentityClusterEmail(
        tenant=_row_tenant(row), address=assert_type(row["address"], str)
    )


def _hydrate_alias(row: _Row) -> IdentityClusterAlias:
    return IdentityClusterAlias(
        tenant=_row_tenant(row),
        given_name=_as_opt_str(row["given_name"]),
        surname=_as_opt_str(row["surname"]),
        middle_name=_as_opt_str(row["middle_name"]),
        name_suffix=_as_opt_str(row["name_suffix"]),
        name_use=NameUse(row["name_use"]),
        name_use_raw_text=_as_opt_str(row["name_use_raw_text"]),
    )


def _as_opt_str(value: object) -> str | None:
    return None if value is None else assert_type(value, str)


def _as_opt_date(value: object) -> datetime.date | None:
    return None if value is None else assert_type(value, datetime.date)
