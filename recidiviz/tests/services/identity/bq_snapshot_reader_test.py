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
"""Tests for the identity clustering BigQuery snapshot reader."""
import datetime
from collections import defaultdict
from unittest import TestCase
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.identity import NameUse, PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.entity_utils import get_all_entities_from_tree
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities_module_context,
)
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
from recidiviz.persistence.entity.serialization import (
    serialize_entity_into_json,
    serialize_entity_tree_into_json,
)
from recidiviz.services.identity.bq_snapshot_reader import (
    ClusterSnapshot,
    read_cluster_snapshot,
)

_CTX = identity_cluster_entities_module_context.IDENTITY_CLUSTER_ENTITIES_CONTEXT
_READER = "recidiviz.services.identity.bq_snapshot_reader"


def _rich_cluster(tenant: Tenant) -> IdentityCluster:
    """A cluster exercising every child entity type, single- and multi-valued."""
    return IdentityCluster(
        tenant=tenant,
        person_type=PersonType.JII,
        birthdate=datetime.date(1990, 1, 1),
        external_ids=(
            IdentityClusterExternalId(
                tenant=tenant, external_id="A123", id_type="US_OZ_LOTR_ID"
            ),
            IdentityClusterExternalId(
                tenant=tenant, external_id="B456", id_type="US_OZ_KDS_PERSON_ID"
            ),
        ),
        name=IdentityClusterName(
            tenant=tenant,
            given_name="Frodo",
            preferred_name="Mr Underhill",
            surname="Baggins",
            middle_name="R",
            name_suffix=None,
        ),
        gender=IdentityClusterGender(
            tenant=tenant, gender=Gender.MALE, gender_raw_text="M"
        ),
        sex=IdentityClusterSex(tenant=tenant, sex=Sex.MALE, sex_raw_text="M"),
        ethnicity=IdentityClusterEthnicity(
            tenant=tenant,
            ethnicity=Ethnicity.NOT_HISPANIC,
            ethnicity_raw_text="N",
        ),
        races=(
            IdentityClusterRace(tenant=tenant, race=Race.WHITE, race_raw_text="W"),
            IdentityClusterRace(tenant=tenant, race=Race.ASIAN, race_raw_text="A"),
        ),
        phone_numbers=(IdentityClusterPhoneNumber(tenant=tenant, number="5551234567"),),
        emails=(IdentityClusterEmail(tenant=tenant, address="frodo@shire.test"),),
        aliases=(
            IdentityClusterAlias(
                tenant=tenant,
                given_name="Maura",
                surname="Labingi",
                middle_name=None,
                name_suffix=None,
                name_use=NameUse.ALIAS,
                name_use_raw_text="aka",
            ),
        ),
    )


def _minimal_cluster(tenant: Tenant) -> IdentityCluster:
    """A cluster with only the required external id and no optional attributes."""
    return IdentityCluster(
        tenant=tenant,
        person_type=PersonType.JII,
        external_ids=(
            IdentityClusterExternalId(
                tenant=tenant, external_id="C789", id_type="US_OZ_LOTR_ID"
            ),
        ),
    )


def _bq_rows(*clusters: IdentityCluster) -> dict[str, list[dict]]:
    """Serializes clusters into the per-table rows the reader reads.

    Mirrors what the pipeline writes and what list_rows returns: each entity's
    row uses its real BigQuery columns (via serialize_entity_into_json), with
    DATE columns as datetime.date rather than the JSON ISO-string form."""
    rows_by_table: dict[str, list[dict]] = defaultdict(list)
    for cluster in clusters:
        for entity in get_all_entities_from_tree(cluster, _CTX):
            row = serialize_entity_into_json(entity, _CTX)
            if row.get("birthdate") is not None:
                row["birthdate"] = datetime.date.fromisoformat(row["birthdate"])
            rows_by_table[entity.get_table_id()].append(row)
    return rows_by_table


def _mock_client(rows_by_table: dict[str, list[dict]]) -> MagicMock:
    client = MagicMock()
    client.project = "test-project"
    client.list_rows.side_effect = lambda ref: rows_by_table.get(ref.table_id, [])
    return client


class ReadClusterSnapshotTest(TestCase):
    """Tests for read_cluster_snapshot."""

    def _read(self, rows_by_table: dict[str, list[dict]]) -> list[ClusterSnapshot]:
        with patch(
            f"{_READER}.metadata.project_id", return_value="test-project"
        ), patch(
            f"{_READER}.bigquery.Client",
            return_value=_mock_client(rows_by_table),
        ):
            return read_cluster_snapshot(Tenant.US_OZ)

    def test_round_trips_a_rich_cluster(self) -> None:
        original = _rich_cluster(Tenant.US_OZ)
        snapshots = self._read(_bq_rows(original))

        self.assertEqual(1, len(snapshots))
        snapshot = snapshots[0]
        self.assertEqual(
            serialize_entity_tree_into_json(original, _CTX),
            serialize_entity_tree_into_json(snapshot.cluster, _CTX),
        )
        self.assertEqual(original.cluster_hash, snapshot.stored_cluster_hash)

    def test_round_trips_a_minimal_cluster(self) -> None:
        original = _minimal_cluster(Tenant.US_OZ)
        snapshots = self._read(_bq_rows(original))

        self.assertEqual(1, len(snapshots))
        self.assertEqual(
            serialize_entity_tree_into_json(original, _CTX),
            serialize_entity_tree_into_json(snapshots[0].cluster, _CTX),
        )

    def test_reads_multiple_clusters_independently(self) -> None:
        rich = _rich_cluster(Tenant.US_OZ)
        minimal = _minimal_cluster(Tenant.US_OZ)
        snapshots = self._read(_bq_rows(rich, minimal))

        by_id = {s.cluster.identity_cluster_id: s for s in snapshots}
        self.assertEqual(
            {rich.identity_cluster_id, minimal.identity_cluster_id}, set(by_id)
        )
        self.assertEqual(
            serialize_entity_tree_into_json(rich, _CTX),
            serialize_entity_tree_into_json(
                by_id[rich.identity_cluster_id].cluster, _CTX
            ),
        )
        self.assertEqual(
            serialize_entity_tree_into_json(minimal, _CTX),
            serialize_entity_tree_into_json(
                by_id[minimal.identity_cluster_id].cluster, _CTX
            ),
        )

    def test_empty_snapshot_returns_no_clusters(self) -> None:
        self.assertEqual([], self._read({}))

    def test_multiple_rows_for_single_valued_attribute_raises(self) -> None:
        original = _rich_cluster(Tenant.US_OZ)
        rows = _bq_rows(original)
        # Duplicate the single gender row so the cluster has two.
        rows[IdentityClusterGender.get_table_id()].append(
            rows[IdentityClusterGender.get_table_id()][0]
        )
        with self.assertRaisesRegex(
            ValueError,
            rf"(?s)has \[1\] malformed clusters.*"
            rf"cluster \[{original.identity_cluster_id}\]: "
            rf"Expected at most one row per cluster",
        ):
            self._read(rows)

    def test_reports_every_malformed_cluster(self) -> None:
        rich = _rich_cluster(Tenant.US_OZ)
        minimal = _minimal_cluster(Tenant.US_OZ)
        rows = _bq_rows(rich, minimal)
        # Corrupt both clusters: duplicate rich's single gender row, and clear
        # minimal's person_type.
        rows[IdentityClusterGender.get_table_id()].append(
            rows[IdentityClusterGender.get_table_id()][0]
        )
        for root in rows[IdentityCluster.get_table_id()]:
            if root["identity_cluster_id"] == minimal.identity_cluster_id:
                root["person_type"] = None

        with self.assertRaisesRegex(
            ValueError,
            rf"(?s)has \[2\] malformed clusters"
            rf".*cluster \[{rich.identity_cluster_id}\]"
            rf".*cluster \[{minimal.identity_cluster_id}\]",
        ):
            self._read(rows)

    def test_mismatched_tenant_across_rows_raises(self) -> None:
        original = _rich_cluster(Tenant.US_OZ)
        rows = _bq_rows(original)
        rows[IdentityClusterGender.get_table_id()][0]["tenant"] = Tenant.US_ND.value

        with self.assertRaisesRegex(ValueError, r"must share the cluster's tenant"):
            self._read(rows)
