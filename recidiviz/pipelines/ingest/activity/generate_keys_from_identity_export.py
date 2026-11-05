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
"""A PTransform that produces the (external ID -> primary key) lookup from the
Identity Service export, replacing the legacy clustering block when a tenant's
identity source is IDENTITY_SERVICE_EXPORT."""
import apache_beam as beam

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common.constants.identity import IdentityStatus, PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.entity.generate_primary_key import PrimaryKey
from recidiviz.pipelines.ingest.types import ExternalIdKey
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import ReadFromBigQuery
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.source_tables.identity_service_export_names import (
    LEGACY_PERSON_ID_COLUMN,
    LEGACY_STAFF_ID_COLUMN,
)
from recidiviz.utils.types import assert_type

_EXTERNAL_ID_COLUMN = assert_type(schema.ExternalId.external_id.name, str)
_ID_TYPE_COLUMN = assert_type(schema.ExternalId.id_type.name, str)
_RECIDIVIZ_ID_COLUMN = assert_type(schema.Identity.recidiviz_id.name, str)
_PERSON_TYPE_COLUMN = assert_type(schema.Identity.person_type.name, str)


class GenerateKeysFromIdentityExport(beam.PTransform):
    """Emits an (ExternalIdKey, PrimaryKey) pair for every active external ID
    of every active identity the export holds for the tenant (excluding
    RECIDIVIZ_EMPLOYEE identities), using the exported legacy_person_id /
    legacy_staff_id as the key.

    The per-pair shape matches the legacy clustering block's output, but NOT
    its coverage invariant: the legacy block builds the lookup from the
    ingested entities, so the lookup and the entities always cover exactly the
    same external IDs. The export and this run's ingested entities can each
    hold IDs the other does not, and either mismatch crashes the shared
    association join until TODO(OBT-51950) reconciles both before the join:

    - The export holds an ID with no ingested rows this run, e.g. a person an
      admin added to the identity service who hasn't appeared in state data
      files yet. Expected and benign, but the resulting empty entity group
      crashes MergeRootEntitiesAcrossDates.
    - This run ingests an ID the export doesn't hold yet, e.g. a person new to
      state data since the last export ran. Routine whenever ingest runs
      between exports, but the missing primary key crashes
      AssociateRootEntitiesWithPrimaryKeys.
    """

    def __init__(
        self,
        *,
        # Tenant whose identities to read from the export, which holds every
        # tenant's identities in shared tables.
        tenant: Tenant,
        # Dataset the export tables live in.
        export_dataset: str,
        # Project the export dataset lives in, used to fully qualify the
        # query's table references.
        project_id: str,
        # Resource labels to apply to the BigQuery read job.
        resource_labels: dict[str, str],
    ) -> None:
        super().__init__()
        self.tenant = tenant
        self.export_dataset = export_dataset
        self.project_id = project_id
        self.resource_labels = resource_labels

    def expand(
        self, input_or_inputs: beam.pvalue.PBegin
    ) -> beam.PCollection[tuple[ExternalIdKey, PrimaryKey]]:
        return (
            input_or_inputs
            | "Read active external IDs of active identities from the export"
            >> ReadFromBigQuery(
                query=self._build_query(), resource_labels=self.resource_labels
            )
            | "Key each external ID by its identity's exported legacy id"
            >> beam.Map(_to_external_id_key_and_primary_key)
        )

    def _build_query(self) -> str:
        """Returns the query for the tenant's active external IDs, each joined
        to its identity's person_type and exported legacy id columns.

        Excludes RECIDIVIZ_EMPLOYEE identities, which deliberately have both
        legacy id columns NULL, so they never reach the keyless-row check in
        _to_external_id_key_and_primary_key. Any other person type does reach
        it and fails loudly there if no legacy id column keys it.
        """
        external_ids_address = ProjectSpecificBigQueryAddress(
            project_id=self.project_id,
            dataset_id=self.export_dataset,
            table_id=assert_type(schema.ExternalId.__tablename__, str),
        ).format_address_for_query()
        identities_address = ProjectSpecificBigQueryAddress(
            project_id=self.project_id,
            dataset_id=self.export_dataset,
            table_id=assert_type(schema.Identity.__tablename__, str),
        ).format_address_for_query()
        tenant_column = assert_type(schema.Identity.tenant.name, str)
        status_column = assert_type(schema.Identity.status.name, str)
        is_active_column = assert_type(schema.ExternalId.is_active.name, str)
        return f"""
SELECT
    external_ids.{_EXTERNAL_ID_COLUMN},
    external_ids.{_ID_TYPE_COLUMN},
    identities.{_RECIDIVIZ_ID_COLUMN},
    identities.{_PERSON_TYPE_COLUMN},
    identities.{LEGACY_PERSON_ID_COLUMN},
    identities.{LEGACY_STAFF_ID_COLUMN}
FROM {external_ids_address} external_ids
JOIN {identities_address} identities
USING ({_RECIDIVIZ_ID_COLUMN})
WHERE identities.{tenant_column} = '{self.tenant.value}'
    AND identities.{status_column} = '{IdentityStatus.ACTIVE.value}'
    AND identities.{_PERSON_TYPE_COLUMN} != '{PersonType.RECIDIVIZ_EMPLOYEE.value}'
    AND external_ids.{is_active_column}
"""


def _to_external_id_key_and_primary_key(
    row: TableRow,
) -> tuple[ExternalIdKey, PrimaryKey]:
    """Returns the ((external_id, id_type), primary key) pair for one queried
    external-ID row, raising if the export computed no legacy key for the row's
    identity."""
    external_id_key: ExternalIdKey = (
        assert_type(row[_EXTERNAL_ID_COLUMN], str),
        assert_type(row[_ID_TYPE_COLUMN], str),
    )
    person_type = PersonType(row[_PERSON_TYPE_COLUMN])
    legacy_id_column = _legacy_id_column_for_person_type(person_type)
    legacy_id = row[legacy_id_column]
    if legacy_id is None:
        raise ValueError(
            f"Identity [{row[_RECIDIVIZ_ID_COLUMN]}] is an active {person_type.value} "
            f"identity with active external IDs (including {external_id_key}) "
            f"but a NULL {legacy_id_column}: the export computed no key for an "
            f"identity this pipeline would try to hydrate. The export computes "
            f"legacy ids from the same active external IDs, so this indicates "
            f"a bug or partial write in the Identity Service export; re-run "
            f"the export for this tenant and investigate if the column is "
            f"still NULL."
        )
    return (external_id_key, assert_type(legacy_id, int))


def _legacy_id_column_for_person_type(person_type: PersonType) -> str:
    """Returns the exported legacy id column that keys identities of the given
    person type."""
    if person_type is PersonType.JII:
        return LEGACY_PERSON_ID_COLUMN
    if person_type is PersonType.STAFF:
        return LEGACY_STAFF_ID_COLUMN
    raise ValueError(
        f"Unexpected person_type [{person_type}] in export key lookup query "
        f"results. Decide which legacy id column keys this person type, or "
        f"exclude it from the query like RECIDIVIZ_EMPLOYEE."
    )
