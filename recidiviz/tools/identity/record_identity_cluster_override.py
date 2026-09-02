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
"""Records one identity_cluster_override row so the identity ingest pipeline
keeps or drops a specific cluster on its next run.

A BLESS override keeps a cluster whose fragments conflict, using values the
reviewer supplies. The script finds the cluster in the rejected_identity_cluster
table, shows the conflicts, and prompts for a value for each conflicting
attribute; leaving a prompt empty stores null for that attribute. An EXCLUDE
override drops a cluster whether or not it conflicts, so it needs only the
cluster's external ids.

A BLESS also copies the rejected cluster's conflicts_hash, and the pipeline
applies the blessing only while the cluster's conflicts still hash to that
value. A cluster whose conflict evidence changes after the review falls back to
rejection, reappearing in rejected_identity_cluster with its current conflicts
and hash. TODO(OBT-43102): add a refresh mode that shows the current conflicts
next to the recorded blessing and, on the reviewer's confirmation, updates the
stored hash in place.

A cluster may have at most one override (the pipeline fails its run on a
duplicate), so the script refuses to write a second row for a cluster that
already has one; delete the existing row first to change a decision.

Example, blessing a cluster whose fragments disagree on surname:

    python -m recidiviz.tools.identity.record_identity_cluster_override \\
        --tenant US_ND --disposition bless \\
        --external-id 12345:US_ND_SID --external-id A9:US_ND_ELITE \\
        --recorded-by kim

Example, excluding a known-bad cluster:

    python -m recidiviz.tools.identity.record_identity_cluster_override \\
        --tenant US_ND --disposition exclude --person-type JII \\
        --external-id 12345:US_ND_SID --external-id 67890:US_ND_SID \\
        --recorded-by kim --note "different people; recycled SID number"

Pass --sandbox-dataset-prefix to write to a sandbox overrides dataset (and, for
a BLESS, read the sandbox rejections dataset) instead of the tenant's real ones.
"""
import argparse
import datetime
import sys

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.enum_parser import parse_enum
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Sex
from recidiviz.ingest.direct.external_id_type_helpers import validate_external_id_types
from recidiviz.pipelines.ingest.identity.dataset_config import (
    identity_overrides_dataset_for_tenant,
    identity_rejections_dataset_for_tenant,
)
from recidiviz.pipelines.ingest.identity.identity_cluster_override import (
    IDENTITY_CLUSTER_OVERRIDE_TABLE_ID,
    BlessedIdentityValues,
    IdentityClusterOverride,
    IdentityClusterOverrideDisposition,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttribute,
)
from recidiviz.pipelines.ingest.identity.read_identity_cluster_overrides import (
    read_identity_cluster_overrides,
)
from recidiviz.pipelines.ingest.identity.rejected_identity_cluster import (
    CONFLICT_FIELD_FIELD,
    CONFLICT_VALUES_FIELD,
    CONFLICTS_HASH_COL,
    EXTERNAL_ID_FIELD,
    EXTERNAL_IDS_COL,
    ID_TYPE_FIELD,
    PERSON_TYPE_COL,
    RECORDED_CONFLICTS_COL,
    REJECTED_IDENTITY_CLUSTER_TABLE_ID,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.types import assert_type


def record_identity_cluster_override(
    *,
    tenant: Tenant,
    disposition: IdentityClusterOverrideDisposition,
    external_ids: tuple[tuple[str, str], ...],
    person_type_arg: PersonType | None,
    recorded_by: str,
    note_arg: str | None,
    project_id: str,
    sandbox_dataset_prefix: str | None,
) -> None:
    """Builds one override from the reviewer's input and writes it to the
    tenant's identity_cluster_override table."""
    validate_external_id_types(
        state_code=tenant.to_state_code(),
        external_id_types_to_check=[id_type for _, id_type in external_ids],
    )
    rejections_dataset_id = identity_rejections_dataset_for_tenant(
        tenant.value, sandbox_dataset_prefix=sandbox_dataset_prefix
    )
    overrides_dataset_id = identity_overrides_dataset_for_tenant(
        tenant.value, sandbox_dataset_prefix=sandbox_dataset_prefix
    )
    client = BigQueryClientImpl(project_id=project_id)

    _check_no_existing_override(
        project_id=project_id,
        overrides_dataset_id=overrides_dataset_id,
        tenant=tenant,
        external_ids=external_ids,
    )

    if disposition is IdentityClusterOverrideDisposition.BLESS:
        rejected_row = _find_rejected_cluster(
            client=client,
            project_id=project_id,
            dataset_id=rejections_dataset_id,
            external_ids=frozenset(external_ids),
        )
        person_type = PersonType(rejected_row[PERSON_TYPE_COL])
        if person_type_arg is not None and person_type_arg is not person_type:
            raise ValueError(
                f"Passed --person-type [{person_type_arg.value}], but the "
                f"rejected cluster records person type [{person_type.value}]."
            )
        conflicts = assert_type(rejected_row[RECORDED_CONFLICTS_COL], list)
        conflicting_attributes = [
            ConflictCheckedAttribute(conflict[CONFLICT_FIELD_FIELD])
            for conflict in conflicts
        ]
        print("Cluster conflicts:")
        for conflict in conflicts:
            print(
                f"  {conflict[CONFLICT_FIELD_FIELD]}: "
                f"[{', '.join(conflict[CONFLICT_VALUES_FIELD])}]"
            )
        blessed_values: BlessedIdentityValues | None = _prompt_for_blessed_values(
            conflicting_attributes
        )
        conflicts_hash: str | None = assert_type(rejected_row[CONFLICTS_HASH_COL], str)
    else:
        if person_type_arg is None:
            raise ValueError("An EXCLUDE override requires --person-type.")
        person_type = person_type_arg
        blessed_values = None
        conflicts_hash = None

    note = note_arg
    while not note:
        note = input("Note: ").strip()
    override = IdentityClusterOverride(
        tenant=tenant,
        person_type=person_type,
        external_ids=external_ids,
        disposition=disposition,
        blessed_values=blessed_values,
        conflicts_hash=conflicts_hash,
        recorded_by=recorded_by,
        recorded_at=datetime.datetime.now(tz=datetime.UTC),
        note=note,
    )

    print(f"\nAbout to write this override to [{project_id}.{overrides_dataset_id}]:")
    print(f"  disposition:  {override.disposition.value}")
    print(f"  person_type:  {override.person_type.value}")
    print(f"  external_ids: {list(override.external_ids)}")
    print(f"  note:         {override.note}")
    if override.blessed_values is not None:
        print("  blessed_values:")
        for field, value in override.blessed_values.to_bq_dict().items():
            print(f"    {field}: {value}")
    prompt_for_confirmation("Continue?")

    client.load_into_table_async(
        address=BigQueryAddress(
            dataset_id=overrides_dataset_id,
            table_id=IDENTITY_CLUSTER_OVERRIDE_TABLE_ID,
        ),
        rows=[override.to_bq_row()],
    ).result()
    print("Recorded.")


def main(argv: list[str]) -> None:
    args = _parse_args(argv)
    record_identity_cluster_override(
        tenant=args.tenant,
        disposition=IdentityClusterOverrideDisposition(args.disposition),
        external_ids=tuple(sorted(args.external_ids)),
        person_type_arg=(
            PersonType(args.person_type) if args.person_type is not None else None
        ),
        recorded_by=args.recorded_by,
        note_arg=args.note,
        project_id=args.project_id,
        sandbox_dataset_prefix=args.sandbox_dataset_prefix,
    )


def _check_no_existing_override(
    *,
    project_id: str,
    overrides_dataset_id: str,
    tenant: Tenant,
    external_ids: tuple[tuple[str, str], ...],
) -> None:
    """Raises if the cluster with the given external ids already has a recorded
    override. A second row for the same cluster would fail the pipeline's next
    run, so the existing row must be deleted before a new decision is recorded."""
    existing_overrides = read_identity_cluster_overrides(
        project_id=project_id, dataset_id=overrides_dataset_id, tenant=tenant
    )
    existing = existing_overrides.get_override(external_ids)
    if existing is not None:
        raise ValueError(
            f"An override for the cluster with external ids "
            f"[{sorted(external_ids)}] already exists: disposition "
            f"[{existing.disposition.value}], recorded by [{existing.recorded_by}] "
            f"at [{existing.recorded_at.isoformat()}]. A cluster may have at most "
            f"one override, so delete the existing row from "
            f"[{overrides_dataset_id}.{IDENTITY_CLUSTER_OVERRIDE_TABLE_ID}] before "
            f"recording a new one."
        )


def _find_rejected_cluster(
    *,
    client: BigQueryClientImpl,
    project_id: str,
    dataset_id: str,
    external_ids: frozenset[tuple[str, str]],
) -> dict[str, object]:
    """Returns the rejected_identity_cluster row whose external ids exactly match
    the given set, raising if none does."""
    query = (
        f"SELECT * FROM `{project_id}.{dataset_id}."
        f"{REJECTED_IDENTITY_CLUSTER_TABLE_ID}`"
    )
    for row in client.run_query_async(query_str=query, use_query_cache=False):
        row_ids = frozenset(
            (record[EXTERNAL_ID_FIELD], record[ID_TYPE_FIELD])
            for record in row[EXTERNAL_IDS_COL]
        )
        if row_ids == external_ids:
            return dict(row)
    raise ValueError(
        f"Found no cluster with external ids [{sorted(external_ids)}] in "
        f"[{dataset_id}.{REJECTED_IDENTITY_CLUSTER_TABLE_ID}]. Only a cluster the "
        f"pipeline rejected can be blessed."
    )


def _prompt_for_blessed_values(
    conflicting_attributes: list[ConflictCheckedAttribute],
) -> BlessedIdentityValues:
    """Prompts for a value for each conflicting attribute and returns them as
    BlessedIdentityValues. An empty response leaves the attribute None, so the
    kept cluster stores null for it. A response that fails to parse re-prompts
    for the same attribute rather than ending the session."""
    values: dict[str, str | datetime.date | Sex | Gender | Ethnicity] = {}
    for attribute in conflicting_attributes:
        while True:
            raw = input(
                f"Blessed value for [{attribute.value}] (leave empty to store null): "
            ).strip()
            if not raw:
                break
            try:
                values[attribute.value] = _parse_blessed_value(attribute, raw)
                break
            except ValueError as e:
                print(e)
    return BlessedIdentityValues(**values)  # type: ignore[arg-type]


def _parse_blessed_value(
    attribute: ConflictCheckedAttribute, raw: str
) -> str | datetime.date | Sex | Gender | Ethnicity:
    """Parses a reviewer's typed value for one conflicting attribute into the
    type that attribute stores. Enum values parse case-insensitively."""
    if attribute is ConflictCheckedAttribute.BIRTHDATE:
        return datetime.date.fromisoformat(raw)
    if attribute is ConflictCheckedAttribute.SEX:
        return parse_enum(Sex, raw)
    if attribute is ConflictCheckedAttribute.GENDER:
        return parse_enum(Gender, raw)
    if attribute is ConflictCheckedAttribute.ETHNICITY:
        return parse_enum(Ethnicity, raw)
    return raw


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parses the script's command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tenant",
        type=Tenant,
        required=True,
        help="Tenant whose cluster is being overridden.",
    )
    parser.add_argument(
        "--disposition",
        type=str.upper,
        choices=[d.value for d in IdentityClusterOverrideDisposition],
        required=True,
        help="Whether to keep the cluster with recorded values (BLESS) or drop "
        "it (EXCLUDE). Case-insensitive.",
    )
    parser.add_argument(
        "--external-id",
        dest="external_ids",
        type=_parse_external_id,
        action="append",
        required=True,
        metavar="VALUE:TYPE",
        help="An external id on the cluster, as value:type. Repeat for each id.",
    )
    parser.add_argument(
        "--person-type",
        type=str.upper,
        choices=[p.value for p in PersonType],
        help="Person type of the cluster. Required for an EXCLUDE override; "
        "derived from the rejected cluster for a BLESS override.",
    )
    parser.add_argument(
        "--recorded-by",
        required=True,
        help="Who is recording this override.",
    )
    parser.add_argument(
        "--note",
        help="Justification for the override. Prompted for if omitted.",
    )
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        help="Project holding the identity_cluster_override table.",
    )
    parser.add_argument(
        "--sandbox-dataset-prefix",
        help="Write to the sandbox overrides dataset with this prefix (and, for "
        "a BLESS, read the sandbox rejections dataset) instead of the tenant's "
        "real ones.",
    )
    return parser.parse_args(argv)


def _parse_external_id(raw: str) -> tuple[str, str]:
    """Parses a "value:type" external id argument into an (external_id, id_type)
    pair."""
    external_id, separator, id_type = raw.partition(":")
    if not separator or not external_id or not id_type:
        raise argparse.ArgumentTypeError(
            f"Expected an external id of the form value:type, found [{raw}]."
        )
    return external_id, id_type


if __name__ == "__main__":
    main(sys.argv[1:])
