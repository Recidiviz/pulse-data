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
"""Checks the declared schema of every deployed view against our column naming and
documentation conventions, once per data platform project."""
import unittest
from typing import Literal, Sequence

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_column import (
    COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
    BigQueryViewColumn,
    Record,
)
from recidiviz.calculator.query.state.views.reference.product_display_person_external_ids import (
    PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_stable_person_external_ids import (
    PRODUCT_STABLE_PERSON_EXTERNAL_IDS_VIEW_BUILDER,
)
from recidiviz.common import attr_validators
from recidiviz.ingest.views.dataset_config import STATE_BASE_VIEWS_DATASET
from recidiviz.metrics.export.exported_view_utils import (
    get_all_metric_export_view_addresses,
)
from recidiviz.tests.big_query.known_undocumented_columns import (
    KNOWN_UNDOCUMENTED_COLUMNS,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import is_meaningful_docstring
from recidiviz.validation.views.view_config import (
    get_view_builders_for_views_to_update as get_validation_view_builders,
)
from recidiviz.view_registry.deployed_address_schema_utils import (
    get_deployed_addresses_without_state_code_column,
)
from recidiviz.view_registry.deployed_view_external_id_exemptions import (
    NORMALIZED_STATE_VIEWS_DATASET,
    get_known_non_export_views_with_person_external_id_column,
    get_known_views_with_unqualified_external_id,
)
from recidiviz.view_registry.deployed_view_graphs import deployed_view_graph_registry


@attr.define(frozen=True, kw_only=True)
class _ProjectViewSchemas:
    """The declared view schemas and schema exemption lists for one project."""

    project_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The project whose deployed view graphs these schemas come from."""

    declared_schemas_by_address: dict[
        BigQueryAddress, Sequence[BigQueryViewColumn]
    ] = attr.ib(validator=attr_validators.is_dict_of(BigQueryAddress, (list, tuple)))
    """Declared schema of every view deployed to this project."""

    known_no_state_code_addresses: set[BigQueryAddress] = attr.ib(
        validator=attr_validators.is_set_of(BigQueryAddress)
    )
    """Addresses exempt from the state_code column requirement."""

    known_has_external_id_addresses: set[BigQueryAddress] = attr.ib(
        validator=attr_validators.is_set_of(BigQueryAddress)
    )
    """Addresses allowed to have a column named exactly external_id."""

    known_non_export_views_with_person_external_id: set[BigQueryAddress] = attr.ib(
        validator=attr_validators.is_set_of(BigQueryAddress)
    )
    """Non-export addresses allowed to have a column ending in person_external_id."""

    allowed_has_person_external_id_addresses: set[BigQueryAddress] = attr.ib(
        validator=attr_validators.is_set_of(BigQueryAddress)
    )
    """Addresses that may have a column ending in person_external_id, and may also
    lack one."""


def _build_project_view_schemas(project_id: str) -> _ProjectViewSchemas:
    """Returns the declared schemas and exemption lists for every view deployed
    to this project."""
    with local_project_id_override(project_id):
        registry = deployed_view_graph_registry(project_id)
        declared_schemas_by_address = {
            vb.address: vb.build().schema
            for graph in registry.view_graphs
            for vb in graph.view_builders
        }
        allowed_has_person_external_id_addresses = (
            # Views that are part of metric exports are allowed to have
            # person_external_id columns (we will enforce in other tests that these
            # columns are properly named).
            get_all_metric_export_view_addresses()
            # It can be useful to have a person external id in a validation view output
            # (when paired with an id_type). Do not penalize if these have a
            # person_external_id column.
            | {vb.address for vb in get_validation_view_builders()}
            | {
                # These reference views have the external ids that should be pulled into
                # exported views at the very end but are not exported directly.
                PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_VIEW_BUILDER.address,
                PRODUCT_STABLE_PERSON_EXTERNAL_IDS_VIEW_BUILDER.address,
            }
            | {
                # These views just mirror our state/normalized_state schemas
                BigQueryAddress(
                    dataset_id=dataset,
                    table_id="state_person_external_id_view",
                )
                for dataset in [
                    NORMALIZED_STATE_VIEWS_DATASET,
                    STATE_BASE_VIEWS_DATASET,
                ]
            }
        )
        return _ProjectViewSchemas(
            project_id=project_id,
            declared_schemas_by_address=declared_schemas_by_address,
            known_no_state_code_addresses=get_deployed_addresses_without_state_code_column(
                project_id
            ),
            known_has_external_id_addresses=get_known_views_with_unqualified_external_id(
                project_id
            ),
            known_non_export_views_with_person_external_id=get_known_non_export_views_with_person_external_id_column(
                project_id
            ),
            allowed_has_person_external_id_addresses=allowed_has_person_external_id_addresses,
        )


def _split_by_has_field(
    declared_schemas_by_address: dict[BigQueryAddress, Sequence[BigQueryViewColumn]],
    field_name: str,
    match_type: Literal["exact", "suffix"],
) -> tuple[set[BigQueryAddress], set[BigQueryAddress]]:
    """Splits the addresses into those whose declared schema has a top-level column
    matching field_name and those that do not. The match_type controls whether we
    match on exact column names or suffixes (e.g. to match columns like
    stable_person_external_id and display_person_external_id).
    """
    does_not_have_field_addresses = set()
    has_field_addresses = set()
    for address, columns in declared_schemas_by_address.items():
        match match_type:
            case "exact":
                has_matching_field = any(field_name == c.name for c in columns)
            case "suffix":
                has_matching_field = any(c.name.endswith(field_name) for c in columns)
        if not has_matching_field:
            does_not_have_field_addresses.add(address)
        else:
            has_field_addresses.add(address)
    return has_field_addresses, does_not_have_field_addresses


def _bad_description_columns(columns: Sequence[BigQueryViewColumn]) -> list[str]:
    """Returns the names of declared columns (recursing into Records) whose
    description is not meaningful. Record subfield names are returned as
    "parent_name.subfield_name".
    """
    names: list[str] = []
    for col in columns:
        if (
            col.description == COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT
            or not is_meaningful_docstring(col.description)
        ):
            names.append(col.name)
        if isinstance(col, Record):
            for sub_name in _bad_description_columns(col.fields):
                names.append(f"{col.name}.{sub_name}")
    return names


class ViewSchemaValidationTest(unittest.TestCase):
    """Checks the declared schema of every deployed view against our column naming
    and documentation conventions, once per data platform project."""

    _project_view_schemas: list[_ProjectViewSchemas] = []

    @classmethod
    def setUpClass(cls) -> None:
        cls._project_view_schemas = [
            _build_project_view_schemas(project_id)
            for project_id in sorted(DATA_PLATFORM_GCP_PROJECTS)
        ]

    def test_views_all_have_state_code_column(self) -> None:
        for project in self._project_view_schemas:
            with self.subTest(project_id=project.project_id):
                self._verify_views_all_have_state_code_column(project)

    def _verify_views_all_have_state_code_column(
        self, project: _ProjectViewSchemas
    ) -> None:
        """Throws if we find any view that does not have a state_code column (and is
        not in our list of exempted views).
        """
        has_state_code_addresses, missing_state_code_addresses = _split_by_has_field(
            project.declared_schemas_by_address, "state_code", match_type="exact"
        )

        expected_missing_state_code_addresses = project.known_no_state_code_addresses

        unexpected_missing_state_code_addresses = (
            missing_state_code_addresses - expected_missing_state_code_addresses
        )
        if unexpected_missing_state_code_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_missing_state_code_addresses, indent_level=2
            )
            raise ValueError(
                f"Found unexpected views with no state_code column:{addresses_list}"
                f"\nIf there is an expected reason why these views don't have a "
                f"state_code column, add exemptions in "
                f"recidiviz/view_registry/deployed_address_schema_utils.py."
            )

        unexpected_has_state_code_addresses = has_state_code_addresses.intersection(
            expected_missing_state_code_addresses
        )
        if unexpected_has_state_code_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_has_state_code_addresses, indent_level=2
            )
            raise ValueError(
                f"Found views / tables that have state_code columns but are returned "
                f"by get_deployed_addresses_without_state_code_column() but which have "
                f"a state_code column:{addresses_list}\nThese should be removed from "
                f"that list."
            )

    def test_views_have_no_unqualified_external_id_columns(self) -> None:
        for project in self._project_view_schemas:
            with self.subTest(project_id=project.project_id):
                self._verify_views_have_no_unqualified_external_id_columns(project)

    def _verify_views_have_no_unqualified_external_id_columns(
        self, project: _ProjectViewSchemas
    ) -> None:
        """Validates that no view in our BQ view graph has a column named "external_id".
        All external id columns should have a qualified name, like sentence_external_id
        or stable_person_external_id.
        """
        has_external_id_addresses, no_external_id_addresses = _split_by_has_field(
            project.declared_schemas_by_address, "external_id", match_type="exact"
        )

        expected_has_external_id_addresses = project.known_has_external_id_addresses

        invalid_addresses = expected_has_external_id_addresses - set(
            project.declared_schemas_by_address
        )
        if invalid_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                invalid_addresses, indent_level=2
            )
            raise ValueError(
                f"Found addresses returned by "
                f"get_known_views_with_unqualified_external_id() which are not a valid "
                f"view address: {addresses_list}"
            )

        unexpected_has_external_id_addresses = (
            has_external_id_addresses - expected_has_external_id_addresses
        )
        if unexpected_has_external_id_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_has_external_id_addresses, indent_level=2
            )
            raise ValueError(
                f"Found unexpected views with an unqualified external_id column:"
                f"{addresses_list}\nThe name external_id is not specific enough and "
                f"can lead to confusion. Please rename to a more specific name, like "
                f"sentence_external_id or display_person_external_id. If there is an "
                f"expected reason why this view has an external_id column (rare!), "
                f"please discuss with someone on Doppler and then add it to "
                f"_KNOWN_VIEWS_WITH_UNQUALIFIED_EXTERNAL_ID_COLUMN."
            )

        unexpected_no_external_id_addresses = no_external_id_addresses.intersection(
            expected_has_external_id_addresses
        )
        if unexpected_no_external_id_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_no_external_id_addresses, indent_level=2
            )
            raise ValueError(
                f"Found views / tables that do not have an external_id columns but are "
                f"listed in _KNOWN_VIEWS_WITH_UNQUALIFIED_EXTERNAL_ID_COLUMN:"
                f"{addresses_list}\nThese should be removed from that list (yay!)."
            )

    def test_non_export_views_have_no_person_external_id_columns(self) -> None:
        for project in self._project_view_schemas:
            with self.subTest(project_id=project.project_id):
                self._verify_non_export_views_have_no_person_external_id_columns(
                    project
                )

    def _verify_non_export_views_have_no_person_external_id_columns(
        self, project: _ProjectViewSchemas
    ) -> None:
        """Validates that views not part of metric exports don't have columns matching
        the pattern *person_external_id. We should not pass external id information
        through our internal, foundational views but rather should join at the very end
        to get a relevant person external id.
        """
        (
            has_person_external_id_addresses,
            no_person_external_id_addresses,
        ) = _split_by_has_field(
            project.declared_schemas_by_address,
            "person_external_id",
            match_type="suffix",
        )

        expected_has_person_external_id_addresses = (
            project.allowed_has_person_external_id_addresses
            | project.known_non_export_views_with_person_external_id
        )

        unexpected_has_person_external_id_addresses = (
            has_person_external_id_addresses - expected_has_person_external_id_addresses
        )
        if unexpected_has_person_external_id_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_has_person_external_id_addresses, indent_level=2
            )
            raise ValueError(
                f"Found unexpected views with a *person_external_id column that are "
                f"not part of metric exports:{addresses_list}\n"
                f"We should not pass external id information through our internal, "
                f"foundational views but rather should join to one of the "
                f"product_display_person_external_id / product_stable_person_external_id "
                f"views at the very end to get a relevant person external id. If there "
                f"is an expected reason why this view has a *person_external_id "
                f"column, please discuss with someone on Doppler and then add it to "
                f"_KNOWN_NON_EXPORT_VIEWS_WITH_PERSON_EXTERNAL_ID_COLUMN in "
                f"recidiviz/view_registry/deployed_view_external_id_exemptions.py."
            )

        unexpected_no_person_external_id_addresses = (
            no_person_external_id_addresses.intersection(
                project.known_non_export_views_with_person_external_id
            )
        )
        if unexpected_no_person_external_id_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_no_person_external_id_addresses, indent_level=2
            )
            raise ValueError(
                f"Found views / tables that do not have a *person_external_id column "
                f"but are listed in "
                f"_KNOWN_NON_EXPORT_VIEWS_WITH_PERSON_EXTERNAL_ID_COLUMN:"
                f"{addresses_list}\nThese should be removed from that list (yay!)."
            )

    def test_meaningful_column_descriptions(self) -> None:
        for project in self._project_view_schemas:
            with self.subTest(project_id=project.project_id):
                self._verify_meaningful_column_descriptions(project)

    def _verify_meaningful_column_descriptions(
        self, project: _ProjectViewSchemas
    ) -> None:
        """Validates that every declared column description is "meaningful":
        non-empty, not indicating unfinished work, and not equal to the
        COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT sentinel. (view_address, column_name)
        pairs in KNOWN_UNDOCUMENTED_COLUMNS are exempted.
        """
        unexpected_bad: dict[BigQueryAddress, list[str]] = {}
        unexpected_fixed: dict[BigQueryAddress, list[str]] = {}

        for address, columns in project.declared_schemas_by_address.items():
            actual_set = set(_bad_description_columns(columns))
            expected_set = set(KNOWN_UNDOCUMENTED_COLUMNS.get(address, []))

            new_bad = sorted(actual_set - expected_set)
            if new_bad:
                unexpected_bad[address] = new_bad

            now_fixed = sorted(expected_set - actual_set)
            if now_fixed:
                unexpected_fixed[address] = now_fixed

        error_chunks: list[str] = []
        if unexpected_bad:
            lines = [
                f"  {addr.to_str()}: {cols}"
                for addr, cols in sorted(
                    unexpected_bad.items(),
                    key=lambda kv: (kv[0].dataset_id, kv[0].table_id),
                )
            ]
            error_chunks.append(
                "Found new columns with non-meaningful descriptions (empty, "
                "TO" + "DO/XXX prefix, or the COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT "
                "sentinel) that are not in KNOWN_UNDOCUMENTED_COLUMNS:\n"
                + "\n".join(lines)
                + "\n\nPlease write a real description for these columns. If there are "
                "known undocumented columns in the same view, please document those as "
                "well and remove them from the KNOWN_UNDOCUMENTED_COLUMNS exemption list."
            )
        if unexpected_fixed:
            lines = [
                f"  {addr.to_str()}: {cols}"
                for addr, cols in sorted(
                    unexpected_fixed.items(),
                    key=lambda kv: (kv[0].dataset_id, kv[0].table_id),
                )
            ]
            error_chunks.append(
                "Found columns listed in KNOWN_UNDOCUMENTED_COLUMNS that now "
                "have a meaningful description (yay!). Please remove them from "
                "recidiviz/tests/big_query/known_undocumented_columns.py:\n"
                + "\n".join(lines)
            )
        if error_chunks:
            raise ValueError("\n\n".join(error_chunks))
