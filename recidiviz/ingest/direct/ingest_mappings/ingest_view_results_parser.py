# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Class that parses the results of an ingest view query into entities based on the
manifest file for this ingest view.
"""
import os
from typing import Dict, Iterator, List, Set, Tuple

from more_itertools import one

from recidiviz.common.common_utils import bidirectional_set_difference
from recidiviz.ingest.direct.ingest_mappings import yaml_schema
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    EntityTreeManifest,
    EntityTreeManifestFactory,
    VariableManifestNode,
    build_manifest_from_raw_typed,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IngestViewResultsParserDelegate,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.utils import environment
from recidiviz.utils.yaml_dict import YAMLDict

# This key tracks the version number for the actual mappings manifest structure,
# allowing us to gate any breaking changes in the file syntax etc.
MANIFEST_LANGUAGE_VERSION_KEY = "manifest_language"


class IngestViewResultsParser:
    """Class that parses ingest view query results into entities based on the manifest
    file for this ingest view.
    """

    def __init__(self, delegate: IngestViewResultsParserDelegate):
        self.delegate = delegate

    def parse(
        self, *, ingest_view_name: str, contents_iterator: Iterator[Dict[str, str]]
    ) -> List[Entity]:
        """Parses ingest view query results into entities based on the manifest file for
        this ingest view.
        """

        manifest_path = self.delegate.get_ingest_view_manifest_path(ingest_view_name)
        output_manifest, expected_input_columns = self.parse_manifest(manifest_path)
        result = []
        for i, row in enumerate(contents_iterator):
            self._validate_row_columns(i, row, expected_input_columns)
            output_tree = output_manifest.build_from_row(row)
            if not output_tree:
                raise ValueError("Unexpected null output tree for row.")
            result.append(output_tree)
        return result

    @staticmethod
    def _validate_row_columns(
        row_number: int, row: Dict[str, str], expected_columns: Set[str]
    ) -> None:
        """Checks that columns in the row match the set of expected columns. Throws if
        there are missing or extra columns.
        """
        input_columns = set(row.keys())
        missing_from_manifest, missing_from_results = bidirectional_set_difference(
            input_columns, expected_columns
        )
        if missing_from_manifest:
            raise ValueError(
                f"Found columns in input results row [{row_number}] not present in "
                f"manifest |input_columns| list: {missing_from_manifest}"
            )
        if missing_from_results:
            raise ValueError(
                f"Found columns in manifest |input_columns| list that are missing from "
                f"results row [{row_number}]: {missing_from_results}"
            )

    def parse_manifest(self, manifest_path: str) -> Tuple[EntityTreeManifest, Set[str]]:
        """Parses the provided manifest, returning a hydrated AST (abstract syntax tree)
        for the output, as well as the set of expected input columns for any CSVs we use
        this manifest to parse.
        """
        manifest_dict = YAMLDict.from_path(manifest_path)

        # Don't pop manifest version key, otherwise schema won't validate
        version = manifest_dict.peek(MANIFEST_LANGUAGE_VERSION_KEY, str)

        json_schema_dir_path = os.path.join(
            os.path.dirname(yaml_schema.__file__), version
        )
        if not os.path.exists(json_schema_dir_path):
            raise ValueError(f"Unsupported language version: [{version}]")

        if not environment.in_gcp():
            # Run schema validation in tests / CI
            manifest_dict.validate(
                json_schema_path=os.path.join(
                    os.path.dirname(yaml_schema.__file__), version, "schema.json"
                )
            )
        _ = manifest_dict.pop(MANIFEST_LANGUAGE_VERSION_KEY, str)

        # TODO(#8981): Add logic to enforce that version changes are accompanied with
        #  proper migrations / reruns.
        input_columns = manifest_dict.pop("input_columns", list)
        unused_columns = manifest_dict.pop("unused_columns", list)

        raw_variable_manifests = manifest_dict.pop_dicts_optional("variables")
        variable_manifests: Dict[str, VariableManifestNode] = {}
        if raw_variable_manifests:
            for raw_variable_manifest in raw_variable_manifests:
                variable_name = one(raw_variable_manifest.keys())
                variable_manifest = VariableManifestNode(
                    variable_name=variable_name,
                    value_manifest=build_manifest_from_raw_typed(
                        raw_field_manifest=raw_variable_manifest.pop_dict(
                            variable_name
                        ),
                        delegate=self.delegate,
                        variable_manifests=variable_manifests,
                        expected_result_type=object,
                    ),
                )
                variable_manifests[variable_name] = variable_manifest

        raw_entity_manifest = manifest_dict.pop_dict("output")
        entity_cls_name = one(raw_entity_manifest.keys())
        entity_cls = self.delegate.get_entity_cls(entity_cls_name=entity_cls_name)
        output_manifest = EntityTreeManifestFactory.from_raw_manifest(
            raw_fields_manifest=raw_entity_manifest.pop_dict(entity_cls_name),
            delegate=self.delegate,
            variable_manifests=variable_manifests,
            entity_cls=entity_cls,
        )

        if len(manifest_dict):
            raise ValueError(
                f"Found unused keys in ingest view manifest: {manifest_dict.keys()}"
            )

        self._validate_input_columns_lists(
            input_columns_list=input_columns,
            unused_columns_list=unused_columns,
            referenced_columns=output_manifest.columns_referenced(),
        )

        self._validate_variables(
            input_variables=set(variable_manifests.keys()),
            referenced_variables=output_manifest.variables_referenced(),
        )

        return output_manifest, set(input_columns)

    @staticmethod
    def _validate_input_columns_lists(
        input_columns_list: List[str],
        unused_columns_list: List[str],
        referenced_columns: Set[str],
    ) -> None:
        """Validates that the |input_columns| and |unused_columns| manifests lists
        conform to expected structure and contain exactly the set of columns that are
        referenced in the |output| section of the manifest.
        """
        input_columns = set()
        for input_col in input_columns_list:
            if input_col in input_columns:
                raise ValueError(
                    f"Found item listed multiple times in |input_columns|: [{input_col}]"
                )
            input_columns.add(input_col)

        unused_columns = set()
        for unused_col in unused_columns_list:
            if unused_col in unused_columns:
                raise ValueError(
                    f"Found item listed multiple times in |unused_columns|: [{unused_col}]"
                )
            unused_columns.add(unused_col)

        for column in input_columns:
            if column.startswith("$"):
                raise ValueError(
                    f"Found column [{column}] that starts with protected "
                    f"character '$'. Adjust ingest view output column "
                    f"naming to remove the '$'."
                )

        (
            expected_referenced_columns,
            unexpected_unused_columns,
        ) = bidirectional_set_difference(input_columns, unused_columns)

        if unexpected_unused_columns:
            raise ValueError(
                f"Found values listed in |unused_columns| that were not also listed in "
                f"|input_columns|: {unexpected_unused_columns}"
            )

        (
            unlisted_referenced_columns,
            unreferenced_columns,
        ) = bidirectional_set_difference(
            referenced_columns, expected_referenced_columns
        )

        if unlisted_referenced_columns:
            raise ValueError(
                f"Found columns referenced in |output| that are not listed in "
                f"|input_columns|: {unlisted_referenced_columns}"
            )

        if unreferenced_columns:
            raise ValueError(
                f"Found columns listed in |input_columns| that are not referenced "
                f"in |output| or listed in |unused_columns|: "
                f"{unreferenced_columns}"
            )

    @staticmethod
    def _validate_variables(
        input_variables: Set[str], referenced_variables: Set[str]
    ) -> None:
        """Checks for variables that are defined but not used."""

        unreferenced_variables = input_variables.difference(referenced_variables)

        if unreferenced_variables:
            raise ValueError(
                f"Found variables listed in |variables| that are not referenced "
                f"in |output|: {unreferenced_variables}"
            )
