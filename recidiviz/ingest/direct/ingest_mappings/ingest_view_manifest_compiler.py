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
from typing import Callable, Dict, Iterator, List, Optional, Set, Type, Union

import attr
from more_itertools import one

from recidiviz.common.common_utils import bidirectional_set_difference
from recidiviz.ingest.direct.ingest_mappings import yaml_schema
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    BooleanLiteralManifest,
    EntityTreeManifest,
    EntityTreeManifestFactory,
    ManifestNode,
    VariableManifestNode,
    build_manifest_from_raw_typed,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegate,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.utils.yaml_dict import YAMLDict

# This key tracks the version number for the actual mappings manifest structure,
# allowing us to gate any breaking changes in the file syntax etc.
MANIFEST_LANGUAGE_VERSION_KEY = "manifest_language"


@attr.define(kw_only=True, frozen=True)
class IngestViewManifest:
    """Class containing instruction for how to parse any given result row from the
    given ingest view.
    """

    ingest_view_name: str
    manifest_language_version: str
    input_columns: List[str]
    unused_columns: List[str]
    should_launch_manifest: ManifestNode[bool]
    output: EntityTreeManifest

    def should_launch(self, context: IngestViewContentsContext) -> bool:
        should_launch_value = self.should_launch_manifest.build_from_row({}, context)
        return should_launch_value if should_launch_value is not None else True

    def hydrated_entity_classes(self) -> Set[Type[Entity]]:
        return {
            node.entity_cls
            for node in self.output.all_nodes_referenced()
            if isinstance(node, EntityTreeManifest)
        }

    def parse_contents(
        self,
        *,
        contents_iterator: Iterator[Dict[str, str]],
        context: IngestViewContentsContext,
        result_callable: Optional[
            Callable[[int, Dict[str, str], Union[Entity, Exception]], None]
        ] = None,
    ) -> List[Entity]:
        """Parses query results from this manifest's ingest view into a list of
        entities.
        """
        result = []
        if not self.should_launch(context):
            raise ValueError(
                f"Cannot parse results for ingest view [{self.ingest_view_name}] "
                f"because should_launch is false."
            )

        for i, row in enumerate(contents_iterator):
            self._validate_row_columns(i, row, set(self.input_columns))

            try:
                output_tree = self.output.build_from_row(row, context)
            except Exception as e:
                if result_callable:
                    result_callable(i, row, e)
                    # If the callable does not raise the exception, just skip that result.
                    continue
                raise e

            if not output_tree:
                raise ValueError("Unexpected null output tree for row.")

            if result_callable:
                result_callable(i, row, output_tree)

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


class IngestViewManifestCompiler:
    """Class that can be used to compile the YAML mappings manifest file for the
    provided ingest view into a manifest object.
    """

    def __init__(self, delegate: IngestViewManifestCompilerDelegate):
        self.delegate = delegate

    def compile_manifest(self, *, ingest_view_name: str) -> IngestViewManifest:
        """Compiles the YAML mappings manifest file for the provided ingest view into
        an object that contains the hydrated AST (abstract syntax tree) for the output
        and other info that will help us parse the result rows from this ingest view
        into entities.
        """
        manifest_path = self.delegate.get_ingest_view_manifest_path(ingest_view_name)
        manifest_dict = YAMLDict.from_path(manifest_path)

        version = manifest_dict.pop(MANIFEST_LANGUAGE_VERSION_KEY, str)

        json_schema_dir_path = os.path.join(
            os.path.dirname(yaml_schema.__file__), version
        )
        if not os.path.exists(json_schema_dir_path):
            raise ValueError(f"Unsupported language version: [{version}]")

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

        raw_launch_env_manifest = manifest_dict.pop_dict_optional("launch_env")
        if raw_launch_env_manifest:
            should_launch_manifest = build_manifest_from_raw_typed(
                raw_field_manifest=raw_launch_env_manifest,
                delegate=self.delegate,
                variable_manifests=variable_manifests,
                expected_result_type=bool,
            )
        else:
            should_launch_manifest = BooleanLiteralManifest(value=True)

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

        return IngestViewManifest(
            ingest_view_name=ingest_view_name,
            manifest_language_version=version,
            input_columns=input_columns,
            unused_columns=unused_columns,
            should_launch_manifest=should_launch_manifest,
            output=output_manifest,
        )

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
                f"|input_columns|: {unlisted_referenced_columns}. This means that these "
                f"columns are used somewhere in your ingest mapping, but aren't included "
                f"in the |input_columns| list. Either add "
                f"{unlisted_referenced_columns} the |input_columns| list and be sure "
                f"they're returned by the ingest view, or remove its usage in the mapping."
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
