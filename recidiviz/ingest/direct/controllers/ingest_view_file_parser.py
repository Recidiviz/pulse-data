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
"""Class that parses ingest view file contents into entities based on the manifest file
 for this ingest view.
"""
import csv
from enum import Enum, auto
from typing import Dict, Iterator, List, Set, Tuple

from more_itertools import one

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.common.common_utils import bidirectional_set_difference
from recidiviz.ingest.direct.controllers.ingest_view_file_parser_delegate import (
    IngestViewFileParserDelegate,
)
from recidiviz.ingest.direct.controllers.ingest_view_manifest import (
    EntityTreeManifest,
    EntityTreeManifestFactory,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.utils.yaml_dict import YAMLDict


class FileFormat(Enum):
    CSV = auto()


# This key tracks the version number for the actual mappings manifest structure,
# allowing us to gate any breaking changes in the file syntax etc.
MANIFEST_LANGUAGE_VERSION_KEY = "manifest_language"

# Minimum supported value in MANIFEST_LANGUAGE_VERSION_KEY field
MIN_LANGUAGE_VERSION = "1.0.0"

# Maximum supported value in MANIFEST_LANGUAGE_VERSION_KEY field
MAX_LANGUAGE_VERSION = "1.0.0"


class IngestViewFileParser:
    """Class that parses ingest view file contents into entities based on the manifest
    file for this ingest view.
    """

    def __init__(self, delegate: IngestViewFileParserDelegate):
        self.delegate = delegate

    @staticmethod
    def _row_iterator(
        contents_handle: GcsfsFileContentsHandle, file_format: FileFormat
    ) -> Iterator[Dict]:
        if file_format == FileFormat.CSV:
            return csv.DictReader(contents_handle.get_contents_iterator())
        raise ValueError(f"Unsupported file format: [{file_format}].")

    def parse(
        self,
        *,
        file_tag: str,
        contents_handle: GcsfsFileContentsHandle,
        file_format: FileFormat,
    ) -> List[Entity]:
        """Parses ingest view file contents into entities based on the manifest file for
        this ingest view.
        """

        manifest_path = self.delegate.get_ingest_view_manifest_path(file_tag)
        output_manifest, expected_input_columns = self.parse_manifest(manifest_path)
        result = []
        for i, row in enumerate(self._row_iterator(contents_handle, file_format)):
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
        missing_from_manifest, missing_from_file = bidirectional_set_difference(
            input_columns, expected_columns
        )
        if missing_from_manifest:
            raise ValueError(
                f"Found columns in input file row [{row_number}] not present in manifest "
                f"|input_columns| list: {missing_from_manifest}"
            )
        if missing_from_file:
            raise ValueError(
                f"Found columns in manifest |input_columns| list that are missing from "
                f"file row [{row_number}]: {missing_from_file}"
            )

    def parse_manifest(self, manifest_path: str) -> Tuple[EntityTreeManifest, Set[str]]:
        """Parses the provided manifest, returning a hydrated AST for the output, as
        well as the set of expected input columns for any CSVs we use this manifest
        to parse.
        """
        manifest_dict = YAMLDict.from_path(manifest_path)

        version = manifest_dict.pop(MANIFEST_LANGUAGE_VERSION_KEY, str)
        if not MIN_LANGUAGE_VERSION <= version <= MAX_LANGUAGE_VERSION:
            raise ValueError(f"Unsupported language version: [{version}]")

        # TODO(#8981): Add logic to enforce that version changes are accompanied with
        #  proper migrations / reruns.
        input_columns = manifest_dict.pop("input_columns", list)
        unused_columns = manifest_dict.pop("unused_columns", list)

        raw_entity_manifest = manifest_dict.pop_dict("output")
        entity_cls_name = one(raw_entity_manifest.keys())
        entity_cls = self.delegate.get_entity_cls(entity_cls_name=entity_cls_name)
        output_manifest = EntityTreeManifestFactory.from_raw_manifest(
            raw_fields_manifest=raw_entity_manifest.pop_dict(entity_cls_name),
            delegate=self.delegate,
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
