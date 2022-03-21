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
"""Base test class for ingest view parser tests."""
import csv
import os
import re
import unittest
from abc import abstractmethod
from typing import List, Sequence

from recidiviz.common.constants.states import StateCode
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    EntityTreeManifest,
    EnumMappingManifest,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser import (
    IngestViewResultsParser,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IngestViewResultsParserDelegateImpl,
    ingest_view_manifest_dir,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import print_entity_trees
from recidiviz.tests.ingest.direct.fixture_util import direct_ingest_fixture_path
from recidiviz.utils.environment import in_ci
from recidiviz.utils.regions import Region, get_region
from recidiviz.utils.yaml_dict import YAMLDict

YAML_LANGUAGE_SERVER_PRAGMA = re.compile(
    r"^# yaml-language-server: \$schema=(?P<schema_path>.*schema.json)$"
)


class StateIngestViewParserTestBase:
    """Base test class for ingest view parser tests."""

    @classmethod
    @abstractmethod
    def schema_type(cls) -> SchemaType:
        pass

    @classmethod
    @abstractmethod
    def region_code(cls) -> str:
        pass

    @property
    @abstractmethod
    def test(self) -> unittest.TestCase:
        pass

    def _region(self) -> Region:
        return get_region(
            self.region_code(),
            is_direct_ingest=True,
        )

    @classmethod
    def _main_ingest_instance(cls) -> DirectIngestInstance:
        # We assume we're ingesting into the SECONDARY ingest instance, which
        # should always have the latest ingest logic updates released to it.
        return DirectIngestInstance.SECONDARY

    def _build_parser(self) -> IngestViewResultsParser:
        region = self._region()
        return IngestViewResultsParser(
            delegate=IngestViewResultsParserDelegateImpl(
                region, self.schema_type(), self._main_ingest_instance()
            )
        )

    def _parse_manifest(self, file_tag: str) -> EntityTreeManifest:
        parser = self._build_parser()
        manifest_ast, _ = parser.parse_manifest(
            manifest_path=parser.delegate.get_ingest_view_manifest_path(file_tag),
        )
        return manifest_ast

    def _parse_enum_manifest_test(
        self, file_tag: str, enum_parser_manifest: EnumMappingManifest
    ) -> None:
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code(),
            file_name=f"{file_tag}.csv",
        )

        contents_handle = LocalFileContentsHandle(fixture_path, cleanup_file=False)
        for row in csv.DictReader(contents_handle.get_contents_iterator()):
            _ = enum_parser_manifest.build_from_row(row)

    def _run_parse_ingest_view_test(
        self,
        ingest_view_name: str,
        expected_output: Sequence[Entity],
        debug: bool = False,
    ) -> None:
        """Runs a test that parses the ingest view into Python entities."""
        self._check_test_matches_file_tag(ingest_view_name)

        parser = self._build_parser()
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code(),
            file_name=f"{ingest_view_name}.csv",
        )
        parsed_output = parser.parse(
            ingest_view_name=ingest_view_name,
            contents_iterator=csv.DictReader(
                LocalFileContentsHandle(
                    fixture_path, cleanup_file=False
                ).get_contents_iterator()
            ),
        )

        if debug:
            if in_ci():
                self.test.fail(
                    "The |debug| flag should only be used for local debugging."
                )

            print("============== EXPECTED ==============")
            print_entity_trees(expected_output)
            print("============== ACTUAL ==============")
            print_entity_trees(parsed_output)

        self.test.assertEqual(expected_output, parsed_output)

    # TODO(#8905): Rename this function once all manifests have been migrated to v2
    #  mappings and have test_all_ingest_view_manifests_parse do the parsing.
    def _v2_manifest_paths(self) -> List[str]:
        region = self._region()
        manifest_dir = ingest_view_manifest_dir(region)

        result = []
        for file in os.listdir(manifest_dir):
            if file == "__init__.py":
                continue
            manifest_path = os.path.join(manifest_dir, file)
            manifest_dict = YAMLDict.from_path(manifest_path)
            if "manifest_language" not in manifest_dict.keys():
                # TODO(#8905): Remove this check once all files have been migrated
                #  to v2 structure.
                continue
            result.append(manifest_path)
        return result

    def test_all_ingest_view_manifests_parse(self) -> None:
        if self.region_code() == StateCode.US_XX.value:
            # Skip template region
            return

        # Make sure building all manifests doesn't crash
        parser = self._build_parser()
        for manifest_path in self._v2_manifest_paths():
            manifest_ast, _ = parser.parse_manifest(manifest_path)
            self.test.assertIsInstance(manifest_ast, EntityTreeManifest)

    def test_all_ingest_view_manifests_are_tested(self) -> None:
        if self.region_code() == StateCode.US_XX.value:
            # Skip template region
            return

        for manifest_path in self._v2_manifest_paths():
            self._check_manifest_has_parser_test(manifest_path)

    def test_all_ingest_view_manifests_define_schema_pragma(self) -> None:
        if self.region_code() == StateCode.US_XX.value:
            # Skip template region
            return

        for manifest_path in self._v2_manifest_paths():
            with open(manifest_path, encoding="utf-8") as f:
                line = f.readline()
                match = re.match(YAML_LANGUAGE_SERVER_PRAGMA, line.strip())
                if not match:
                    raise ValueError(
                        f"First line of manifest file [{manifest_path}] does not match "
                        f"expected pattern."
                    )
                relative_schema_path = match.group("schema_path")
                abs_schema_path = os.path.normpath(
                    os.path.join(os.path.dirname(manifest_path), relative_schema_path)
                )
                if not os.path.exists(abs_schema_path):
                    raise ValueError(f"Schema path [{abs_schema_path}] does not exist.")

    def _check_manifest_has_parser_test(self, manifest_path: str) -> None:
        """Validates that manifest at the given path has an associated test defined in
        this test class.
        """
        file_tag = self._file_tag_for_manifest(manifest_path)
        expected_test_name = self._expected_test_name_for_tag(file_tag)

        all_tests = [t for t in dir(self) if t.startswith("test_")]
        if expected_test_name not in all_tests:
            self.test.fail(
                f"Missing test for [{file_tag}] - expected test with name "
                f"[{expected_test_name}]",
            )

    def _file_tag_for_manifest(self, manifest_path: str) -> str:
        """Parses the ingest view file tag from the ingest view manifest path."""
        _, manifest_file = os.path.split(manifest_path)
        manifest_name, _ = os.path.splitext(manifest_file)

        return manifest_name[len(self.region_code()) + 1 :]

    @staticmethod
    def _expected_test_name_for_tag(file_tag: str) -> str:
        """Returns the name we expect for the parser test for this file tag."""
        return f"test_parse_{file_tag}"

    def _check_test_matches_file_tag(self, file_tag: str) -> None:
        """Validates that the file tag that is being processed is the expected file tag
        given the current test name.
        """
        expected_test_name = self._expected_test_name_for_tag(file_tag)
        actual_test_name = self.test._testMethodName  # pylint: disable=protected-access

        if actual_test_name != expected_test_name:
            self.test.fail(
                f"Unexpected test name [{actual_test_name}] for file_tag "
                f"[{file_tag}]. Expected [{expected_test_name}]."
            )
