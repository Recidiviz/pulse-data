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
import datetime
import os
import re
import unittest
from abc import abstractmethod
from typing import Callable, Dict, List, Optional, Sequence
from unittest.mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.ingest.direct.direct_ingest_regions import (
    DirectIngestRegion,
    get_direct_ingest_region,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    EntityTreeManifest,
    EnumMappingManifest,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    MANIFEST_LANGUAGE_VERSION_KEY,
    IngestViewManifestCompiler,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
    ingest_view_manifest_dir,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import print_entity_trees
from recidiviz.tests.ingest.direct.fixture_util import DirectIngestTestFixturePath
from recidiviz.tests.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_test import (
    ingest_mappingest_json_schema_path,
)
from recidiviz.tests.test_debug_helpers import launch_entity_tree_html_diff_comparison
from recidiviz.utils import environment
from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    in_ci,
)
from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.utils.yaml_dict_validator import validate_yaml_matches_schema

YAML_LANGUAGE_SERVER_PRAGMA = re.compile(
    r"^# yaml-language-server: \$schema=(?P<schema_path>.*schema.json)$"
)

DEFAULT_UPDATE_DATETIME = datetime.datetime(2021, 4, 14, 0, 0, 0)


class StateIngestViewParserTestBase:
    """Base test class for ingest view parser tests."""

    @classmethod
    @abstractmethod
    def state_code(cls) -> StateCode:
        pass

    @classmethod
    def state_code_str_upper(cls) -> str:
        return cls.state_code().value.upper()

    @classmethod
    def region_code(cls) -> str:
        return cls.state_code().value.lower()

    @property
    @abstractmethod
    def test(self) -> unittest.TestCase:
        pass

    def _region(self) -> DirectIngestRegion:
        return get_direct_ingest_region(self.region_code())

    @property
    def _manifest_compiler(self) -> IngestViewManifestCompiler:
        return IngestViewManifestCompiler(
            delegate=StateSchemaIngestViewManifestCompilerDelegate(
                region=self._region()
            )
        )

    def _run_parse_ingest_view_test(
        self,
        ingest_view_name: str,
        expected_output: Sequence[Entity],
        # TODO(#30495): Get data types/converters from YAMLs
        column_converters: Optional[Dict[str, Callable]] = None,
        debug: bool = False,
        project: str = GCP_PROJECT_STAGING,
    ) -> None:
        """Runs a test that parses the ingest view into Python entities.

        It reads the input from the following file:
        `recidiviz/tests/ingest/direct/direct_ingest_fixtures/ux_xx/{ingest_view_name}.csv`

        column_converters is a dictionary mapping {column_name: callable converter}.
          - The column name is the *pre-parsed* name of an ingest view column, not the
            name of the entity field it gets parsed into.
          - The callable is a callable that will convert the column value to the
            expected type that the values produced by this ingest view will be.
            This can be used in cases where the csv.DictReader "smartly" auto-converts
            values to a different type (e.g. an integer) that is incorrect.
        """
        # TODO(#15801): Move the fixture files to `ingest_view` subdirectory.
        self._check_test_matches_file_tag(ingest_view_name)

        in_gcp_staging = project == GCP_PROJECT_STAGING
        in_gcp_prod = project == GCP_PROJECT_PRODUCTION
        fixture_path = DirectIngestTestFixturePath.for_extract_and_merge_fixture(
            region_code=self.region_code(),
            file_name=f"{ingest_view_name}.csv",
        ).full_path()
        with patch.object(
            environment, "in_gcp_staging", return_value=in_gcp_staging
        ), patch.object(environment, "in_gcp_production", return_value=in_gcp_prod):
            manifest = self._manifest_compiler.compile_manifest(
                ingest_view_name=ingest_view_name
            )
            fixture_content = list(
                csv.DictReader(
                    LocalFileContentsHandle(
                        fixture_path, cleanup_file=False
                    ).get_contents_iterator()
                )
            )
            # TODO(#30495): Get this from YAMLs
            if column_converters:
                for row in fixture_content:
                    for field, converter in column_converters.items():
                        row[field] = converter(row[field])
            parsed_output = manifest.parse_contents(
                contents_iterator=fixture_content,
                context=IngestViewContentsContextImpl.build_for_tests(),
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
            launch_entity_tree_html_diff_comparison(
                found_root_entities=parsed_output,
                expected_root_entities=expected_output,
                region_code=self.region_code(),
                print_tree_structure_only=False,
            )

        self.test.assertEqual(expected_output, parsed_output)

    def _ingest_view_manifest_paths(self) -> List[str]:
        region = self._region()
        manifest_dir = ingest_view_manifest_dir(region)

        result = []
        for file in os.listdir(manifest_dir):
            if file in ("__init__.py", "__pycache__"):
                continue
            manifest_path = os.path.join(manifest_dir, file)
            result.append(manifest_path)
        return result

    def test_all_ingest_view_manifests_conform_to_schema(self) -> None:
        """
        Validates ingest view mapping YAML files against our JSON schema.
        We want to do this validation so that we
        don't forget to add JSON schema (and therefore
        IDE) support for new features in the language.
        """
        if self.state_code() == StateCode.US_XX:
            # Skip template region
            return
        for manifest_path in self._ingest_view_manifest_paths():
            manifest_dict = YAMLDict.from_path(manifest_path)
            version = manifest_dict.peek(MANIFEST_LANGUAGE_VERSION_KEY, str)
            validate_yaml_matches_schema(
                yaml_dict=manifest_dict,
                json_schema_path=ingest_mappingest_json_schema_path(version),
            )

    def test_all_ingest_view_manifests_parse(self) -> None:
        if self.state_code() == StateCode.US_XX:
            # Skip template region
            return
        region = self._region()
        collector = IngestViewManifestCollector(
            region=region,
            delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
        )
        for manifest in collector.ingest_view_to_manifest.values():
            manifest_ast = manifest.output
            self.test.assertIsInstance(manifest_ast, EntityTreeManifest)

    def test_all_ingest_view_manifests_are_tested(self) -> None:
        if self.state_code() == StateCode.US_XX:
            # Skip template region
            return

        for manifest_path in self._ingest_view_manifest_paths():
            self._check_manifest_has_parser_test(manifest_path)

    def test_all_ingest_view_manifests_define_schema_pragma(self) -> None:
        if self.state_code() == StateCode.US_XX:
            # Skip template region
            return

        for manifest_path in self._ingest_view_manifest_paths():
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


class EnumManifestParsingTestCase(unittest.TestCase):
    """This TestCase serves as the base for testing complex parsing of enums from raw text."""

    @property
    def state_code(self) -> StateCode:
        raise NotImplementedError("Ensure your subclass has a StateCode defined!")

    @property
    def _manifest_compiler(self) -> IngestViewManifestCompiler:
        return IngestViewManifestCompiler(
            delegate=StateSchemaIngestViewManifestCompilerDelegate(
                region=get_direct_ingest_region(self.state_code.value)
            )
        )

    def parse_manifest(self, ingest_view_name: str) -> EntityTreeManifest:
        manifest = self._manifest_compiler.compile_manifest(
            ingest_view_name=ingest_view_name
        )
        return manifest.output

    def run_enum_manifest_parsing_test(
        self, file_tag: str, enum_parser_manifest: EnumMappingManifest
    ) -> None:
        fixture_path = DirectIngestTestFixturePath.for_enum_raw_text_fixture(
            region_code=self.state_code.value,
            file_name=f"{file_tag}.csv",
        ).full_path()

        contents_handle = LocalFileContentsHandle(fixture_path, cleanup_file=False)
        for row in csv.DictReader(contents_handle.get_contents_iterator()):
            _ = enum_parser_manifest.build_from_row(
                row,
                context=IngestViewContentsContextImpl.build_for_tests(),
            )
