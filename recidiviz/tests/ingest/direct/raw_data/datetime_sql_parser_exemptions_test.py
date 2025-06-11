# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for datetime_sql_parser_exemptions.py"""
import unittest
from enum import Enum, auto
from pprint import pformat

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_documentation_generator import (
    DirectIngestDocumentationGenerator,
)
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.raw_data.datetime_sql_parser_exemptions import (
    DATETIME_PARSER_EXEMPTIONS_FILES_REFERENCED_IN_DOWNSTREAM_VIEWS_ONLY,
    DATETIME_PARSER_EXEMPTIONS_FILES_REFERENCED_IN_INGEST_VIEWS_AND_DOWNSTREAM_VIEWS,
    DATETIME_PARSER_EXEMPTIONS_FILES_REFERENCED_IN_INGEST_VIEWS_ONLY,
    DATETIME_PARSER_EXEMPTIONS_NO_DOWNSTREAM_REFERENCES,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.tools.raw_data_reference_reasons_yaml_loader import (
    RawDataReferenceReasonsYamlLoader,
)


class FileTagReferenceType(Enum):
    """Enum telling us where a raw data file tag is referenced downstream."""

    NO_REFERENCES = auto()
    INGEST_VIEWS_ONLY = auto()
    DOWNSTREAM_VIEWS_ONLY = auto()
    INGEST_VIEW_AND_DOWNSTREAM = auto()

    def exemptions_map(self) -> dict[StateCode, dict[str, list[str]]]:
        if self is FileTagReferenceType.NO_REFERENCES:
            return DATETIME_PARSER_EXEMPTIONS_NO_DOWNSTREAM_REFERENCES
        if self is FileTagReferenceType.INGEST_VIEWS_ONLY:
            return DATETIME_PARSER_EXEMPTIONS_FILES_REFERENCED_IN_INGEST_VIEWS_ONLY
        if self is FileTagReferenceType.DOWNSTREAM_VIEWS_ONLY:
            return DATETIME_PARSER_EXEMPTIONS_FILES_REFERENCED_IN_DOWNSTREAM_VIEWS_ONLY
        if self is FileTagReferenceType.INGEST_VIEW_AND_DOWNSTREAM:
            return DATETIME_PARSER_EXEMPTIONS_FILES_REFERENCED_IN_INGEST_VIEWS_AND_DOWNSTREAM_VIEWS
        raise ValueError(f"Unexpected reference type {self}")

    def expected_exemptions_map_name(self) -> str:
        if self is FileTagReferenceType.NO_REFERENCES:
            return "DATETIME_PARSER_EXEMPTIONS_NO_DOWNSTREAM_REFERENCES"
        if self is FileTagReferenceType.INGEST_VIEWS_ONLY:
            return "DATETIME_PARSER_EXEMPTIONS_FILES_REFERENCED_IN_INGEST_VIEWS_ONLY"
        if self is FileTagReferenceType.DOWNSTREAM_VIEWS_ONLY:
            return (
                "DATETIME_PARSER_EXEMPTIONS_FILES_REFERENCED_IN_DOWNSTREAM_VIEWS_ONLY"
            )
        if self is FileTagReferenceType.INGEST_VIEW_AND_DOWNSTREAM:
            return "DATETIME_PARSER_EXEMPTIONS_FILES_REFERENCED_IN_INGEST_VIEWS_AND_DOWNSTREAM_VIEWS"
        raise ValueError(f"Unexpected reference type {self}")

    def sentence_string(self) -> str:
        if self is FileTagReferenceType.NO_REFERENCES:
            return "not referenced in ingest views or downstream views"
        if self is FileTagReferenceType.INGEST_VIEWS_ONLY:
            return "referenced only in ingest views"
        if self is FileTagReferenceType.DOWNSTREAM_VIEWS_ONLY:
            return "referenced only in downstream BQ views (not ingest views)"
        if self is FileTagReferenceType.INGEST_VIEW_AND_DOWNSTREAM:
            return "referenced in both ingest views and downstream views"
        raise ValueError(f"Unexpected reference type {self}")


class TestDatetimeSqlParserExemptions(unittest.TestCase):
    """Tests for datetime_sql_parser_exemptions.py"""

    file_tags_referenced_in_ingest_views: dict[StateCode, set[str]]
    file_tags_referenced_in_downstream_views: dict[StateCode, set[str]]

    @classmethod
    def setUpClass(cls) -> None:
        cls.file_tags_referenced_in_ingest_views = {
            state_code: cls._get_file_tags_referenced_in_ingest_views(state_code)
            for state_code in get_existing_direct_ingest_states()
        }
        cls.file_tags_referenced_in_downstream_views = {
            state_code: cls._get_file_tags_referenced_in_downstream_views(state_code)
            for state_code in get_existing_direct_ingest_states()
        }

    @staticmethod
    def _get_file_tags_referenced_in_ingest_views(state_code: StateCode) -> set[str]:
        view_collector = DirectIngestViewQueryBuilderCollector(
            get_direct_ingest_region(region_code=state_code.value), []
        )
        return set(
            DirectIngestDocumentationGenerator.get_referencing_views(view_collector)
        )

    @staticmethod
    def _get_file_tags_referenced_in_downstream_views(
        state_code: StateCode,
    ) -> set[str]:
        return set(
            RawDataReferenceReasonsYamlLoader.get_downstream_referencing_views(
                state_code
            )
        )

    def get_file_tag_reference_type(
        self, state_code: StateCode, file_tag: str
    ) -> FileTagReferenceType:
        in_ingest_views = (
            file_tag in self.file_tags_referenced_in_ingest_views[state_code]
        )
        in_downstream_views = (
            file_tag in self.file_tags_referenced_in_downstream_views[state_code]
        )
        if in_ingest_views and in_downstream_views:
            return FileTagReferenceType.INGEST_VIEW_AND_DOWNSTREAM
        if in_ingest_views:
            return FileTagReferenceType.INGEST_VIEWS_ONLY
        if in_downstream_views:
            return FileTagReferenceType.DOWNSTREAM_VIEWS_ONLY
        return FileTagReferenceType.NO_REFERENCES

    def _run_find_unnecessary_exemptions_test_for_type(
        self, reference_type: FileTagReferenceType
    ) -> None:
        exemptions_map = reference_type.exemptions_map()
        for state_code in get_existing_direct_ingest_states():
            if state_code not in exemptions_map:
                continue

            region_raw_file_config = get_region_raw_file_config(
                region_code=state_code.value.lower(),
            )
            bad_exemptions = {}
            for file_tag, exempt_columns in exemptions_map[state_code].items():
                bad_exemptions_for_file_tag = [
                    column.name
                    for column in region_raw_file_config.raw_file_configs[
                        file_tag
                    ].current_columns
                    if column.datetime_sql_parsers and column.name in exempt_columns
                ]

                if bad_exemptions_for_file_tag:
                    bad_exemptions[file_tag] = sorted(bad_exemptions_for_file_tag)
            if bad_exemptions:
                raise ValueError(
                    f"Found file_tags with columns that are exempt from having valid"
                    f"datetime_sql_parsers but which now DO have valid parsers. Please "
                    f"remove these from "
                    f"{reference_type.expected_exemptions_map_name()}:\n"
                    f"{pformat(bad_exemptions, indent=4)}"
                )

    def _run_exemptions_categorized_correctly_test_for_type(
        self, reference_type: FileTagReferenceType
    ) -> None:
        """Runs a test to check that exemptions in the map associated with raw data
        reference type |reference_type| are actually in the appropriate exemptions map.
        """
        exemptions_map = reference_type.exemptions_map()
        for state_code in get_existing_direct_ingest_states():
            if state_code not in exemptions_map:
                continue

            file_tags_in_no_references_list = exemptions_map[state_code]
            for file_tag in file_tags_in_no_references_list:
                actual_reference_type = self.get_file_tag_reference_type(
                    state_code, file_tag
                )
                if actual_reference_type is reference_type:
                    continue

                if reference_type is FileTagReferenceType.NO_REFERENCES:
                    # In this case, this person is referencing this data for the first
                    # time. We should force them to add parsers for this file in this
                    # case.
                    action_string = (
                        "Please add datetime_sql_parsers to all columns in this file "
                        "before referencing this data downstream. You can use "
                        "recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers "
                        "to help hydrate these."
                    )
                else:
                    action_string = (
                        f"Please move the exemptions for this file to "
                        f"{actual_reference_type.expected_exemptions_map_name()}."
                    )

                raise ValueError(
                    f"Found [{state_code.value}] file_tag [{file_tag}] with entries in "
                    f"{reference_type.expected_exemptions_map_name()}"
                    f" but which is now {actual_reference_type.sentence_string()}. "
                    f"{action_string}"
                )

    def test_exemptions_for_files_referenced_in_ingest_views_and_downstream_views(
        self,
    ) -> None:
        self._run_exemptions_categorized_correctly_test_for_type(
            FileTagReferenceType.INGEST_VIEW_AND_DOWNSTREAM
        )
        self._run_find_unnecessary_exemptions_test_for_type(
            FileTagReferenceType.INGEST_VIEW_AND_DOWNSTREAM
        )

    def test_exemptions_for_files_only_in_ingest_views(self) -> None:
        self._run_exemptions_categorized_correctly_test_for_type(
            FileTagReferenceType.INGEST_VIEWS_ONLY
        )
        self._run_find_unnecessary_exemptions_test_for_type(
            FileTagReferenceType.INGEST_VIEWS_ONLY
        )

    def test_exemptions_for_files_only_in_downstream_views(self) -> None:
        self._run_exemptions_categorized_correctly_test_for_type(
            FileTagReferenceType.DOWNSTREAM_VIEWS_ONLY
        )
        self._run_find_unnecessary_exemptions_test_for_type(
            FileTagReferenceType.DOWNSTREAM_VIEWS_ONLY
        )

    def test_exemptions_for_files_not_referenced_in_any_views(self) -> None:
        self._run_exemptions_categorized_correctly_test_for_type(
            FileTagReferenceType.NO_REFERENCES
        )
        self._run_find_unnecessary_exemptions_test_for_type(
            FileTagReferenceType.NO_REFERENCES
        )
