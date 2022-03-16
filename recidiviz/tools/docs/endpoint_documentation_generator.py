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
"""Class providing methods for generating documentation regarding endpoints.

The script can be run using pipenv shell:
    python -m recidiviz.tools.docs.endpoint_documentation_generator
"""
import logging
import os
import re
import sys
from collections import defaultdict
from functools import lru_cache
from typing import Dict, List, Set
from unittest import mock

from flask import Flask
from werkzeug.routing import Rule

from recidiviz.common.file_system import delete_files, get_all_files_recursive
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.docs.summary_file_generator import update_summary_file
from recidiviz.tools.docs.utils import DOCS_ROOT_PATH
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.string import StrictStringFormatter

ENDPOINT_DOCS_DIRECTORY = os.path.join(DOCS_ROOT_PATH, "endpoints")
ENDPOINT_CATALOG_SPECIFICATION = os.path.join(
    ENDPOINT_DOCS_DIRECTORY, "endpoint_specification_template.md"
)


class EndpointDocumentationGenerator:
    """Class providing methods for generating documentation regarding endpoints."""

    ROOT_ENDPOINT_FILE_NAME = "_root_"
    PATH_PARAMETERS_REGEX = re.compile(r"^<([a-z_]+)(:[a-z_]+)?>")
    MARKDOWN_TABLE_OF_CONTENTS_FOR_SUMMARY = """  - [{endpoint}](endpoints/{markdown_path})
"""

    @staticmethod
    def _get_path_parts(endpoint: str) -> List[str]:
        return endpoint.strip("/").split("/")

    def _is_path_parameter(self, endpoint_part: str) -> bool:
        if re.match(pattern=self.PATH_PARAMETERS_REGEX, string=endpoint_part):
            return True
        return False

    @staticmethod
    def _escape_special_characters(endpoint: str) -> str:
        return endpoint.replace("<", "&lt;").replace(">", "&gt;")

    def _get_path_parameter_name(self, path_parameters: str) -> str:
        """Given a string that represents path parameters, returns the underlying name
        of the path parameter.

        For example, <schema> returns schema, while <path:filename> returns path."""
        return path_parameters.strip("<").strip(">").split(":")[0]

    def get_top_level_prefix(self, endpoint: str) -> str:
        """Returns the topmost url prefix for a given endpoint. If the endpoint only
        has one part, it returns itself stripped of any /."""
        return self._get_path_parts(endpoint)[0]

    def get_path_parameters(self, endpoint: str) -> List[str]:
        """Returns path parameters if there are any for a given endpoint.
        Path parameters are enclosed by <>.

        For example: /my/endpoint/<path> would return [<path>]."""
        endpoint_parts = self._get_path_parts(endpoint)
        return [
            endpoint_part
            for endpoint_part in endpoint_parts
            if self._is_path_parameter(endpoint_part)
        ]

    def get_endpoint_docs_file_name(self, endpoint: str) -> str:
        """Returns the innermost part of the URL to be used as the endpoint file name.
        If the innermost part is a path parameter, returns the name of the path parameter.
        If an endpoint has only one part, it will return _root_ because the one part should be
        a relative directory instead."""
        endpoint_parts = self._get_path_parts(endpoint)
        if len(endpoint_parts) == 1:
            return self.ROOT_ENDPOINT_FILE_NAME
        if self._is_path_parameter(endpoint_parts[-1]):
            return self._get_path_parameter_name(endpoint_parts[-1])
        return endpoint_parts[-1]

    def get_endpoint_docs_relative_dir(self, endpoint: str) -> str:
        """Returns the outer part of the URL to be the relative directory of the endpoint file.
        If any of the parts of the branch have path parameters, replaces the string with the
        name of the path parameter.
        If an endpoint only has one part, it returns itself.

        For example: /foo/<baz>/bar returns foo/baz.
                     /single/ returns single"""
        endpoint_parts = self._get_path_parts(endpoint)
        if len(endpoint_parts) == 1:
            return endpoint_parts[0]
        branch = [
            self._get_path_parameter_name(endpoint_part)
            if self._is_path_parameter(endpoint_part)
            else endpoint_part
            for endpoint_part in endpoint_parts
        ]
        return os.path.join("", *branch[:-1])

    def generate_documentation_markdown_for_endpoint(
        self, specification_template_path: str, rule: Rule
    ) -> str:
        """Reads in a specification template and formats a rule accordingly."""
        rule_methods = rule.methods if rule.methods is not None else set()
        methods = ", ".join(sorted(list(rule_methods)))
        escaped_path_params = [
            self._escape_special_characters(path_params)
            for path_params in self.get_path_parameters(rule.rule)
        ]
        path_params = ", ".join(sorted(escaped_path_params))
        with open(specification_template_path, "r", encoding="utf-8") as spec_file:
            spec = StrictStringFormatter().format(
                spec_file.read(),
                endpoint=self._escape_special_characters(rule.rule),
                methods=methods,
                path_params=path_params,
            )
            return spec

    def generate_markdown_path_for_endpoint(
        self, docs_directory: str, endpoint: str
    ) -> str:
        """Produces the raw file path for the Markdown file that will hold documentation about a given endpoint."""
        return os.path.join(
            docs_directory,
            self.get_endpoint_docs_relative_dir(endpoint),
            f"{self.get_endpoint_docs_file_name(endpoint)}.md",
        )

    def generate_markdown_table_of_contents_for_endpoint(
        self, docs_directory: str, endpoint: str
    ) -> str:
        """Produces an entry for the table of contents for a given documentation file produced."""
        markdown_path = self.generate_markdown_path_for_endpoint(
            docs_directory, endpoint
        )
        return StrictStringFormatter().format(
            self.MARKDOWN_TABLE_OF_CONTENTS_FOR_SUMMARY,
            endpoint=self._escape_special_characters(endpoint),
            markdown_path=os.path.relpath(markdown_path, docs_directory),
        )

    def generate_documentation_for_endpoint(
        self, docs_directory: str, specification_template_path: str, rule: Rule
    ) -> None:
        """Generates appropriate documentation for a given rule and writes to the path outlined."""
        path = self.generate_markdown_path_for_endpoint(docs_directory, rule.rule)
        documentation = self.generate_documentation_markdown_for_endpoint(
            specification_template_path, rule
        )
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as markdown_doc_file:
            markdown_doc_file.write(documentation)


@lru_cache(maxsize=None)
def app_rules() -> List[Rule]:
    with mock.patch(
        "recidiviz.cloud_storage.gcsfs_factory.GcsfsFactory.build",
        return_value=FakeGCSFileSystem(),
    ):
        with mock.patch(
            "recidiviz.utils.metadata.project_id", return_value=GCP_PROJECT_STAGING
        ):
            # pylint: disable=import-outside-toplevel
            from recidiviz.justice_counts.control_panel.server import (
                get_blueprints_for_justice_counts_documentation,
            )
            from recidiviz.server import get_blueprints_for_documentation

            temp_app = Flask(__name__)
            all_blueprints_with_url_prefixes = (
                get_blueprints_for_documentation()
                + get_blueprints_for_justice_counts_documentation()
            )
            for blueprint, url_prefix in all_blueprints_with_url_prefixes:
                temp_app.register_blueprint(blueprint, url_prefix=url_prefix)
            return list(temp_app.url_map.iter_rules())


def generate_documentation_for_new_endpoints(
    generator: EndpointDocumentationGenerator,
) -> bool:
    """
    Loops through all of the endpoints that we have added to our AppEngine blueprints
    and for any new endpoints, add their documentation to the appropriate markdown location.
    Returns whether or not new endpoint documentation was added.
    """
    existing_endpoint_docs: Set[str] = get_all_files_recursive(ENDPOINT_DOCS_DIRECTORY)
    new_endpoint_docs: Set[str] = {ENDPOINT_CATALOG_SPECIFICATION}

    anything_modified = False
    for rule in app_rules():
        markdown_path = generator.generate_markdown_path_for_endpoint(
            docs_directory=ENDPOINT_DOCS_DIRECTORY, endpoint=rule.rule
        )
        new_endpoint_docs.add(markdown_path)

        if not os.path.exists(markdown_path):
            anything_modified |= True
            generator.generate_documentation_for_endpoint(
                docs_directory=ENDPOINT_DOCS_DIRECTORY,
                specification_template_path=ENDPOINT_CATALOG_SPECIFICATION,
                rule=rule,
            )

    # Delete any deprecated endpoint files
    deprecated_files = existing_endpoint_docs.difference(new_endpoint_docs)
    if deprecated_files:
        delete_files(deprecated_files, delete_empty_dirs=True)
        anything_modified |= True

    return anything_modified


def _create_ingest_catalog_summary_for_endpoints(
    generator: EndpointDocumentationGenerator,
) -> List[str]:
    """Creates the Endpoint Catalog portion of SUMMARY.md, as a list of lines."""

    top_level_prefixes_to_endpoints: Dict[str, List[str]] = defaultdict(list)
    for rule in app_rules():
        top_level_prefix = f"/{generator.get_top_level_prefix(rule.rule)}/"
        top_level_prefixes_to_endpoints[top_level_prefix].append(rule.rule)
    endpoint_catalog_summary = ["## Endpoint Catalog\n\n"]

    for top_level_prefix, endpoints in sorted(top_level_prefixes_to_endpoints.items()):
        table_of_contents = sorted(
            [
                generator.generate_markdown_table_of_contents_for_endpoint(
                    docs_directory=ENDPOINT_DOCS_DIRECTORY, endpoint=endpoint
                )
                for endpoint in endpoints
            ]
        )
        endpoint_catalog_summary.extend([f"- {top_level_prefix}\n"] + table_of_contents)

    return endpoint_catalog_summary


def main() -> int:
    docs_generator = EndpointDocumentationGenerator()
    added = generate_documentation_for_new_endpoints(docs_generator)
    if added:
        update_summary_file(
            _create_ingest_catalog_summary_for_endpoints(docs_generator),
            "## Endpoint Catalog",
        )

    return 1 if added else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(main())
