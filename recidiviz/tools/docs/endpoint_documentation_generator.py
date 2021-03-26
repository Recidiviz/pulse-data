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
"""Class providing methods for generating documentation regarding endpoints."""
import os
import re
from typing import List

from werkzeug.routing import Rule


class EndpointDocumentationGenerator:
    """Class providing methods for generating documentation regarding endpoints."""

    PATH_PARAMETERS_REGEX = re.compile(r"^<([a-z_]+)(:[a-z_]+)?>")
    MARKDOWN_TABLE_OF_CONTENTS_FOR_SUMMARY = """- [{endpoint}](endpoints/{markdown_path})
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
        return endpoint.replace("<", "&lt;").replace(">", r"&gt;")

    def _get_path_parameter_name(self, path_parameters: str) -> str:
        """Given a string that represents path parameters, returns the underlying name
        of the path parameter.

        For example, <schema> returns schema, while <path:filename> returns path."""
        return path_parameters.strip("<").strip(">").split(":")[0]

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

    def get_leaf_url(self, endpoint: str) -> str:
        """Returns the innermost part of the URL. If the innermost part is a path parameter,
        returns the name of the path parameter. If an endpoint has only one part, it returns itself."""
        endpoint_parts = self._get_path_parts(endpoint)
        if len(endpoint_parts) == 1:
            return endpoint_parts[0]
        if self._is_path_parameter(endpoint_parts[-1]):
            return self._get_path_parameter_name(endpoint_parts[-1])
        return endpoint_parts[-1]

    def get_branch_of_url(self, endpoint: str) -> str:
        """Returns the outer part of the URL that is not a leaf. If any of the parts of the
        branch have path parameters, replaces the string with the name of the path parameter.
        If an endpoint only has one part, it returns an empty string (no branch).

        For example: /foo/<baz>/bar returns foo/baz as the branch."""
        endpoint_parts = self._get_path_parts(endpoint)
        if len(endpoint_parts) == 1:
            return ""
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
        methods = ", ".join(sorted(list(rule.methods)))
        escaped_path_params = [
            self._escape_special_characters(path_params)
            for path_params in self.get_path_parameters(rule.rule)
        ]
        path_params = ", ".join(sorted(escaped_path_params))
        with open(specification_template_path, "r") as spec_file:
            spec = spec_file.read().format(
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
            self.get_branch_of_url(endpoint),
            f"{self.get_leaf_url(endpoint)}.md",
        )

    def generate_markdown_table_of_contents_for_endpoint(
        self, markdown_path: str, docs_directory: str, endpoint: str
    ) -> str:
        """Produces an entry for the table of contents for a given documentation file produced."""
        return self.MARKDOWN_TABLE_OF_CONTENTS_FOR_SUMMARY.format(
            endpoint=self._escape_special_characters(endpoint),
            markdown_path=os.path.relpath(markdown_path, docs_directory),
        )

    def generate_documentation_for_endpoint(
        self, docs_directory: str, specification_template_path: str, rule: Rule
    ) -> str:
        """Generates appropriate documentation for a given rule and returns the entry for the table of contents
        that should be pasted into docs/SUMMARY.md."""
        path = self.generate_markdown_path_for_endpoint(docs_directory, rule.rule)
        documentation = self.generate_documentation_markdown_for_endpoint(
            specification_template_path, rule
        )
        os.makedirs(os.path.dirname(path), exist_ok=True)
        table_of_content_entry = self.generate_markdown_table_of_contents_for_endpoint(
            markdown_path=path,
            docs_directory=docs_directory,
            endpoint=rule.rule,
        )
        with open(path, "w") as markdown_doc_file:
            markdown_doc_file.write(documentation)
        return table_of_content_entry


if __name__ == "__main__":
    from flask import Flask
    from recidiviz.server import all_blueprints_with_url_prefixes

    temp_app = Flask(__name__)
    for blueprint, url_prefix in all_blueprints_with_url_prefixes:
        temp_app.register_blueprint(blueprint, url_prefix=url_prefix)
    generator = EndpointDocumentationGenerator()
    for app_rule in temp_app.url_map.iter_rules():
        print(
            generator.generate_documentation_for_endpoint(
                docs_directory="docs/endpoints",
                specification_template_path="docs/endpoints/endpoint_specification_template.md",
                rule=app_rule,
            )
        )
